"use strict";
var _ = require('underscore');
var zkPath = require('../zkPath');
var zookeeper = require('node-zookeeper-client');
var Promise = require('bluebird');
var using = Promise.using;
module.exports = function  (zkClient) {
    var defaultOptions = {quorum: true, coordinatorCommits: true, sitesCreateNodes: true, timeout: 5000};
    var zkLib = require('../zkLib')(zkClient);

    function setupWaitForConsensus(path, options) {
        var consensusCoordinator = trackConsensus(options);
        return zkLib.watchAllChildren(path,  consensusCoordinator.watcher).spread(function  (children, stats) {
            // set data that we may have missed because of delayed watches(?)
            Promise.each(children, function (c) {
                var childPath = zkPath.join(path,c);
                return zkLib.getData(childPath).spread(function (data) {
                    return consensusCoordinator.update(childPath, data);
                });
            });
            // don't really wait for the updates to happen. We will anyways wait somewhere.
            return consensusCoordinator;
        }).catch(function  (err) {
            console.log("Error occured watching children", err);
            return {promise: Promise.resolve('ABORT')};
        });
    }

    function trackConsensus(options) {
        // options we care about is quorum.
        // if true N/2 + 1 sites need to vote for commit.
        // if false , all sites need to vote for commit, NOTE this means all nodes need to vote commit
        // if a number, that many sites need vote for commit
        var consensusStatus= {}, resolve, reject;
        var promise = new Promise(function () {
            resolve = arguments[0];
            reject = arguments[1];
        });
        var commitVotesNeeded = options.sites.length;
        var abortVotesNeeded = 1;

        if(_.isNumber(options.quorum) ){
            commitVotesNeeded = options.quorum;
            abortVotesNeeded = options.quorum;
        }
        if(options.quorum === true){
            commitVotesNeeded = Math.floor(options.sites.length /2) + 1;
            abortVotesNeeded = Math.abs(Math.floor(options.sites.length /2) - 1 );
        }
        console.log("Need %s votes to commit and %s votes to abort", commitVotesNeeded, abortVotesNeeded);
        var resolveIfConsensus = function () {
            var commitVotes = _.select(_.values(consensusStatus), _.bind(_.isEqual, _, 'COMMIT')).length;
            var abortVotes = _.select(_.values(consensusStatus), _.bind(_.isEqual, _, 'ABORT')).length;
            var noVotes = _.select(_.values(consensusStatus), _.bind(_.isEqual, _, null)).length;
            console.log("C", commitVotes, "A", abortVotes, "N", noVotes);
            if(commitVotes === 0 && abortVotes === 0) return null; // don't even bother when there are no votes for either
            if(commitVotes >= commitVotesNeeded){
                return resolve('COMMIT');
            }
            if(abortVotes >= abortVotesNeeded){
                return resolve('ABORT');
            }
        };
        var updateFn = function (path, buf) {
            if(buf!==null){
                console.log("Updating ", path , "with", buf.toString('UTF-8'));
                consensusStatus[path] = buf.toString('UTF-8');
            } else {
                console.log("Updating ", path , "with", null);
                consensusStatus[path] = null;
            }
            resolveIfConsensus();
        };

        var watchFn =function name(event) {
            var path = zkPath.childNode(event.getPath());
            var type = event.getType();
            console.log("Path change event on ", path);
            if(type === zookeeper.Event.NODE_DELETED){
                console.log("node", event.getPath(), "deleted");
                updateFn(path, null);
            }
            if(type === zookeeper.Event.NODE_CREATED){
                console.log("node", event.getPath(), "created");
                updateFn(path, null);
            }
            if(type === zookeeper.Event.DATA_CHANGED){
                console.log("node", event.getPath(), "data changed");
                zkLib.getData(event.getPath()).spread(function  (buf) {
                    console.log("Got update", consensusStatus[path], buf.toString('UTF-8'));
                    updateFn(path, buf);
                });
            }
        };

        return  {
            promise: promise,
            watcher: watchFn,
            update: updateFn
        };
    }

    return {
        coordinator: function coordinator(path) {
            return {
                execute: function (command, opts, cb) {
                    var execOpts = _.defaults(defaultOptions, opts);
                    var sites = execOpts.sites;
                    var transactionPath = zkPath.join(path, 'transaction');
                    zkLib.ensurePath(transactionPath)
                        .then(function createChildSiteNodes() {
                            return Promise.each(sites, function createChildNode(site) {
                                console.log("Creating ephemeral nodes for site", site.id, site.selfPath());
                                    return zkLib.create(site.selfPath(), null, zookeeper.CreateMode.EPHEMERAL);
                                });
                        })
                        .then(function  prepare() {
                            return Promise.each(sites, function  (site) {
                                return site.prepare(command, execOpts);
                            })
                                .then(_.partial(setupWaitForConsensus, transactionPath, execOpts)).tap(console.log);
                        }).then(function (consensusCoordinator) {
                            console.log("waiting for consensus");
                            return consensusCoordinator.promise;
                        })
                        .then(function commitTransaction(state) {
                            console.log("Consensus has emerged.", state);
                            // One option is to let sites themselves decided when all the nodes agree.
                            // This means less messages from coordinator to the sites, leads to lesser issues and is faster.
                            // Other option is let coordinator send the commit/abort message to all sites and this leads to more messages from coordinator,
                            // but less load on the zookeeper, because of all the watches/queries from sites.
                            return Promise.each(sites, function send(site) {
                                console.log("Sending to site", site.id, state);
                                if(state ==='COMMIT'){
                                    return site.commit();
                                } else {
                                    return site.abort();
                                }
                            });
                            // var maybePromise = null;
                            // if(execOpts.coordinatorCommits) {
                            //     maybePromise = waitForAgreement().then(sendCommitToSites);
                            // }
                            // // each node commits the transaction autonomously.
                            // return Promise.resolve(maybePromise).then(waitForSitesToCommit);
                        }).then(function  () {
                            console.log('Transaction complete');
                        }).finally(function  () {
                            // ok we should not be doing this stuff here,ignoring all advice for now.
                            console.log(". Cleaning up", transactionPath);
                            return zkLib.rmPath(transactionPath);
                        }).nodeify(cb);
                }
            };
        },
        site: function  (transactionPath, id, intentFn, commitFn, abortFn) {
            var site = {
                id: id,
                prepare: function  (command, opts) {
                    // setup wait actually does not wait. It only sets up watches on the children, and returns an object to wait on.
                    console.log("Setting up site prepare.");
                    var waitForConsensus = null;
                    if(opts.coordinatorCommits){
                        console.log("coordinator commits");
                        waitForConsensus= {promise: Promise.resolve()};
                    } else {
                        waitForConsensus = setupWaitForConsensus(transactionPath, opts);
                    }
                    // wait if coordinatorCommits there is no need to wait for consensus. Coordinator will send an explicit
                    // COMMIT, ABORT message, othewise we need to wait for consensus and update accordingly.
                    return Promise.join(Promise.promisify(intentFn)(command)
                                        .then(site.voteToCommit)
                                        .catch(site.voteToRollback),
                                        waitForConsensus.promise);

                },
                voteToRollback: function () {
                    console.log("Voting to Abort", site.selfPath());
                    return zkLib.setData(site.selfPath(), 'ABORT');
                },
                selfPath: function  () {
                    return zkPath.join(transactionPath, id);
                },
                voteToCommit: function  () {
                    console.log("Voting to commit", site.selfPath());
                    return zkLib.setData(site.selfPath(), 'COMMIT');
                },
                commit: function  (command) { // ideally this should be the transactionid
                    if(commitFn && _.isFunction(commitFn)){
                        return Promise.promisify(commitFn)(command);
                    } else{
                        console.log("No commit function provided. So commit is success");
                        return true;
                    }
                },
                abort: function  (command) {
                    if(abortFn && _.isFunction(abortFn)) {
                        return Promise.promisify(abortFn)(command);
                    } else {
                        console.log("No Abort function specified, Abort is success");
                        return true;
                    }
                }
            };
            return site;
        }
    };
};
