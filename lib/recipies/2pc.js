"use strict";
var _ = require('underscore');
var zkPath = require('../zkPath');
var zookeeper = require('node-zookeeper-client');
var Promise = require('bluebird');
var returning = require('../promiseHelper').returning;
var fsm = require('../fsm');
var using = Promise.using;
// In a classic two phase commit protocol. there is a coordinator and there are sites that act upon the transaction.
// * Coordinator sends a prepare message to all sites .
// * each site can vote on weather it can commit the transaction and responds with either COMMIT/ ABORT, as the case may be
// ** Typically the node would write to the stable storage a prepare log record(_before_ sending the COMMIT/ABORT)
// * If coordinator receives COMMIT from all sites, it can ask the sites to go ahead and commit the transaction.
//         * coordinator now sends COMMIT message to all the sites
//         ** sites will now commit the transaction.
//         ** sites will then send and ACK response post commit. Before sending the response whey would typically write the commit record to stable storage.
// * If the coordinator receives a ABORT from any of the sites, it will have to force the transaction to abort.
//         * coordinator will then send ABORT message to all the sites(especially the ones that are ready to COMMIT, most times coordinators will not send ABORT to the sites that voted ABORT)
//         ** Sites on receiving the ABORT message will abort the transaction(typically by writing the abort record to stable storage)
//         ** Sites will then send an ACK response to the coordinator
//  * If the coordinator does not receive any message from one of the sites (in a specified ) it is treated as an ABORT and the flow follows the ABORT flow
//  * After all ack messages have been received, the transaction is assumed to have completed(END of transaction record is inserted.)


// 2 pc with presumed abort needs us to know about recovery mechanisms. SO there they are.
// Not that presumed abort and presumed commit are defined for classic 2pc and should be used with them and not with other quorums
/// It is important to be aware of the recovery mechanisms because the sites and the coordinators need to implement that.
// *


// For using classic 2 phase commit following options is to be used.
    // {quorum: false, coordinatorCommits: true, sitesCreateNodes: false, timeout: 5000};
    // options
// We allow a bit of flexibility. with commits. Following are the changes that may make sense in certain cases.
// quorum: true/false or a number, true applies a standard quorum for commit votes(2*n +1). false: apply no quorum(classic 2pc). or if a number is specified needs that many votes for the commit to succeed.
// cordinatorCommits -> In an alternative to classic 2PC, (to reduce number of messages exchanged) we could let sites operate independently, ie. wait for the sites to figure out if commit is a go/ no go.
// siteCreateNodes is an implementation detail. there may be sitations when sites die right after coordinator creates the zookeeper nodes. This means that coordinator would waiting for non existant nodes(which is bad). The alternative is to allow sites to create it themselves and in the case they are dead they never get to create them.
    // sendAbortToAllSites -> As an optimization(in Classic 2PC) we could chose to not send ABORT to sites that already voted for ABORT. This will ensure we do not send ABORTS to failed sites(timeout) and even sites that could not write to tlogs.
    // presumedAbort adds the enhancements described by Efficient commit protocols for tree of process model of distributed transactions. ie. No ack messages from the sites on abort message, So Coordinator does not even wait for ack messages on abort.(In all such cases they are treated as abort.)

module.exports = function  (zkClient) {
    var defaultOptions = {quorum: true, coordinatorCommits: true, sitesCreateNodes: true, timeout: 5000, sendAbortToAllSites: true, presumedAbort:false};
    var zkLib = require('../zkLib')(zkClient);

    function setupWaitForConsensus(path, options) {
        var consensusCoordinator = trackConsensus(options);
        return zkLib.watchAllChildren(path, consensusCoordinator.watcher).each(function (child) {
            // set data that we may have missed because of delayed watches(?)
            var childPath = zkPath.join(path,child);
            return zkLib.getData(zkPath.join(path,child)).get(0)
                .then(_.partial(consensusCoordinator.update, childPath));
        }).then(returning(consensusCoordinator))
            .catch(function  (err) {
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
        var timeoutHandle;
        var promise = new Promise(function () {
            resolve = arguments[0];
            reject = arguments[1];
        });
        var commitVotesNeeded = options.sites.length;
        var abortVotesNeeded = 1;

        if(_.isNumber(options.quorum) ){
            commitVotesNeeded = options.quorum;
            abortVotesNeeded = options.sites.length - options.quorum;
        }
        if(options.quorum === true){
            commitVotesNeeded = Math.floor(options.sites.length /2) + 1;
            abortVotesNeeded = options.sites.length - commitVotesNeeded ;
        }
        console.log("Need %s votes to commit and %s votes to abort", commitVotesNeeded, abortVotesNeeded);

        var resolveIfConsensus = function () {
            if(promise.isResolved()) {
                return null;
            }
            var grouped = _.groupBy(_.values(consensusStatus));
            var commitVotes = grouped.COMMIT ? grouped.COMMIT.length : 0;
            var abortVotes = grouped.ABORT ? grouped.ABORT.length : 0;
            var noVotes = options.sites.length - (commitVotes + abortVotes);

            console.log("C", commitVotes, "A", abortVotes, "N", noVotes);
            if(commitVotes === 0 && abortVotes === 0) {
                return null; // don't even bother when there are no votes for either
            }
            if(commitVotes >= commitVotesNeeded){
                clearTimeout(timeoutHandle);
                return resolve('COMMIT');
            }
            // don't abort till commit votes and no votes can still reach the quorum... Assuming that nodes can drop out
            if(abortVotes > abortVotesNeeded ){
                clearTimeout(timeoutHandle);
                return resolve('ABORT');
            }
            if(noVotes +commitVotes < commitVotesNeeded) {
                clearTimeout(timeoutHandle);
                return resolve('ABORT');// there is no chance to make quorum with remaining votes also.
            }
            return null;
        };

        var updateFn = function (path, buf) {
            if(buf){
                consensusStatus[path] = buf.toString('UTF-8');
            } else {
                consensusStatus[path] = null;
            }
            resolveIfConsensus();
        };

        // In general this should not modify existing values(because prepare should be once and once only.) If Sites drop out after commitint to commit, the recovery should take over.
        var watchFn = function name(event) {
            var path = zkPath.childNode(event.getPath());
            var type = event.getType();
            console.log("Path change event on ", path, event);
            if(type === zookeeper.Event.NODE_DELETED){
                console.log("node", event.getPath(), "deleted");
                // There are atleast 2 cases here.
                // 1) The node previously agreed to commit and wrote it to it's transaction log -> In this case we keep the status as COMMIT(when the node comes back up it will do the recovery(talk to the coordinator and resolve))
                // 2) It never got the chance to prepare, and it was null in the past and it is still null now, so we don't update the record.
//                updateFn(path, null);
            }
            if(type === zookeeper.Event.NODE_CREATED){
                console.log("node", event.getPath(), "created");
                updateFn(path, null);
            }
            if(type === zookeeper.Event.NODE_DATA_CHANGED){
                zkLib.getData(event.getPath()).get(0).then(_.partial(updateFn, path));
            }
        };

        timeoutHandle = setTimeout(function  () {
            console.log("No consensus till the timeout period. Aborting transaction", options.timeout);
            resolve('ABORT');
        }, options.timeout);

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
                    var execOpts = _.defaults(opts, defaultOptions);
                    var sites = execOpts.sites;
                    var transactionPath = zkPath.join(path, 'transaction');
                    zkLib.ensurePath(transactionPath).then(returning(sites))
                        .each(function createChildNode(site) {
                            console.log("Creating ephemeral nodes for site", site.id, site.selfPath());
                            return zkLib.create(site.selfPath(), null, zookeeper.CreateMode.EPHEMERAL);
                        })
                        .then(_.partial(setupWaitForConsensus, transactionPath, execOpts))
                        .then(function  prepare(consensusCoordinator) {
                            // prepare sends messages to the sites. It should not wait on the prepare to finish.
                            // so using the async version. and not Promise.each
                             _.each(sites, function (site) {
                                return site.prepare(command, execOpts);
                            });
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
                                    console.log('Commiting transaction at site', site.id);
                                    return site.commit();
                                } else {
                                    console.log('Aborting transaction at site', site.id);
                                    return site.abort();
                                }
                            }).then(function  () {
                                if(state === 'ABORT') {
                                    return Promise.reject('ABORT');
                                } else {
                                    return Promise.resolve('COMMIT');
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
                        console.log("coordinator commits, so site is not waiting.");
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
