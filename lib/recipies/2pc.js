"use strict";
var _ = require('underscore');
var zkPath = require('../zkPath');
var fsm = require('../fsm');
var zookeeper = require('node-zookeeper-client');
var Promise = require('bluebird');
var returning = require('../promiseHelper').returning;

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


// For using classic 2 phase commit following options should be used.
    // {quorum: false, coordinatorCommits: true, sitesCreateNodes: false, timeout: 5000};
// Other options

// We allow a bit of flexibility. with commits. Following are the changes that may make sense in certain cases.
// quorum: true/false or a number, true applies a standard quorum for commit votes(2*n +1). false: apply no quorum(classic 2pc). or if a number is specified needs that many votes for the commit to succeed.
// cordinatorCommits -> In an alternative to classic 2PC, (to reduce number of messages exchanged) we could let sites operate independently, ie. wait for the sites to figure out if commit is a go/ no go.
// siteCreateNodes is an implementation detail. there may be sitations when sites die right after coordinator creates the zookeeper nodes. This means that coordinator would waiting for non existant nodes(which is bad). The alternative is to allow sites to create it themselves and in the case they are dead they never get to create them.
    // sendAbortToAllSites -> As an optimization(in Classic 2PC) we could chose to not send ABORT to sites that already voted for ABORT. This will ensure we do not send ABORTS to failed sites(timeout) and even sites that could not write to tlogs.
    // presumedAbort adds the enhancements described by Efficient commit protocols for tree of process model of distributed transactions. ie. No ack messages from the sites on abort message, So Coordinator does not even wait for ack messages on abort.(In all such cases they are treated as abort.)

module.exports = function  (zkClient) {
    var defaultOptions = {quorum: false, coordinatorCommits: true, sitesCreateNodes: true, timeout: 5000, sendAbortToAllSites: false, presumedAbort: false, presumedCommit: false};
    var zkLib = require('../zkLib')(zkClient);
    var consensus = {
        init: function (options) {
            consensus.options = options;
            consensus.phase1Promise = new Promise(function () {
                consensus.resolve = arguments[0];
                consensus.reject = arguments[1];
            });
            consensus.phase2Promise = new Promise(function () {
                consensus.resolvePhase2 = arguments[0];
                consensus.rejectPhase2 = arguments[1];
            });

            var context = {
                commitVotesNeeded:  options.sites.length,
                abortVotesNeeded: 1
            };

            if(_.isNumber(options.quorum) ){
                context.commitVotesNeeded = options.quorum;
                context.abortVotesNeeded = options.sites.length - options.quorum;
            }
            if(options.quorum === true){
                context.commitVotesNeeded = Math.floor(options.sites.length /2) + 1;
                context.abortVotesNeeded = options.sites.length - context.commitVotesNeeded ;
            }
            consensus.fsm = fsm({
                states: ['PREPARING', 'COMMITTING', 'ABORTING', 'COMMITTED', 'ABORTED'],
                initialState: {state: 'PREPARING', timeout: {value: 5000, nextState: 'ABORTING'}},
                finalStates: ['COMMITTED', 'ABORTED'],
                context: context,
                onCommitting: function (context) {
                    console.log("committing");
                    context.committing = {};
                    consensus.resolve('COMMIT');
                },
                onCommitted: function (context) {
                    console.log('Committed');
                    consensus.resolvePhase2('COMMITTED');
                },
                onAborted: function (context) {
                    console.log('Aborted');
                    consensus.resolvePhase2('ABORTED');
                },
                onAborting: function (context) {
                    console.log('aborting');
                    context.aborting= {};
                    consensus.resolve('ABORTING');
                },
                onPreparing: function (context) {
                    context.preparing = {};
                },
                abortingTransitionFn: function (event, context) {
                    var path = zkPath.childNode(event.getPath());
                    var type = event.getType();
                    console.log("Path change event on ", path, event);
                    if(type === zookeeper.Event.NODE_DATA_CHANGED){
                        zkLib.getData(event.getPath())
                            .get(0)
                            .then(_.bind(consensus.updatePhase2Abort, this, path, context));
                    }
                },
                committingTransitionFn: function (event, context) {
                    var path = zkPath.childNode(event.getPath());
                    var type = event.getType();
                    console.log("Path change event on ", path, event);
                    if(type === zookeeper.Event.NODE_DATA_CHANGED){
                        zkLib.getData(event.getPath()).get(0).then(_.bind(consensus.updatePhase2Commit, this, path, context));
                    }
                },
                preparingTransitionFn: function (event, context) {
                    var path = zkPath.childNode(event.getPath());
                    var type = event.getType();
                    console.log("Path change event on ", path, event);
                    if(type === zookeeper.Event.NODE_DATA_CHANGED){
                        zkLib.getData(event.getPath()).get(0).then(_.bind(consensus.updatePhase1, this, path, context));
                    }
                },
                transitionFn: function (event, context) {
                    console.log("xxxxxxxxxxxxxxxxxxxx Should ideally not be called");
                }
            });
        },
        watcher: function (event) {
            return consensus.fsm.sendEvent(event);
        },
        didSiteVoteForCommit: function (site) {
            return consensus.fsm.context().preparing[site] === 'COMMIT';
        },
        updatePhase1: function (path, context, data) {
            context.preparing[path] = data ?  data.toString('UTF-8') : null;
            var grouped = _.chain(context.preparing).values().groupBy().value();
            var commitVotes = grouped.COMMIT ? grouped.COMMIT.length : 0;
            var abortVotes = grouped.ABORT ? grouped.ABORT.length : 0;
            var noVotes = consensus.options.sites.length - (commitVotes + abortVotes);
            console.log("C", commitVotes, "A", abortVotes, "N", noVotes);
            if(commitVotes === 0 && abortVotes === 0) {
                return this.noStateChange(); // don't even bother when there are no votes for either
            }
            if(commitVotes >= context.commitVotesNeeded){
                return this.committing({value: consensus.options.timeout, nextState: 'ABORTED'});
            }
            // don't abort till commit votes and no votes can still reach the quorum... Assuming that nodes can drop out
            if(abortVotes > context.abortVotesNeeded  || noVotes +commitVotes < context.commitVotesNeeded){
                return this.aborting({value: consensus.options.timeout, nextState: 'ABORTED'});
            }
            return this.noStateChange(); // no state change
        },
        updatePhase2Abort: function (path, context, data) {
            context.aborting[path] = data ?  data.toString('UTF-8') : null;
            // Abort has been sent to all the nodes that agreed to commit.
            // so track the nodes that agreed to commit and ensure their responses have come.
            var grouped = _.chain(context.preparing).keys().groupBy(function (site) {
                return context.preparing[site];
            }).value();

            var commitVotes = grouped.COMMIT ? grouped.COMMIT: [];
            var allAborted = _.all(commitVotes, function (site) {
                return context.aborting[site] === 'ABORTED';
            });
            if(allAborted) {
                return this.aborted();
            }
            return this.noStateChange();
        },
        updatePhase2Commit: function (path, context, data) {
            context.committing[path] = data ?  data.toString('UTF-8') : null;
            var grouped = _.chain(context.preparing).keys().groupBy(function (site) {
                return context.preparing[site];
            }).value();

            var commitVotes = grouped.COMMIT ? grouped.COMMIT: [];
            var allCommitted = _.all(commitVotes, function (site) {
                return context.committing[site] === 'COMMITTED';
            });
            var anyAborted = _.any(commitVotes, function (site) {
                return context.committing[site] === 'ABORTED';
            });

            if(allCommitted) {
                return this.committed();
            }
            if(anyAborted) {
                return this.aborted();
            }
            return this.noStateChange();
        }
    };

    function setupWaitForConsensus(path, options) {
        return watchSites(path, consensus);
    }

    function watchSites(path, consensus) {
        return zkLib.watchAllChildren(path, consensus.watcher)
            .then(returning(consensus))
            .catch(function  (err) {
                console.log("Error occured watching children", err);
                return {phase1Promise: Promise.resolve('ABORT'), phase2Promise: Promise.resolve('ABORTED')};
            });
    }

    return {
        coordinator: function coordinator(path) {
            return {
                execute: function (command, opts, cb) {
                    var execOpts = _.defaults(opts, defaultOptions);
                    var sites = execOpts.sites;
                    var transactionPath = zkPath.join(path, 'transaction');
                    consensus.init(execOpts);

                    var phase1 = function(sites, transactionPath, execOpts) {
                        return zkLib.ensurePath(transactionPath).then(returning(sites))
                            .each(function createChildNode(site) {
                                return zkLib.create(site.selfPath(), null, zookeeper.CreateMode.EPHEMERAL);
                            })
                            .then(_.partial(setupWaitForConsensus, transactionPath, execOpts))
                            .then(function  prepare(consensus) {
                                // prepare sends messages to the sites. It should not wait on the prepare to finish. because in the real world there is no blocking going on.
                                // so using the async version. and not Promise.each
                                _.each(sites, function (site) {
                                    return site.prepare(command, execOpts);
                                });
                                return consensus.phase1Promise;
                            });
                    };
                    var phase2 = function phase2(state) {
                        console.log("Consensus has emerged.", state);
                        return watchSites(transactionPath, consensus).then(function () {

                            _.each(sites, function send(site) {
                                var sendAbortSignal = true;
                                // as an optimization, send abort only to sites that voted to commit. But do it only for classic paxos.
                                if(state === 'ABORT') {
                                    if(!execOpts.sendAbortToAllSites){
                                        sendAbortSignal = consensus.didSiteVoteForCommit(site.childPath);
                                    }
                                    if(sendAbortSignal) {
                                        return site.abort(command);
                                    }

                                } else {
                                    return  site.commit(command);
                                }
                                return true;
                            });
                            return consensus.phase2Promise;
                        });
                    };

                    // the core of the execution happens here.
                    phase1(sites, transactionPath, execOpts)
                        .then(_.partial(phase2))
                        .then(function  respondToClient(state) {
                            console.log("Responses to prepare have arrived", state);
                            return state === 'ABORTED' ?  Promise.reject('ABORTED'): Promise.resolve('COMMIT');
                        }).finally(function  cleanup() {
                            console.log('Transaction complete');
                            // ok we should not be doing this stuff here,ignoring all advice for now.
                            consensus.fsm.reset();
                            console.log(". Cleaning up", transactionPath);
                            return zkLib.rmPath(transactionPath);
                        }).nodeify(cb);// clients use callback style.
                }
            };
        },
        site: function (transactionPath, id, intentFn, commitFn, abortFn) {
            var site = {
                id: id,
                fsm: fsm({states: ['PREPARING', 'COMMITTING', 'ABORTING', 'COMMITTED', 'ABORTED'],
                          initialState: 'PREPARING',
                          finalStates: ['COMMITTED', 'ABORTED'],
                          context: {},
                          transitionFn: function transition(event, context) {
                              console.log("Should not come here.");
                          }}),
                prepare: function  (command, opts) {
                    // setup wait actually does not wait. It only sets up watches on the children, and returns an object to wait on.
                    console.log("Setting up site prepare.");
                    var waitForConsensus = null;
                    if(opts.coordinatorCommits){
                        console.log("coordinator commits, so site is not waiting.");
                        waitForConsensus= {phase1Promise: Promise.resolve()};
                    } else {
                        waitForConsensus = setupWaitForConsensus(transactionPath, opts);
                    }
                    // wait if coordinatorCommits there is no need to wait for consensus. Coordinator will send an explicit
                    // COMMIT, ABORT message, othewise we need to wait for consensus and update accordingly.
                    return Promise.join(Promise.promisify(intentFn)(command)
                                        .then(_.bind(site.voteToCommit, site, opts))
                                        .catch(_.bind(site.voteToAbort, site, opts)),
                                        waitForConsensus.phase1Promise);
                },
                voteToAbort: function (opts) {
                    console.log("Voting to Abort", site.selfPath());
                    var nextState = 'ABORTING'
                    if(opts.sendAbortToAllSites) {
                        site.fsm.aborting();
                    } else {
                        nextState = 'ABORTED';
                        site.fsm.aborted(); // we can unilaterally abort
                    }
                    return zkLib.setData(site.selfPath(), nextState);
                },
                selfPath: function  () {
                    return zkPath.join(transactionPath, id);
                },
                voteToCommit: function  () {
                    console.log("Voting to commit", site.selfPath());
                    site.fsm.committing();
                    return zkLib.setData(site.selfPath(), 'COMMIT');
                },
                commit: _.partial(executeAndAcknowledge, 'COMMITTED', commitFn),
                abort: _.partial(executeAndAcknowledge, 'ABORTED', abortFn)
            };
            function executeAndAcknowledge(state, stateFn, command) {
                console.log("***************executing command", command, " for state ", state);
                var p =  Promise.resolve(state);
                if(stateFn && _.isFunction(stateFn)){
                    p = Promise.promisify(stateFn)(command).then(returning(state)).catch(returning("ABORTED"));
                }
                return p.then(function (state) {
                    site.fsm.sendEvent(state);
                    return state;
                }).then(_.bind(zkLib.setData, zkLib, site.selfPath()));
            }
            return site;
        }

    };
};
