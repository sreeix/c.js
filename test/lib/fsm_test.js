"use strict";
var fsm = require('../../lib/fsm');
var _ = require('underscore');
describe(" fsm", function() {


    it("works for simplest machine", function  (arg) {
        var tp = fsm({states:[], initialState: 'XXX', finalStates: []})
    })

    it("is in initial state ", function() {
        var tp = fsm({
            states: ['PREPARE', "COMMITTING", "ABORTING", "COMMITTED", "ABORTED"],
            initialState: 'PREPARE',
            finalStates: ['COMMITTED', 'ABORTED'],
            context: {commitVotesNeeded: 2, abortVotesNeeded: 2, commitVotes: 0, abortVotes: 0},
            onCommitting: function (context) {
                console.log("***********Committing.");
            },
            onCommitted: function (context) {
                console.log("**********************Committed.");
            },
            committingTransitionFn: function (event, context) {
                return this.committed();
            },
            transitionFn: function (event, context) {
                if(_.first(_.values(event)) === 'COMMIT'){
                    context.commitVotes++;
                }
                if(_.first(_.values(event)) === 'ABORT'){
                    context.abortVotes++;
                }
                if(context.abortVotes > context.abortVotesNeeded){
                    return this.aborting();
                }
                if(context.commitVotes >= context.commitVotesNeeded){
                    return this.committing();
                }
                return this.noStateChange();
            }
        });

        tp.currentState.should.equal('PREPARE');
        tp.sendEvent({site1: 'COMMIT'});
        tp.currentState.should.equal('PREPARE');
        tp.sendEvent({site2: 'COMMIT'});
        tp.isCommitting().should.equal(true);
        tp.currentState.should.equal('COMMITTING');
        tp.sendEvent({site2: 'COMMIT'});
        tp.currentState.should.equal('COMMITTED');
    });
});
