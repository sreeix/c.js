"use strict";
var fsm = require('../../lib/fsm');
var _ = require('underscore');
describe(" fsm", function() {


    it("is in initial state ", function() {
        fsm.init({
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

        fsm.currentState.should.equal('PREPARE');
        fsm.sendEvent({site1: 'COMMIT'});
        fsm.currentState.should.equal('PREPARE');
        fsm.sendEvent({site2: 'COMMIT'});
        fsm.isCommitting().should.equal(true);
        fsm.currentState.should.equal('COMMITTING');
        fsm.sendEvent({site2: 'COMMIT'});
        fsm.currentState.should.equal('COMMITTED');

    });
});
