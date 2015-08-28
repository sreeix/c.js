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
            committingTransitionFn: function (event, context) {
                return fsm.committed();
            },
            transitionFn: function (event, context) {
                console.log("----", context);
                if(_.first(_.values(event)) === 'COMMIT'){
                    context.commitVotes++;
                }
                if(_.first(_.values(event)) === 'ABORT'){
                    context.abortVotes++;
                }
                if(context.abortVotes > context.abortVotesNeeded){
                    return fsm.aborting();
                }
                if(context.commitVotes >= context.commitVotesNeeded){
                    return fsm.committing();
                }
                return fsm.noStateChange();
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
