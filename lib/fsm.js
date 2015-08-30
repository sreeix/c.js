"use strict";
var _ = require('underscore');
var humanize = require("underscore.string/humanize");

// fsm does some magic. For all the state we will create
// is<StateName> method to check for what state we are in.
// <stateName>() method that changes the current state to the specified state.(nostateChange function exists to ensure no state trantition happens from the transition function)
// transitions from state will invoke on<State> function (if defined) to notify users of state changes.
// We expect a transitionFn all the time, but to make this manageable you could implement a <CurrentState>TransitionFn that will be specifically invoked when the current state is that.
// the fsm iteself does everything blocking, but the transition functions can be and will be async. We will not block on the transition functions. they can update the data when it is ready.



module.exports = function create(defn) {
    function resetTimeoutIfAny() {
        if(fsm.timerHandle){
            clearTimeout(fsm.timerHandle);
        }
    }

    function setupTimeoutIfAny(timeout) {
        if(timeout){
            fsm.timerHandle = setTimeout(function () {
                fsm.setState(timeout.nextState);
            }, timeout.value);
        }
    }
    var checkStateFunction = function (state) {
        return this.currentState === state;
    };

    var fsm = {
        init: function (defn) {
            fsm.defn = defn;
            _.each(defn.states, function (state) {
                fsm['is'+humanize(state)] = _.bind(checkStateFunction,  fsm, state); // is<State>(like isCommited)
                fsm[state.toLowerCase()] = _.bind(fsm.setState, fsm, state); // set<State> mthod(like committed() will set current state to committed)
            });
            fsm.setState(defn.initialState);
        },
        context: function () {
            return fsm.defn.context;
        },
        sendEvent: function handler(event) {
            if(fsm.isFinal()) {
                console.warn("The FSM is already in a final state %s, won;t allow any transitions", fsm.currentState);
                return fsm.currentState; // no change in state we are already in the final one.
            }
            var execFn = fsm.defn.transitionFn; // use the global transition fucntion.
        // is there a custom transition functon for this fsm?
            console.log("Looking for the custom TransitionFn", fsm.currentState.toLowerCase()+"TransitionFn");
            if(_.isFunction(fsm.defn[fsm.currentState.toLowerCase()+"TransitionFn"])){
                execFn = fsm.defn[fsm.currentState.toLowerCase()+"TransitionFn"];
            }
            var nextState = execFn.apply(fsm, [event, fsm.defn.context]);
            if(_.isEmpty(nextState) || !fsm.isValidState(nextState)){
                nextState = fsm.noStateChange();
            }
            return fsm.setState(nextState);
        },
        isFinal: function () {
            return _.contains(fsm.defn.finalStates, fsm.currentState);
        },
        noStateChange: function () {
            return fsm.currentState;
        },
        isValidState: function (state) {
            return _.contains(fsm.defn.states, state);
        },

        // timeout can be timeout: {value: <time in ms to wait>, nextState: '<state> onTimeout go to this state'}
        setState: function (nextState, timeout) {
            var newState = nextState;
            if(fsm.isFinal()){
                return fsm.currentState; // thre will be no state change after a final state is reached.
            }
            if(_.isObject(nextState)){
                newState = nextState.state;
                timeout = nextState.timeout;
            }

            if(fsm.currentState !== newState ) { // if there is actually a state change
                resetTimeoutIfAny();
                if(_.isFunction(fsm.defn['on'+humanize(newState)])) {
                    // send the notification that state is going to change. The notification is async, and we will not wait for the response.
                    fsm.defn['on'+humanize(newState)].apply(fsm, [fsm.defn.context]);
                }
                setupTimeoutIfAny(timeout);
            }
            return (fsm.currentState = newState);
        },
        reset: function () {
            fsm.defn ={};
            resetTimeoutIfAny();
        }
    };
    fsm.init(defn);
    return fsm;
};
