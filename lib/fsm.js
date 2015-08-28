"use strict";
var _ = require('underscore');
var humanize = require("underscore.string/humanize");


var checkStateFunction = function (state) {
    return this.currentState === state;
};

// fsm does some magic. For all the state we will create
// is<StateName> method to check for what state we are in.
// <stateName>() method that changes the current state to the specified state.(nostateChange function exists to ensure no state trantition happens from the transition function)
// transitions from state will invoke on<State> function (if defined) to notify users of state changes.
// We expect a transitionFn all the time, but to make this manageable you could implement a <CurrentState>TransitionFn that will be specifically invoked when the current state is that.
var fsm = {
    init: function (defn) {
        fsm.defn = defn;
        _.each(defn.states, function (state) {
            fsm['is'+humanize(state)] = _.bind(checkStateFunction,  fsm, state);
            fsm[state.toLowerCase()] = _.bind(fsm.setState, fsm, state);
        });
        fsm.currentState = defn.initialState;
    },
    sendEvent: function handler(event) {
        var execFn = fsm.defn.transitionFn;
        if(_.isFunction(fsm.defn[fsm.currentState.toLowerCase()+"TransitionFn"])){
            execFn = fsm.defn[fsm.currentState.toLowerCase()+"TransitionFn"];
        }
        var nextState = execFn.apply(fsm, [event, fsm.defn.context]);
        if(_.isEmpty(nextState) || !fsm.isValidState(nextState)){
            nextState = fsm.noStateChange();
        }
        fsm.currentState = nextState;
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
    setState: function (nextState) {
        // send the notification that state is going to change. The notification is async, and we will not wait for the response.
        if(fsm.currentState !== nextState && _.isFunction(fsm.defn['on'+humanize(nextState)])) {
            fsm.defn['on'+humanize(nextState)].apply(fsm, [fsm.context]);
        }
        return (fsm.currentState = nextState);
    }
};

module.exports = fsm;
