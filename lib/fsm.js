"use strict";
var _ = require('underscore');
var humanize = require("underscore.string/humanize");


var checkStateFunction = function (state) {
    return this.currentState === state;
};
var setState = function (state) {
    return (this.currentState = state);
};

var fsm = {
    init: function (defn) {
        fsm.defn = defn;
        _.each(defn.states, function (state) {
            fsm['is'+humanize(state)] = _.bind(checkStateFunction,  fsm, state);
            fsm[state.toLowerCase()] = _.bind(setState, fsm, state);
        });
        fsm.currentState = defn.initialState;
    },
    sendEvent: function handler(event) {
        var execFn = fsm.defn.transitionFn;
        if(_.isFunction(fsm.defn[fsm.currentState.toLowerCase()+"TransitionFn"])){
            execFn = fsm.defn[fsm.currentState.toLowerCase()+"TransitionFn"];
        }
        var nextState = execFn(event, fsm.defn.context);
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
    }
};

module.exports = fsm;
