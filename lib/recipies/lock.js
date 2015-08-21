'use strict';
var _ = require('underscore');

var zookeeper = require('node-zookeeper-client');

module.exports = function(zkClient) {
    return {
        at: function (path) {

            return {
                lock: function (timeout, cb) {
                    if(_.isFunction(timeout)) {
                        cb = timeout;
                        timeout =  Number.POSITIVE_INFINITY;
                    }
                    return cb(function onDone(arg) {

                        unlock(function () {
                            console.log('Unlocking, done.');
                        });
                    });

                },
                unlock: function (cb) {

                }
            }
        },
        cleanup: function (path) {

        }
    }
};
