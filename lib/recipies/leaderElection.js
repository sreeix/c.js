"use strict";
var zookeeper = require('node-zookeeper-client');
var zkPath = require('../zkPath');
var locker = require('./lock');

// the protocol is very similar to a lock. leadership is actually a lock that lasts as long as the node exists
// Failure modes: Leader is
module.exports = function (client) {
    var zkLib = require('../zkLib')(client);

    return {
        requestLeadership: function  (path, cb) {
            var leaderLock =  locker(client).at(path);
            leaderLock.lock(function (err, releaseFn) {
                if(err) {
                    return cb(err);
                }
                return cb(null, releaseFn);
            });
        }
    };
};
