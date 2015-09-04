"use strict";
var zookeeper = require('node-zookeeper-client');
var zkPath = require('../zkPath');
// the protocol is very similar to a lock. leadership is actually a lock that lasts as long as the node exists
// Failure modes: Leader is
module.exports = function (client) {
    var zkLib = require('../zkLib')(client);

    return {
        register: function  (path, onLeader) {
            return zkLib.ensurePath(path).then(function (path) {
                return zkLib.create(zkPath.join(path, 'leader-', new Buffer('leadership'), zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL));
            });
        }
    };
};
