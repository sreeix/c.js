"use strict";

var zookeeper = require('node-zookeeper-client');
var _ = require('underscore');
var zkPath = require('../zkPath');
var Promise = require('bluebird');

var returning = require('../promiseHelper').returning;
/**
 This creates a persistent Ephemeral Node on zookeeper. A persistent Ephemeral node is an ephemeral node that is recreated on zookeeper failures.
ie. Lets say a service has registered itself at /foo/bar/my.service(as an ephemeral node for service discovery). When the service actually shutsdown/ crashes
accidentally, the node /foo/bar/my.service is automatically removed. But consider the case that the network disconnected(the service actually stayed up), because zookeeper
cannot differentiate between service crash / network disconnect it would still remove the /foo/bar/my.service node. In such a case persistentEphemeralNode will recreate the node /foo/bar/my.service
when the connection comes back up. This in some cases prevents the need to restart the service to reregister the node.

This may not be a good usecase for certain other usecases(like locks)

There is actually only one connection listener that handles multiple ephemeral nodes, so this is not really a heavy usage.
*/
module.exports = function(client) {
    var zkLib = require('../zkLib')(client);
    var ephemeralPaths = [];

    var setDataIfNeeded = function setDataIfNeeded(path, data) {
        if(data){
            return zkLib.setData(path, data);
        }
        return Promise.resolve(data);
    };

    var setupNotifications = function () {
        client.on('state', function (state) {
            if(state === zookeeper.State.SYNC_CONNECTED){
                return Promise.each(ephemeralPaths, function (pathObj) {
                    return zkLib.exists(pathObj.path).then(function (pathStat) {
                        if(pathStat) {
                            console.log("The path exists... Did some other node set it up?");
                        } else {
                            return zkLib.ensurePath(pathObj.path)
                                .then(_.partial(setDataIfNeeded, pathObj.path, pathObj.data));
                        }
                        return Promise.resolve();
                    });
                });
            }
            if(state === zookeeper.State.DISCONNECTED){
                return console.log("State is disconnected, now we don't have access to the node");
            }
            if(state === zookeeper.State.CONNECTED_READ_ONLY){
                console.log("State is connected, but is in read only mode. Can't create");
            }
            if(state === zookeeper.State.EXPIRED){
                // the right thing to do here is to reconnect, which will send the connected event, and then recreate the nodes.
                //but we'll let the reconnect happen only once and not here....
            }
            return console.log("Got State", state);
        });
    };
    var closeNotifications = function (path) {
        ephemeralPaths = _.drop(ephemeralPaths, function (pathObj) {
            return pathObj.path === path;
        });
    };
    var addNotificationPath = function (path, data) {
        if(!_.contains(ephemeralPaths, path)) {
            ephemeralPaths.push({path: path, data: data});
        }
    };
    setupNotifications();

    return {
        create: function (path, data, cb) {
            if(_.isFunction(data)) {
                cb = data;
                data = null;
            }
            return zkLib.ensurePath(path)
                .then(setDataIfNeeded)
                .then(_.partial(addNotificationPath, path, data))
                .return({path: path, close: closeNotifications})
                .nodeify(cb);
        }
    };
};
