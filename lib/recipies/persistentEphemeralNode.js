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

Also it will add itself back if it is explicitly deleted. This is useful in cases where zk parent nodes are removed explictly(for cleanup)

This *may* not be a good usecase for certain other usecases(like locks)

There is actually only one connection listener that handles multiple ephemeral nodes, so this is not really a heavy usage.

Also note that this will not work for currently for sequential ephemeral nodes.
*/
module.exports = function(client) {
    var zkLib = require('../zkLib')(client);
    var ephemeralPaths = [];
    var active = false;
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
                        console.log("Got path stat", pathStat);
                        if(pathStat) {
                            console.log("The path exists... Did some other node set it up?");
                            return Promise.resolve();
                        }
                        return makeNode(pathObj);
                    });
                });
            }
            if(state === zookeeper.State.DISCONNECTED){
                return console.log("State is disconnected, now we don't have access to the node");
            }
            if(state === zookeeper.State.CONNECTED_READ_ONLY){
                return console.log("State is connected, but is in read only mode. Can't create");
            }
            if(state === zookeeper.State.EXPIRED){
                // the right thing to do here is to reconnect, which will send the connected event, and then recreate the nodes.
                //but we'll let the reconnect happen only once and not here....
                return console.log("Session expired. WIll wait for reconnection and then recreate the node and watches");
            }
            return console.log("Got State", state);
        });
    };
    var closeNotifications = function (path) {
        return (ephemeralPaths = _.reject(ephemeralPaths, _.partial(_.isEqual, path)));
    };

    var addNotificationPath = function (pathInfo) {
        console.log("Adding path info", pathInfo);
        if(!_.findWhere(ephemeralPaths, {path: pathInfo.path})) {
            ephemeralPaths.push(pathInfo);
        }
    };
    var makeNode = function (pathInfo) {
        var path = pathInfo.path, data = pathInfo.data, mode = pathInfo.mode;
        return zkLib.ensurePath(zkPath.parent(path)).then(function (parent) {
            var node = zkPath.childNode(path);
            return data ? zkLib.create(path, new Buffer(data),  mode): zkLib.create(path, mode);
        });
    };
    var watchPath = function (path) {
        return client.existsAsync(path, function watchme(event) {
            if(event.name === 'NODE_DELETED') {
                var match = _.findWhere(ephemeralPaths, {path: event.path});
                if(match){
                    console.log("Path matched an existing ep node, that was deleted, recreating it", match);
                    return makeNode(match).then(watchPath);
                }
            }
            console.log("Got event", event);
        }).return(path);
    };

    setupNotifications();

    return {
        create: function (path, data, mode) {
            // path is mandatory the rest are defaulted data to null and mode to EPHEMERAL

            mode = mode || zookeeper.CreateMode.EPHEMERAL;
            var x = {path: path,
                     close: function () {
                         if(x.active) {
                             x.active = false;
                             closeNotifications(path);
                         }
                     },
                     active: true
                    };
            var pathInfo = {path: path, data: data, mode: mode};
            console.log("-------------------------------", pathInfo);
            return makeNode(pathInfo).then(function (path) {
                pathInfo.mode = zookeeper.CreateMode.EPHEMERAL; // turn into a ephemeral node the next time, even if it is a sequential node, this will ensure that same node is created again on failure.
                return x.path = pathInfo.path = path; // needed in case of sequential ephemeral nodes
            }).then(watchPath)
                .then(_.partial(addNotificationPath, pathInfo))
                .return(x);
        }
    };
};
