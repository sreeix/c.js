"use strict";

var zookeeper = require('node-zookeeper-client');
var _ = require('underscore');
var zkPath = require('../zkPath');
var Promise = require('bluebird');

var returning = require('../promiseHelper').returning;

module.exports = function(client) {
    var zkLib = require('../zkLib')(client);
    var ephemeralPaths = [];

    var setDataIfNeeded = function setDataIfNeeded(path, data) {
        if(data){
            console.log("Data exists setting it.", data);
            return zkLib.setData(path, data);
        }
        return Promise.resolve("");
    };

    var setupNotifications = function () {
        client.on('state', function (state) {
            if(state === zookeeper.State.SYNC_CONNECTED){
                console.log('Zookeeper connected creating the path.');
                return Promise.each(ephemeralPaths, function (pathObj) {
                    console.log("Ensuring path and data for ", pathObj);
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
                console.log("This needs special handling.. need to implement it. ");
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
                .return({path: path, close: closeNotifications}).tap(console.log)
                .nodeify(cb);
        }
    };
};
