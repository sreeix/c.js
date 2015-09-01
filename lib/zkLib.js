"use strict";
var Promise = require('bluebird');
var zkPath = require('./zkPath');
var zookeeper = require('node-zookeeper-client');
var _ = require('underscore');

module.exports = function (client) {
    Promise.promisifyAll(client);
    return {
        ensurePath: _.bind(client.mkdirpAsync, client),
        create : _.bind(client.createAsync, client),
        remove: _.bind(client.removeAsync, client),
        setData: function (path, data) {
            return client.setDataAsync(path, new Buffer(data)).catch(function () {
                return console.log("WARN: could not update %s with %s", path, data);
            });
        },
        exists: _.bind(client.existsAsync, client),
        getData:_.bind(client.getDataAsync, client),
        getChildren: function(path){
            return new Promise(function (resolve, reject) {
                client.getChildren(path, function (err, children, stat) {
                    if(err) {
                        return reject(err);
                    }
                    return resolve(children);
                });
            });
        },
        rmPath: function  (path) {
            return client.removeAsync(path).catch(function  (err) {
                console.log("Warn: deletion of path failed", path);
                return ;
            });
        },
        // this is not the same as get children and watch on the parent, it will actually get all children, and then apply
        // watcher on each of the children.
        watchAllChildren: function (path, onWatch) {
            return client.getChildrenAsync(path).get(0).then(function (children) {
                return Promise.each(children, function (child) {
                    return client.getDataAsync(zkPath.join(path, child), onWatch).get(0);
                });
            });
        },
        watchDeleted: function (path) {
            console.log("Watching on ", path);
            return new Promise(function (resolve, reject) {
                client.exists(path, function  (event) {
                    // iff successful then we wait for the deletion of the node. and resolve
                    console.log('Got event: %s.', event);
                    if(event.getType() === zookeeper.Event.NODE_DELETED) {
                        return resolve();
                    }
                    // else wait for ever, or the timeout
                }, function (err, stat) {
                    if(err){
                        console.log("Failed to set watch on ", path, err);
                        return reject(err);
                    }
                    if(!stat) {
                        console.log("Node no longer exists, move on");
                        return resolve();
                    }
                });
            });
        }
    };
};
