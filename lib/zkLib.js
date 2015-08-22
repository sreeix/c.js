var Promise = require('bluebird');
var zookeeper = require('node-zookeeper-client');

module.exports = function (client) {
    Promise.promisifyAll(client);
    return {
        ensurePath: function (path) {
            return client.mkdirpAsync(path);
        },
        create : function (path, data, type) {
            return client.createAsync(path, data, type);
        },
        remove: function (path) {
            return client.removeAsync(path);
        },
        getChildren: function(path){
            return new Promise(function (resolve, reject) {
                client.getChildren(path, function (err, children, stat) {
                    if(err) {
                        return reject(err);
                    }
                    console.log("got children", children);
                    return resolve(children);
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
                    };
                });
            });
            client.existsAsync(path, onChange).then(function (stat) {
                if(stat) {
                    console.log("Watcher registered on node", path);
                } else {
                    console.log("Node no longer exists.");
                }
            }).catch(function  (err) {
                console.log("Failed to set watch on the node", path, err);

            });
        }
    };
};
