"use strict";
var Promise = require('bluebird');
var zkPath = require('./zkPath');
var zookeeper = require('node-zookeeper-client');
var promiseHelper = require('./promiseHelper');
var _ = require('underscore');

// All methods of this zk class are retired, the options can define how zk reties are handled, by default we do a liner 3 reties before giving up.

function zkErrorsToRetry(err) { // this is a total hack to avoild coupling promise retries with the zookeeper retries, basically handles the zk specific retriy or not
    var code;
    console.log("**********", err);
    if(err && err.getCode) {
        code = err.getCode();
        return (code === zookeeper.Exception.API_ERROR ||
                code === zookeeper.Exception.OPERATION_TIMEOUT || // if it timed out.
                code === zookeeper.Exception.SYSTEM_ERROR // something server side failed
               );
    }
    return true; // retry by default.
}

// probably turn it into a standard callback and then just nodeify for getting a promise. Otherwise this is too specific for a promise method
// But this is a complicated method. If the node does not exist then we immediately resolve the promise.
// if we fail to set watch on the path for some reason we'd reject the promise.
// If we set the watch successfully then we resolve the promise only when the watch is deleted. Othewise the promise is not resolved.
// We do not support any timeout on this function(we probably should). A persistentWatch variable is used to gracefully handle zookeeper disconnects..
// THe only case we currently do not handle is the case of session disconnect in which case we would not reconnect. (We should)
function registerDeletionWatch(client, path, opts, notifier, rejectFn, resolveFn) {
    console.log("registeering watch for path", path, opts);
    return client.exists(path, function (event) {
        // iff successful then we wait for the deletion of the node. and resolve
        console.log('Got event: %s.', event);
        if(event.getType() === zookeeper.Event.NODE_DELETED) {
            return resolveFn();
        }
            // else wait for ever, or the timeout
        return null;
    }, function (err, stat) {
        if(err){
            console.log("Failed to set watch on ", path, err);
            return rejectFn(err);
        }
        if(!stat) {
            console.log("Node no longer exists, move on");
            return resolveFn();
        }
        // if persistentWatch then set up watcher to watch for connection changes.
        if(opts.persistentWatch) {
            // register back on reconnect.
            notifier.onReconnect(_.partial(registerDeletionWatch, client, path, opts, notifier, rejectFn, resolveFn));
        }

        return null;// if we set the watch successfully then wait for the node deletion even to occur.
    });
}


module.exports = function (client, opts) {
    var options = _.defaults({},
                             opts,
                             {retry: {
                                 type: 'linear',
                                 errorFilter: zkErrorsToRetry }
                             });
    // this is messy. Having multi args true seems to be making single argument functions
    // get an array. will need to relook this and remove multiargs. In such cases, look
    // for exists get(0)..
    Promise.promisifyAll(client, {multiArgs: true});

    var retryFn = function (fnToWrap, context) {
        return _.wrap(fnToWrap, function (promiseFn) {
            var ps = _.rest(_.toArray(arguments), 1);
            var execPromise = function() {
                context = context || null;
                return promiseFn.apply(context, ps);
            };
            return promiseHelper.withRetry(execPromise, options.retry);
        });
    };

    var l = {
        ensurePath: retryFn(client.mkdirpAsync, client),
        create : retryFn(client.createAsync, client),
        remove: retryFn(client.removeAsync, client),
        rmr: retryFn(function removeRecursive(path) {
            // patch the path and remove the leaves and go upward.
            return l.exists(path).get(0).then(function (stat) {
                if(stat) {
                    console.log(path, "Exists");
                    return client.getChildrenAsync(path).spread(function (children, stats) {
                        return Promise.each(children, function (child) {
                            return removeRecursive(zkPath.join(path,child));
                        });
                    }).then(function removeRoot() {
                        return client.removeAsync(path);
                    });
                } else {
                    return Promise.resolve();
                }
            });
        }),
        setData: function (path, data) {
            return client.setDataAsync(path, new Buffer(data)).catch(function () {
                return console.log("WARN: could not update %s with %s", path, data);
            });
        },
        exists: retryFn(client.existsAsync, client),
        getData: retryFn(client.getDataAsync, client),
        getChildren: retryFn(function(path){
            return new Promise(function (resolve, reject) {
                client.getChildren(path, function (err, children, stat) {
                    if(err) {
                        return reject(err);
                    }
                    return resolve(children);
                });
            });
        }),
        rmPath: retryFn(function (path) {
            return client.removeAsync(path).catch(function  (err) {
                console.log("Warn: deletion of path failed", path);
                return null;
            });
        }),
        /**
         * Data watchers get node deleted and node_data_changed events.
         **/
        getAndWatchNodeData: function (path, watcher, x) {
            return client.getDataAsync(path, function  dataWatcher(event) {
                if(event.name === 'NODE_DELETED') {
                    // no need to register the watch. Just get it out of the list
                    // it's actually ok for the child to not be there, this may be because 1) multiple watchers 2) child watchers also get thsi.
                    x.children = _.reject(x.children, function (c) {
                        return x.path === event.path;
                    });

                } else {
                    l.getAndWatchNodeData(path, watcher, x);
                }
                watcher(event);
            }).spread(function (data, stat) {
                x.data = data ?  data.toString('utf-8') : null;
                return [data, stat];
            });
        },
        /**
         * Children watchers get node_deleted and node_children_changed events.
         **/
        getAndWatchNodeChildren: function (path, options, watcher, x) {
            return client.getChildrenAsync(path, function childrenwatcher (event){
                if(event.name !== 'NODE_DELETED') { // this means that self is deleted. In this case we do not do anything as we assume that the self watcher also gets it and that cleans up the node.
                    // this is a hammer, we need to register the watcher only on the newly added nodes, otherwise we will have way too many callbacks.
                    l.getAndWatchNodeChildren(event.path, options, watcher, x);
                }
                watcher(event);
            }).get(0).map(function(child) {
                // the child may already exist in the children and in such case we can skip adding the watcher on it. Just add watchers to the new nodes only.
                var existingChild = _.find(x.children, function (item) {
                    return item.path === zkPath.join(path, child);
                });
                if(existingChild) {
                    return existingChild;
                } else {
                    return l.addSelfAndChildWatcher(zkPath.join(path, child), options, watcher);
                }

            }).then(function (all) {
                x.children = all;
            });
        },
        // this is not the same as get children and watch on the parent, it will actually get all children, and then apply
        // watcher on each of the children.
        // following options can be provided
        // {times: 1, added: true, deleted: true, depth: 3, data: true} or {depth: Infinity, times: Infinity} this will keep watching the node/nodes forever. Default is once only(just like regular zookeeper watches)
        addSelfAndChildWatcher: function (path, options, onWatch, x) {
            var root = x || {path: path, data: null, children: []};
            var dataPromise = Promise.resolve();
            if(options.data) {
                dataPromise = l.getAndWatchNodeData(path, onWatch, root);
            }
            return dataPromise.then(function () {
                return l.getAndWatchNodeChildren(path, options, onWatch, root);
            }).return(root);
        },
        watchNonExistantNode: function (path, options, onWatch) {
            var x = {path: path, data: null, children: []};
            return l.exists(path, function  existsWatcher(event) {
                // ignore the rest of the events. The watcher is triggered for
                // deleted and data changed.
                // only if the node is created, watch all the children and flow from there.
                if(event.name === 'NODE_CREATED') {
                    l.addSelfAndChildWatcher(path, options, onWatch, x); // make this async
                }
                return onWatch();
            }).return(x);
        },
        watchAllChildren: function (path, options, onWatch) {
            var defaultOpts = {times: Infinity, depth: Infinity, data: true};
            if(arguments.length == 1) {
                options = defaultOpts;
            }
            if(arguments.length === 2 && _.isFunction(options)){
                onWatch = options;
                options = defaultOpts;
            }
            onWatch = onWatch || function  (e) {
                console.log(e);
            };
            options = _.defaults(options, defaultOpts);

            // do a fake exists to confirm if it actually exists(don't attach a watcher yet)
            return l.exists(path).get(0).then(function (stat) {
                return  stat ? l.addSelfAndChildWatcher(path, options, onWatch) : l.watchNonExistantNode(path, options, onWatch);
            });

        },
        watchDeleted: function (path, opts) {
            var options = _.defaults({}, opts, {persistentWatch: true, timeout: Number.POSITIVE_INFINITY});
            return new Promise(function (resolve, reject) {
                registerDeletionWatch(client, path, options, l.notifier, reject, resolve);
            });
        }
    };

    l.notifier = {
        hasPreviousDisconnect: false,// this provides us the flag to not call the connection event on the first connect.
        connectSubscribers: [],
        disconnectSubscribers: [],
        onReconnect: function (fn) { // this function is not called the first time. it is called only after the disconnect and reconnect.
            l.notifier.connectSubscribers.push(fn);
        },
        onDisconnect: function (fn) {
            l.notifier.disconnectSubscribers.push(fn);
        }
    };

    client.on('state', function (event) {
        if(event === zookeeper.State.SYNC_CONNECTED){
            if(l.notifier.hasPreviousDisconnect) {
                _.each(l.notifier.connectSubscribers,function(fn) {
                    fn();
                });
            }
            l.notifier.hasPreviousDisconnect = false;
        }
        if(event === zookeeper.State.DISCONNECTED) {
            l.notifier.hasPreviousDisconnect = true;
        }
        if(event === zookeeper.State.EXPIRED) {
            _.each(l.notifier.disconnectSubscribers, function(fn) {
                fn();
            });

            // register a reconnect for the connection. This will ensure that we retry repeatedly.
            //TODO
        }

    });
    return l;
};
