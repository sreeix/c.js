'use strict';
var _ = require('underscore');
var util = require('util');
var Promise = require('bluebird');

var zookeeper = require('node-zookeeper-client');

function lockNumberFromPath (path) {
    return parseInt(_.last(path.split('-')), 10)
}
function gotLock(cb) {
    return new Promise(function (resolve, reject) {
        return cb(null, resolve);
    });
}

module.exports = function(zkClient) {
    var zkLib = require('../zkLib')(zkClient);
    var getLockChildren = function (lockPath) {
        console.log("getting children for ", lockPath);
        return zkLib.getChildren(lockPath).then(function (children) {
            return _.chain(children).sortBy(lockNumberFromPath).map(function  (p) {
                return lockPath+"/"+p;
            }).value();
        });
    };

    return {
        at: function (path) {
            var l = {
                path: null,
                n: Number.NEGATIVE_INFINITY,
                lock: function (timeout, cb) {
                    if(_.isFunction(timeout)) {
                        cb = timeout;
                        timeout =  Number.POSITIVE_INFINITY;
                    }
                    return zkLib.ensurePath(path)
                        .then(function (path) {
                            return zkLib.create(path+'/lock-', new Buffer('lock'),  zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL);
                        })
                        .then(function assignLockNumber(path) {
                            l.path = path;
                            console.log("My lock", l.path);
                            return l.n = lockNumberFromPath(path);
                        })
                        .then(_.partial(getLockChildren, path))
                        .then(function (children) {
                            return _.without(children, l.path);
                        })
                        .then(function watchNext(sortedLocks) {
                            console.log("Sorted without self", sortedLocks);
                            if(_.isEmpty(sortedLocks)) {
                                console.log("No other locks found, taking it")
                                return true;
                            }
                            var lowest = lockNumberFromPath(_.first(sortedLocks));
                            if(lowest >= l.n) {  // this has the lock, as this is the lowest
                                console.log("All other locks are later than this lock. taking it.")
                                return true;
                            }
                            return zkLib.watchDeleted(_.last(sortedLocks)).then(_.partial(watchNext, _.initial(sortedLocks)));
                            // return zkLib.watchDeleted(_.last(sortedLocks),  _.partial(watchNext, _.initial(sortedLocks)));
                        })
                        .then(_.partial(gotLock, cb))
                        .then(function () {
                            console.log("Resolved Now unlocking.");
                            return l.unlock();
                        })
                        .catch(function (err) {
                            console.log("Error happened", err);
                            l.unlock();
                            return cb(err);
                        });
                },
                unlock: function () {
                    zkLib.remove(l.path);
                }
            };
            return l;
        }
    }
};
