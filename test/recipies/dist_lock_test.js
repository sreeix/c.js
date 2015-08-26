"use strict";
var zookeeper = require('node-zookeeper-client');

var locker = require('../../lib/recipies/lock');

var _ = require('underscore');

describe('Lock', function () {
    var client;
    var zkLib;
    var createLockNode =  function  () {
        return zkLib.create('/bjn/tests/test-locks/lock-',  new Buffer('lock'),  zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL);
    };

    beforeEach(function(done) {
        client = zookeeper.createClient('localhost:2181');
        client.connect();
        client.once('connected', function () {
            console.log("Connected to zookeeper");
            zkLib = require('../../lib/zkLib')(client);
            done();
        });
    });
    afterEach(function () {
        console.log("Closing Zk Connection");
        client.close();
    });


    describe("lock", function () {
        this.timeout(10000);

       it('should create locks when no other exists', function (done) {
            var lock = locker(client).at('/bjn/tests/test-locks');
            lock.lock(function (err, releaseFn) {
                if(err) {
                    // Is i mandatory to call release fn, maybe not.
                    return done(err);
                }
                console.log("Lock acquired... Now doing magic");
                releaseFn();
                return done();
            });
        });
        it('should wait for single lock', function (done) {
            var lockReleased , lockAcquired;
            var lock = locker(client).at('/bjn/tests/test-locks');
            // create another node for the lock key so that we block on that.
            createLockNode().then(function (lockPath) {
                setTimeout(function () {
                    console.log("releasing previous lock");
                    zkLib.remove(lockPath);
                    lockReleased = new Date();
                }, 3000);
                return true;
            }).then(function  (l) {
                lock.lock(function (err, releaseFn) {
                    lockAcquired = new Date();
                    if(err) {
                        // Is i mandatory to call release fn, maybe not.
                        return done(err);
                    }
                    releaseFn();
                    (lockReleased < lockAcquired).should.equal(true);
                    return done();
                });
            });
        });

        it('should wait for single lock', function (done) {
            var lockReleased , lockAcquired;
            var lock = locker(client).at('/bjn/tests/test-locks');
            // create another node for the lock key so that we block on that.
            createLockNode().then(function (lockPath) {
                setTimeout(function () {
                    console.log("releasing previous lock");
                    zkLib.remove(lockPath);
                    lockReleased = new Date();
                }, 3000);
                return true;
            }).then(function  (l) {
                lock.lock(function (err, releaseFn) {
                    lockAcquired = new Date();
                    if(err) {
                        // Is i mandatory to call release fn, maybe not.
                        return done(err);
                    }
                    releaseFn();
                    (lockReleased < lockAcquired).should.equal(true);
                    return done();
                });
            });
        });

        it('should wait for sequencial locks', function (done) {
            var lock1Released , lock2Released, lockAcquired;
            var lock = locker(client).at('/bjn/tests/test-locks');
            // create another node for the lock key so that we block on that.
            createLockNode().then(function (lockPath) {
                setTimeout(function () {
                    console.log("releasing", lockPath);
                    zkLib.remove(lockPath);
                    lock1Released = true;
                }, 2000);
                return true;
            }).
                then(createLockNode).then(function (lockPath) {
                    setTimeout(function () {
                        console.log("releasing", lockPath);
                        zkLib.remove(lockPath);
                        lock2Released = true;
                    }, 4000);
                    return true;
                }).then(function  (l) {
                    lock.lock(function (err, releaseFn) {
                        lock1Released .should.equal(true);
                        lock2Released .should.equal(true);

                        lockAcquired = new Date();
                        if(err) {
                            // Is i mandatory to call release fn, maybe not.
                            return done(err);
                        }
                        releaseFn();
                        return done();
                    });
                });
        });


        // in this case the lock 2 is gone(crashed?), but lock1 stays for a much longer time. so we need to wait for the lock 1 to be cleared as well, before acquiring the lock.
        it('should wait for reverse sequencial locks', function (done) {
            var lock1Released , lock2Released, lockAcquired;
            var lock = locker(client).at('/bjn/tests/test-locks');
            // create another node for the lock key so that we block on that.
            createLockNode().then(function (lockPath) {
                setTimeout(function () {
                    console.log("releasing", lockPath);
                    zkLib.remove(lockPath);
                    lock1Released = true;
                }, 4000);
                return true;
            }).
                then(createLockNode).then(function (lockPath) {
                    setTimeout(function () {
                        console.log("releasing", lockPath);
                        zkLib.remove(lockPath);
                        lock2Released = true;
                    }, 1000);
                    return true;
                }).then(function  (l) {
                    lock.lock(function (err, releaseFn) {
                        lock1Released .should.equal(true);
                        lock2Released .should.equal(true);
                        lockAcquired = new Date();
                        if(err) {
                            // Is i mandatory to call release fn, maybe not.
                            return done(err);
                        }
                        releaseFn();
                        return done();
                    });
                });
        });
    });
});
