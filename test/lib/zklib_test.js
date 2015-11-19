"use strict";
var Promise = require('bluebird');
var zookeeper = require("node-zookeeper-client");

var should = require('should');
var _ = require('underscore');

describe("zk library", function() {
    var client;
    var zkLib;
    var testRoot = "/test-temp";

    beforeEach(function(done) {
        Promise.longStackTraces();
        client = zookeeper.createClient('localhost:2181');
        client.connect();
        client.once('connected', function () {
            console.log("Connected to zookeeper");
            zkLib = require('../../lib/zkLib')(client);
            done();
        });
    });

    afterEach(function (done) {
        console.log("Cleaning up");
        zkLib.rmr(testRoot).then(function () {
            console.log("Closing Zk Connection");
            client.close();
            return done();
        }).catch(done);
    });

    xdescribe("rmr", function() {
        it("removes single node", function(done) {
            return client.create(testRoot, new Buffer("test"), zookeeper.CreateMode.EPHEMERAL,
                                 function (err, path) {
                                     if(err) {return done(err);}
                                     return zkLib.rmr(testRoot).then(function () {
                                         return done();
                                     }).catch(done);
                                 });

        });
        it("removes one level deep root", function(done) {
            return client.create(testRoot, new Buffer("test"), zookeeper.CreateMode.PERSISTENT,
                                 function (err, path) {
                                     if(err) {return done(err);}
                                     return client.create(testRoot + '/foo',
                                                          new Buffer('test'),
                                                          zookeeper.CreateMode.PERSISTENT, function  (err) {
                                                              if(err) {return done(err);}
                                                              return zkLib.rmr(testRoot).then(function () {
                                                                  client.exists(testRoot, function (err, stat) {
                                                                      if(err) {return done(err);}
                                                                      console.log("Did we find the stat", stat);
                                                                      should(stat).be.null(); // stat ok means that node exists, after rmr it should be gone
                                                                      return done();
                                                                  })

                                                              }).catch(done);
                                                          });
                                 });

        });

        it("removes multilevel node", function(done) {

            return client.mkdirp(testRoot +'/is/a/very/long/chain/so/child/is/very/far',
                                 new Buffer("test"), zookeeper.CreateMode.PERSISTENT,
                                 function (err, path) {
                                     if(err) {return done(err);}
                                     return zkLib.rmr(testRoot).then(function () {
                                         client.exists(testRoot, function (err, stat) {
                                             if(err) {return done(err);}
                                             console.log("Did we find the stat", stat);
                                             should(stat).be.null(); // stat ok means that node exists, after rmr it should be gone
                                             return done();
                                         });

                                     }).catch(done);
                                 });

        });


        it("removes non existant node", function(done) {
            return client.create(testRoot, new Buffer("test"), zookeeper.CreateMode.EPHEMERAL,
                                 function (err, path) {
                                     if(err) {return done(err);}
                                     return zkLib.rmr('/foo').then(function () {
                                         return done();
                                     }).catch(done);
                                 });

        });

    });

    describe("watchChildren", function () {
        xit("watches no children of empty node", function(done) {
            client.create(testRoot, new Buffer("test"), zookeeper.CreateMode.EPHEMERAL, function (err, path) {
                zkLib.watchAllChildren(testRoot, {recursive: false, times: 1, added:false, deleted:false}, function  watcher(event) {
                    // there will be a delete notification of the root.
                    if(event.name === 'NODE_DELETED') {
                        return true;
                    }
                    return done("were not expecting a watch invocation");
                }).then(function (children) {
                    children.length.should.equal(0);

                }).delay(100).then(function (value) {
                    done();
                }).catch(done);
            });
        });

        xit("watches one node addition", function(done) {
            var watchCount = 0;
            client.create(testRoot, new Buffer("test"), zookeeper.CreateMode.PERSISTENT, function (err, path) {
                zkLib.watchAllChildren(testRoot, {recursive: false, times: 1, added:false, deleted:false}, function watcher(event) {
                    console.log("********got event", event);
                    if(event.name !== 'NODE_DELETED') {
                        watchCount++;
                    }
                }).then(function (children) {
                    children.length.should.equal(0);
                }).then(function name(arg) {
                    return client.createAsync(testRoot+"/foo", zookeeper.CreateMode.PERSISTENT);
                }).delay(100).then(function () {
                    watchCount.should.equal(1);
                    done();
                }).catch(done);
            });
        });

        xit("watches multiple node additions", function(done) {
            var watchCount = 0;
            client.create(testRoot, new Buffer("test"), zookeeper.CreateMode.PERSISTENT, function (err, path) {
                zkLib.watchAllChildren(testRoot,
                                       {recursive: false, times: 1, added:false, deleted:false},
                                       function watcher(event, data) {
                                           console.log("got event", event);
                                           if(data) {
                                               console.log(data.toString());
                                           }
                                           if(event.name !== 'NODE_DELETED') {
                                               watchCount++;
                                           }

                                       }).then(function (children) {
                                           children.length.should.equal(0);
                                       }).then(function () {
                                           return client.createAsync(testRoot+"/foo", zookeeper.CreateMode.PERSISTENT)
                                               .delay(100).then(function () {
                                                   console.log("created bar");
                                                   return client.createAsync(testRoot+"/bar", zookeeper.CreateMode.PERSISTENT);
                                               });
                                       }).delay(100).then(function () {
                                           console.log("Checking watch count", watchCount);
                                           watchCount.should.equal(2);
                                           console.log("done");
                                           done();
                                       }).catch(function  (err) {
                                           console.log("got error", err);
                                           throw err;
                                       });
            });
        });


        xit("watches multiple node deletions", function(done) {
            var childrenChange = false, deleteWatchNotification = false;
            client.create(testRoot, new Buffer("test"), zookeeper.CreateMode.PERSISTENT, function (err, path) {
                zkLib.watchAllChildren(testRoot,
                                       {recursive: false, times: 1, added:false, deleted:false},
                                       function watcher(event, data) {
                                           console.log("got event", event);
                                           if(data) {
                                               console.log(data.toString());
                                           }
                                           if(event.name !== 'NODE_CHILDREN_CHANGED') {
                                               childrenChange = true;
                                           }
                                           if (event.name === 'NODE_DELETED' && event.path==='/test-temp/foo'){
                                               deleteWatchNotification = true;
                                           }

                                       }).then(function (children) {
                                           children.length.should.equal(0);
                                       }).then(function () {
                                           return client.createAsync(testRoot+"/foo", zookeeper.CreateMode.PERSISTENT)
                                               .then(function () {
                                                   console.log("created foo");
                                                   return Promise.delay(100); /// to ensure that 2 watches are not clubbed togather.
                                               }).then(function () {
                                                   console.log("removed foo");
                                                   return client.removeAsync(testRoot+"/foo", zookeeper.CreateMode.PERSISTENT);
                                               });
                                       }).delay(100).then(function () {
                                           childrenChange.should.equal(true);
                                           deleteWatchNotification.should.be.ok();
                                           console.log("done");
                                           done();
                                       }).catch(function  (err) {
                                           console.log("got error", err);
                                           throw err;
                                       });
            });
        });


        it("watches multiple level nodes", function(done) {
            var watchCount = 0;
            client.create(testRoot, new Buffer("test"), zookeeper.CreateMode.PERSISTENT, function (err, path) {
                zkLib.watchAllChildren(testRoot,
                                       {recursive: false, times: 1, added:false, deleted:false},
                                       function watcher(event, data) {
                                           console.log("got event", event);
                                           if(data) {
                                               console.log(data.toString());
                                           }
                                           watchCount++;
                                       }).delay(100).then(function () {
                                           console.log("Creating the /foo/bar node");
                                           return client.mkdirpAsync(testRoot+"/foo/bar", zookeeper.CreateMode.PERSISTENT)
                                               .delay(100)
                                               .then(function () {
                                                   console.log("removed foo");
                                                   return client.removeAsync(testRoot+"/foo/bar", zookeeper.CreateMode.PERSISTENT);                                               });
                                       }).delay(100).then(function () {
                                           console.log("Checking watch count", watchCount);
                                           watchCount.should.equal(2);
                                           console.log("done");
                                           done();
                                       }).catch(function  (err) {
                                           console.log("got error", err);
                                           throw err;
                                       });
            });
        });
    });
});
