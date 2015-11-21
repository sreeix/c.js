"use strict";
var Promise = require('bluebird');
var zookeeper = require("node-zookeeper-client");

var should = require('should');
var _ = require('underscore');

describe("zk library", function() {
    var client;
    var zkLib, zkPath = require('../../lib/zkPath');
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
        zkLib.rmr(testRoot).then(function () {
            console.log("Closing Zk Connection");
            client.close();
            return done();
        }).catch(done);
    });

    describe("rmr", function() {
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
                                                                      should(stat).be.null(); // stat ok means that node exists, after rmr it should be gone
                                                                      return done();
                                                                  });

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
        var services;
        it("watches no children of empty node", function(done) {
            client.create(testRoot, new Buffer("test"), zookeeper.CreateMode.EPHEMERAL, function (err, path) {
                zkLib.watchAllChildren(testRoot,
                                       {recursive: false, times: 1, added:false, deleted:false},
                                       function  watcher(event) {
                                           // there will be a delete notification of the root.
                                           if(event.name === 'NODE_DELETED') {
                                               return true;
                                           }
                                           return done("were not expecting a watch invocation");
                                       }).then(function (services) {

                                           services.children.length.should.equal(0);
                                           services.data.toString('utf-8').should.equal('test');
                                       }).delay(100).then(function (value) {
                                           done();
                                       }).catch(done);
            });
        });


        it("watches an existing heirarchy", function(done) {
            client.mkdirp(testRoot + "/foo", new Buffer("test"),
                          zookeeper.CreateMode.PERSISTENT,
                          function (err, path) {
                              zkLib.watchAllChildren(testRoot,
                                                     {recursive: false, times: 1, added:false, deleted:false},
                                                     function  watcher(event) {
                                                         // there will be a delete notification of the root.
                                                         if(event.name === 'NODE_DELETED') {
                                                             return true;
                                                         }
                                                     }).then(function (services) {
                                                         services.children.length.should.equal(1);
                                                         services.children[0].children.length.should.equal(0);
                                                         services.children[0].data.toString('utf-8').should.equal('test');
                                                         services.data.toString('utf-8').should.equal('test');
                                                     }).delay(100).then(function (value) {
                                                         done();
                                                     }).catch(done);
                          });
                });

        it("watches one node addition", function(done) {
            var watchCount = 0
            client.create(testRoot, new Buffer("test"), zookeeper.CreateMode.PERSISTENT, function (err, path) {
                zkLib.watchAllChildren(testRoot,
                                       {recursive: false, times: 1, added:false, deleted:false},
                                       function watcher(event) {
                                           if(event.name !== 'NODE_DELETED') {
                                               watchCount++;
                                           }
                                       }).then(function (s) {
                                           services = s;
                                           services.children.length.should.equal(0);
                                       }).then(function (arg) {
                                           return client.createAsync(testRoot+"/foo", zookeeper.CreateMode.PERSISTENT);
                                       }).delay(100).then(function () {
                                           watchCount.should.equal(1);
                                           services.children.length.should.equal(1);
                                           should(services.children[0].data).be.equal(null);
                                           done();
                                       }).catch(done);
            });
        });

        it("watches multiple node additions", function(done) {
            var watchCount = 0;
            client.create(testRoot, new Buffer("test"), zookeeper.CreateMode.PERSISTENT, function (err, path) {
                zkLib.watchAllChildren(testRoot,
                                       {recursive: false, times: 1, added:false, deleted:false},
                                       function watcher(event, data) {
                                           if(data) {
                                               console.log(data.toString());
                                           }
                                           if(event.name !== 'NODE_DELETED') {
                                               watchCount++;
                                           }

                                       }).then(function (s) {
                                           services = s;
                                           s.children.length.should.equal(0);
                                       }).then(function () {
                                           return client.createAsync(testRoot+"/foo", zookeeper.CreateMode.PERSISTENT)
                                               .delay(100).then(function () {
                                                   return client.createAsync(testRoot+"/bar", new Buffer("bar"), zookeeper.CreateMode.PERSISTENT);
                                               });
                                       }).delay(100).then(function () {
                                           watchCount.should.equal(2);
                                           should(services.children.length).be.equal(2);
                                           done();
                                       }).catch(function  (err) {
                                           console.log("got error", err);
                                           throw err;
                                       });
            });
        });


        it("watches multiple node deletions", function(done) {
            var childrenChange = false, deleteWatchNotification = false;
            client.create(testRoot, new Buffer("test"), zookeeper.CreateMode.PERSISTENT, function (err, path) {
                zkLib.watchAllChildren(testRoot,
                                       {recursive: false, times: 1, added:false, deleted:false},
                                       function watcher(event, data) {
                                           if(event.name !== 'NODE_CHILDREN_CHANGED') {
                                               childrenChange = true;
                                           }
                                           if (event.name === 'NODE_DELETED' && event.path==='/test-temp/foo'){
                                               deleteWatchNotification = true;
                                           }

                                       }).then(function (s) {
                                           services = s;
                                           services.children.length.should.equal(0);
                                       }).then(function () {
                                           return client.createAsync(testRoot+"/foo", zookeeper.CreateMode.PERSISTENT)
                                               .delay(100)
                                               .then(function () {
                                                   return client.removeAsync(testRoot+"/foo", zookeeper.CreateMode.PERSISTENT);
                                               });
                                       }).delay(100).then(function () {
                                           childrenChange.should.equal(true);
                                           deleteWatchNotification.should.be.ok();
                                           services.children.length.should.equal(0);
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
                                           watchCount++;
                                       }).then(function  (s) {
                                           services = s;

                                       }).delay(500).then(function () {
                                           return client.mkdirpAsync(testRoot+"/foo/bar", zookeeper.CreateMode.PERSISTENT)
                                               .delay(200)
                                               .then(function () {
                                                   return client.removeAsync(testRoot+"/foo/bar", zookeeper.CreateMode.PERSISTENT);
                                               });
                                       }).delay(500).then(function () {
                                           console.log("Checking watch count", watchCount);
                                           done();
                                       }).catch(function  (err) {
                                           console.log("got error", err);
                                           throw err;
                                       });
            });
        });


        it("watches for changes on newly added nodes", function(done) {
            var watchCount = 0;
            client.create(testRoot, new Buffer("test"), zookeeper.CreateMode.PERSISTENT, function (err, path) {
                zkLib.watchAllChildren(testRoot,
                                       {recursive: false, times: 1, added:false, deleted:false},
                                       function watcher(event, data) {

                                           watchCount++;
                                       }).then(function (s) {
                                           services = s;

                                       }).delay(500).then(function () {
                                           return client.mkdirpAsync(testRoot+"/foo/bar/baz", zookeeper.CreateMode.PERSISTENT).then (function () {
                                               return client.createAsync(testRoot+"/foo/bar/baz/k", new Buffer("bar-baz-k"), zookeeper.CreateMode.PERSISTENT)
                                           }).then(function () {
                                               return client.mkdirpAsync(testRoot+"/foo/bing/bong", zookeeper.CreateMode.PERSISTENT).then (function () {
                                                   return client.createAsync(testRoot+"/foo/bing/bong/k", new Buffer("bing-bong-k"), zookeeper.CreateMode.PERSISTENT)
                                               })
                                           }).delay(1000).then(function () {
                                               var foo = services.children[0];
                                               var bing = _.find(services.children[0].children, function  (c) {
                                                   return zkPath.childNode(c.path) === 'bing';
                                               });
                                               var bar = _.find(services.children[0].children, function  (c) {
                                                   return zkPath.childNode(c.path) === 'bar';
                                               });

                                               should(foo.children.length).be.equal(2);

                                               should(bing).be.ok();
                                               should(bar).be.ok();
                                               should(bar.children[0].children[0].data).be.equal('bar-baz-k');
                                               should(bing.children[0].children[0].data).be.equal('bing-bong-k')
                                               return client.setDataAsync(testRoot+"/foo/bing/bong/k", new Buffer('updated-bing-bong-k')).delay(100).then(function () {

                                                   should(bing.children[0].children[0].data).be.equal('updated-bing-bong-k');
                                               });

                                           }).then(function() {
                                               return done();
                                           }).catch(function  (err) {
                                               console.log("got error", err);
                                               throw err;
                                           });
                                       });
            });
        });
        it("watches data changes on node", function(done) {
            var watchCount = 0;
            client.create(testRoot, new Buffer("test"), zookeeper.CreateMode.PERSISTENT, function (err, path) {
                zkLib.watchAllChildren(testRoot,
                                       {recursive: false, times: 1, added:false, deleted:false},
                                       function watcher(event, data) {
                                           watchCount++;
                                       }).then(function  (s) {
                                           services = s;
                                       }).delay(500).then(function () {
                                           return client.mkdirpAsync(testRoot+"/foo/bar", zookeeper.CreateMode.PERSISTENT)
                                               .delay(200)
                                               .then(function () {
                                                   return client.setDataAsync(testRoot+"/foo/bar", new Buffer('Yo'));
                                               });
                                       }).delay(500).then(function () {
                                           should(services.children[0].children[0].path).be.equal("/test-temp/foo/bar");
                                           should(services.children[0].children[0].data).be.equal("Yo");
                                       }).then(function () {
                                           return client.setDataAsync(testRoot+"/foo/bar", new Buffer('Yo Data'));
                                       }).delay(100).then(function () {
                                           should(services.children[0].children[0].data).be.equal("Yo Data");
                                       }).then(function () {
                                           return client.setDataAsync(testRoot+"/foo/bar", new Buffer(''));
                                       }).delay(100).then(function () {
                                           should(services.children[0].children[0].data).not.be.ok();
                                           done();
                                       }).catch(function  (err) {
                                           console.log("got error", err);
                                           throw err;
                                       });
            });
        });
    });
});
