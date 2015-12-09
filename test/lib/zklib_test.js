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
            return client.create(testRoot, new Buffer("test"), zookeeper.CreateMode.PERSISTENT,
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
            return client.create(testRoot, new Buffer("test"), zookeeper.CreateMode.PERSISTENT,
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
        describe("with defaults", function() {
            it("watches no children of empty node", function(done) {
                client.create(testRoot, new Buffer("test"), zookeeper.CreateMode.EPHEMERAL, function (err, path) {
                    zkLib.watchAllChildren(testRoot, function  watcher(event) {
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

            it("watches children non existant node", function(done) {
                return zkLib.watchAllChildren(testRoot, function  watcher(event) {
                }).delay(10).then(function (s) {
                    services = s ;
                    services.children.length.should.equal(0);
                    should(services.data).be.not.ok();
                }).then(function  () {
                    return client.mkdirpAsync(testRoot, new Buffer('test'), zookeeper.CreateMode.PERSISTENT);
                }).delay(100).then(function () {
                    should(services.data).be.equal('test');
                }).then(function () {
                    done();
                }).catch(done);
            });


            it("watches an existing heirarchy", function(done) {
                client.mkdirp(testRoot + "/foo", new Buffer("test"),
                              zookeeper.CreateMode.PERSISTENT,
                              function (err, path) {
                                  zkLib.watchAllChildren(testRoot, function  watcher(event) {
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
                    zkLib.watchAllChildren(testRoot, function watcher(event) {
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
                    zkLib.watchAllChildren(testRoot, function watcher(event, data) {
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
                    zkLib.watchAllChildren(testRoot, function watcher(event, data) {
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
            it("applies optimal watchers", function(done) {
                // the idea being even though zk will track it as one watcher, we don't want to watch the same node with different callbacks as it may create a larger footprint on client than needed.

                var childrenWatchCounts = {}, deleteCounts = {};
                childrenWatchCounts[testRoot+"/foo"] = 0;
                childrenWatchCounts[testRoot] = 0;
                deleteCounts[testRoot+'/foo/eaz'] = 0;
                deleteCounts[testRoot+'/foo/bar'] = 0;
                client.create(testRoot, new Buffer("test"), zookeeper.CreateMode.PERSISTENT, function (err, path) {
                    zkLib.watchAllChildren(testRoot,
                                           function watcher(event) {
                                               if(event.name === 'NODE_CHILDREN_CHANGED') {
                                                   childrenWatchCounts[event.path]++;
                                               }
                                               if(event.name === 'NODE_DELETED') {
                                                   deleteCounts[event.path]++;
                                               }

                                           }).then(function (s) {
                                               services = s;
                                           }).delay(1000).then(function () {

                                               var x = [testRoot + '/foo', testRoot+"/foo/bar", testRoot+"/foo/baz", testRoot+'/foo/bazz', testRoot+'/foo/buzz', testRoot+'/foo/caz', testRoot+'/foo/daz', testRoot+'/foo/eaz'];
                                               return Promise.mapSeries(x, function (path) {
                                                   console.log("Creating path", path);
                                                   return client.createAsync(path, zookeeper.CreateMode.PERSISTENT).delay(500);
                                               }).then(function () {
                                                   console.log("now deletiong eaz");
                                                   return client.removeAsync(testRoot+'/foo/eaz');
                                               }).delay(1500).then(function() {
                                                   console.log("The watch cound ===", childrenWatchCounts, deleteCounts);
                                                   childrenWatchCounts[testRoot+"/foo"].should.be.equal(8); // We should see of 1)bar, 2) baz, 3) bazz, 4) buzz, 5) caz, 6) daz, 7, eaz 8 remove eaz
                                                   childrenWatchCounts[testRoot].should.be.equal(1); // We shouls see the 1) foo on the root.
                                                   deleteCounts[testRoot+"/foo/eaz"].should.be.equal(2); // 2 because of the self watch and the children watch.
                                                   deleteCounts[testRoot+"/foo/bar"].should.be.equal(0); // 0 because it's not been deleted yet.
                                               }).then(function () {
                                                   console.log("now deletiong eaz");
                                                   return client.removeAsync(testRoot+'/foo/bar');

                                               }).delay(1000).then(function () {
                                                   deleteCounts[testRoot+"/foo/bar"].should.be.equal(2); // 2 because of the self watch and the children watch.
                                               }).then(function () {
                                                   done();
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
        xdescribe("depth watch", function  () {
            var services;
            it("nothing if depth is 0", function() {
                return client.createAsync(testRoot, new Buffer('xxx'), zookeeper.CreateMode.PERSISTENT)
                    .then(function() {
                        return zkLib.watchAllChildren(testRoot, {depth: 0}, function (evt) {
                            console.log(evt);
                            if(evt.name !== 'NODE_DELETED') {
                                throw new Error('Unexpected event', evt);
                            }
                        });
                    }).then(function (s) {
                        services = s;
                    }).then(function () {
                        services.children.length.should.equal(0);
                    }).then(function () {
                        return client.createAsync(zkPath.join(testRoot, 'foo'));
                    }).delay(100).then(function () {
                        console.log(services);
                        services.children.length.should.equal(0);
                    });
            });

            it("depth is 1", function() {
                var barNode;
                return client.createAsync(testRoot, new Buffer('xxx'), zookeeper.CreateMode.PERSISTENT)
                    .then(function() {
                        return zkLib.watchAllChildren(testRoot, {depth: 1}, function (evt) {
                            // console.log(evt);
                            // if(evt.name !== 'NODE_DELETED') {
                            //     throw new Error('Unexpected event', evt);
                            // }
                        });
                    }).then(function (s) {
                        services = s;
                    }).then(function () {
                        services.children.length.should.equal(0);
                    }).then(function () {
                        return client.createAsync(zkPath.join(testRoot, 'foo'));
                    }).delay(100).then(function () {
                        services.children.length.should.equal(1);
                    }).then(function () {
                        return client.createAsync(zkPath.join(testRoot, 'bar'));
                    }).delay(100).then(function () {
                        services.children.length.should.equal(2);
                    }).then(function () {
                        return client.createAsync(zkPath.join(testRoot, 'bar','car'));
                    }).delay(100).then(function () {
                        barNode = _.find(services.children, function (c) {
                            console.log(c);
                            return c.path === zkPath.join(testRoot,'bar');
                        });
                        services.children.length.should.equal(2);
                        barNode.children.length.should.equal(0);
                    }).then(function () {
                        return client.createAsync(zkPath.join(testRoot, 'bar','car', 'tar'));
                    }).delay(100).then(function () {
                        var carNode = _.find(barNode.children, function (c) {
                            return c.path === zkPath.join(testRoot,'bar','car');
                        });
                        should(carNode).be.not.ok();
                    });
            });


            it("depth is 2", function() {
                var barNode;
                return client.createAsync(testRoot, new Buffer('xxx'), zookeeper.CreateMode.PERSISTENT)
                    .then(function() {
                        return zkLib.watchAllChildren(testRoot, {depth: 2}, function (evt) {
                            // console.log(evt);
                            // if(evt.name !== 'NODE_DELETED') {
                            //     throw new Error('Unexpected event', evt);
                            // }
                        });
                    }).then(function (s) {
                        services = s;
                    }).then(function () {
                        services.children.length.should.equal(0);
                    }).then(function () {
                        return client.createAsync(zkPath.join(testRoot, 'foo'));
                    }).delay(100).then(function () {
                        services.children.length.should.equal(1);
                    }).then(function () {
                        return client.createAsync(zkPath.join(testRoot, 'bar'));
                    }).delay(100).then(function () {
                        services.children.length.should.equal(2);
                    }).then(function () {
                        return client.createAsync(zkPath.join(testRoot, 'bar','car'));
                    }).delay(100).then(function () {
                        barNode = _.find(services.children, function (c) {
                            return c.path === zkPath.join(testRoot,'bar');
                        });
                        services.children.length.should.equal(2);
                        barNode.children.length.should.equal(1);
                    }).then(function () {
                        return client.createAsync(zkPath.join(testRoot, 'bar','car', 'tar'));
                    }).delay(100).then(function () {
                        var carNode = _.find(barNode.children, function (c) {
                            return c.path === zkPath.join(testRoot,'bar','car');
                        });
                        should(carNode).be.ok();
                        should(carNode.children.length).be.equal(0);
                    });
            });

            it("depth is INFinity", function() {
                var barNode;
                return client.createAsync(testRoot, new Buffer('xxx'), zookeeper.CreateMode.PERSISTENT)
                    .then(function() {
                        return zkLib.watchAllChildren(testRoot, {depth: Infinity}, function (evt) {
                            // console.log(evt);
                            // if(evt.name !== 'NODE_DELETED') {
                            //     throw new Error('Unexpected event', evt);
                            // }
                        });
                    }).then(function (s) {
                        services = s;
                    }).then(function () {
                        services.children.length.should.equal(0);
                    }).then(function () {
                        return client.createAsync(zkPath.join(testRoot, 'foo'));
                    }).delay(100).then(function () {
                        services.children.length.should.equal(1);
                    }).then(function () {
                        return client.createAsync(zkPath.join(testRoot, 'bar'));
                    }).delay(100).then(function () {
                        services.children.length.should.equal(2);
                    }).then(function () {
                        return client.createAsync(zkPath.join(testRoot, 'bar','car'));
                    }).delay(100).then(function () {
                        barNode = _.find(services.children, function (c) {
                            return c.path === zkPath.join(testRoot,'bar');
                        });
                        services.children.length.should.equal(2);
                        barNode.children.length.should.equal(1);
                    }).then(function () {
                        return client.createAsync(zkPath.join(testRoot, 'bar','car', 'tar'));
                    }).delay(100).then(function () {
                        var carNode = _.find(barNode.children, function (c) {
                            return c.path === zkPath.join(testRoot,'bar','car');
                        });
                        should(carNode).be.ok();
                        should(carNode.children.length).be.equal(1);
                    });
            });
        });

        describe("data watchers", function  () {
            it("does not watch data if disallowed on single node", function(done) {
                client.create(testRoot, new Buffer("test"), zookeeper.CreateMode.EPHEMERAL, function (err, path) {
                    zkLib.watchAllChildren(testRoot, {data:false},
                                           function  watcher(event) {
                                               // there will be a delete notification of the root.
                                               if(event.name === 'NODE_DELETED') {
                                                   return true;
                                               }
                                               return done("were not expect().pecting a watch invocation");
                                           }).delay(100).then(function (s) {
                                               services = s;
                                               services.children.length.should.equal(0);
                                               services.data.toString('utf-8').should.equal('test');
                                               return client.setDataAsync(testRoot, new Buffer('updated-test'));
                                           }).delay(400).then(function  () {
                                               services.data.toString('utf-8').should.equal('test'); // the values have not updated, because thre are no watchers
                                           }).then(function (value) {
                                               done();
                                           }).catch(done);
                });
            });
            it("does not watch data if disallowed recursively", function(done) {
                client.createAsync(testRoot, new Buffer("test"), zookeeper.CreateMode.PERSISTENT)
                    .then(function (path) {
                        return zkLib.watchAllChildren(testRoot, {data:false, depth: 2},
                                               function  watcher(event) {
                                                   // there will be a delete notification of the root.
                                                   if(event.name === 'NODE_DELETED' ||
                                                      event.name=== 'NODE_CHILDREN_CHANGED') {
                                                       console.log("Event", event);
                                                       return true;
                                                   }
                                                   return done("were not expect().pecting a watch invocation");
                                               });
                    }).delay(100).then(function (s) {
                        services = s;
                        services.children.length.should.equal(0);
                        services.data.toString('utf-8').should.equal('test');
                        return client.setDataAsync(testRoot, new Buffer('updated-test'));
                    }).then(function () {
                        return client.createAsync(zkPath.join(testRoot, 'foo'), new Buffer("foo"), zookeeper.CreateMode.EPHEMERAL);
                    }).delay(100).then(function () {
                        console.log(services);
                        services.children.length.should.equal(1);
                        services.children[0].data.toString('utf-8').should.equal('foo');
                    }).then(function () {
                        return client.setDataAsync(zkPath.join(testRoot, 'foo'), new Buffer('foo-new'));
                    }).delay(400).then(function () {
                        services.children[0].data.toString('utf-8').should.equal('foo'); // should not update the data.
                    }).then(function (value) {
                        done();
                    }).catch(done);
            });

            it("does not watch data if explictly allowed on single node", function(done) {
                client.create(testRoot, new Buffer("test"), zookeeper.CreateMode.EPHEMERAL, function (err, path) {
                    zkLib.watchAllChildren(testRoot, {data: true},
                                           function  watcher(event) {
                                               // there will be a delete notification of the root.
                                               if(event.name === 'NODE_DELETED') {
                                                   return true;
                                               }
                                               // expect data invocation on change
                                           }).then(function (s) {
                                               services = s;
                                               services.children.length.should.equal(0);
                                               services.data.toString('utf-8').should.equal('test');
                                               return client.setDataAsync(testRoot, new Buffer('updated-test'));
                                           }).delay(400).then(function  () {
                                               services.data.toString('utf-8').should.equal('updated-test');
                                           }).then(function (value) {
                                               done();
                                           }).catch(done);
                });
            });

        });

    });
});
