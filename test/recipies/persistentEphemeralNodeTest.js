"use strict";
var zookeeper = require('node-zookeeper-client');
var should = require('should');


var pen = require('../../lib/recipies/persistentEphemeralNode');
describe("persistentEphemeralNode", function() {
    var client, zkLib;
    beforeEach(function(done) {
        client = zookeeper.createClient('localhost:2181');
        client.connect();
        client.once('connected', function () {
            console.log("Connected to zookeeper");
            zkLib = require('../../lib/zkLib')(client);
            return zkLib.ensurePath('/bjn/tmp').then(function () {
                return done();
            });

        });
    });
    afterEach(function (done) {
        console.log("Closing Zk Connection");
        return zkLib.rmr('/bjn/tmp').then(function () {
            console.log("deleted nodes");
            client.close();
            done();
        }).catch(done);

    });


    it("creates a new node", function(done) {
        console.log("creating new node");
        pen(client).create('/bjn/tmp/persistent1','hello')
            .then(function ( path) {
                path.path.should.equal('/bjn/tmp/persistent1');
                return client.getDataAsync('/bjn/tmp/persistent1').then(function  (data) {
                    should(data.toString('utf-8')).be.equal('hello');
                    path.close();
                    done();
                });
            }).catch(done);

    });
    it("creates a new node without any data", function(done) {
        console.log("creating new node");
        pen(client).create('/bjn/tmp/persistent1')
            .then(function (path) {
                return client.getDataAsync('/bjn/tmp/persistent1').then(function (data) {
                    console.log("Got Path", path);
                    should(data).be.not.be.ok();
                    path.path.should.equal('/bjn/tmp/persistent1');
                    path.close();
                    done();
                });
            }).catch(done);
    });


    it("creates a new ephemeral sequntial node without any data", function(done) {
        console.log("creating new node");
        pen(client).create('/bjn/tmp/persistent1',
                           null,
                           zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL
                          ).then(function (path) {
                              console.log("Got Path", path);
                              return client.getDataAsync(path.path).then(function (data) {
                                  should(data).be.not.be.ok();
                                  path.path.should.not.equal('/bjn/tmp/persistent1');
                                  path.close();
                                  done();
                              });
                          }).catch(done);
    });


    it("creates a new ephemeral sequntial node with data", function(done) {
        console.log("creating new node");
        pen(client).create('/bjn/tmp/persistent1',
                           'foobarbaz',
                           zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL
                          ).then(function (path) {
                              console.log("Got Path", path);
                              return client.getDataAsync(path.path).then(function (data) {
                                  should(data.toString('utf-8')).be.equal('foobarbaz');
                                  path.path.should.not.equal('/bjn/tmp/persistent1');
                                  path.close();
                                  done();
                              });
                          }).catch(done);
    });

    it("creates a new ephemeral sequntial node without any data is recreated", function(done) {
        console.log("creating new node");
        pen(client).create('/bjn/tmp/persistent1',
                           'foobarbaz',
                           zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL
                          ).then(function (path) {
                              console.log("Got Path", path);
                              return client.getDataAsync(path.path).then(function (data) {
                                  should(data.toString('utf-8')).be.equal('foobarbaz');
                                  path.path.should.not.equal('/bjn/tmp/persistent1');
                                  path.close();
                                  done();
                              });
                          }).catch(done);
    });

    it("creates itself after it is explicitly removed", function(done) {
        var path;
        console.log("creating new node");
        pen(client).create('/bjn/tmp/persistent1').then(function (p) {
            path = p;
            console.log("Got Path", path);
            path.path.should.equal('/bjn/tmp/persistent1');

        }).then(function () {
            console.log("removing the node");
            return client.removeAsync('/bjn/tmp/persistent1');

        }).delay(100).then(function () {
            return client.getDataAsync('/bjn/tmp/persistent1')
                .then(function (data,stat) {
                    should(data).be.undefined();
                });
        }).then(function () {
            path.close();
            done();
        }).catch(done);
    });

    it("creates itself with data after it is explicitly removed", function(done) {
        var path;
        console.log("creating new node");
        pen(client).create('/bjn/tmp/persistent1', 'Hello world').then(function (p) {
            path = p;
            console.log("Got Path", path);
            path.path.should.equal('/bjn/tmp/persistent1');

        }).then(function () {
            return client.removeAsync('/bjn/tmp/persistent1');
        }).delay(100).then(function () {
            return client.getDataAsync('/bjn/tmp/persistent1')
                .then(function (data) {
                    should(data.toString('utf-8')).be.equal('Hello world');
                });
        }).then(function () {
            path.close();
            done();
        }).catch(done);
    });

    it("creates itself with data after it is explicitly removed", function(done) {
        var path;
        console.log("creating new node");
        pen(client).create('/bjn/tmp/persistent1', 'Hello world').then(function (p) {
            path = p;
            console.log("Got Path", path);
            path.path.should.equal('/bjn/tmp/persistent1');

        }).then(function () {
            return client.removeAsync('/bjn/tmp/persistent1');
        }).delay(100).then(function () {
            return client.getDataAsync('/bjn/tmp/persistent1')
                .then(function (data) {
                    should(data.toString('utf-8')).be.equal('Hello world');
                });
        }).then(function () {
            path.close();
            done();
        }).catch(done);
    });



    it("creates sequential ephemeral at same path after it is explicitly removed multiple times", function(done) {
        var path;
        console.log("creating new node");
        pen(client).create('/bjn/tmp/persistent1', null, zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL)
            .then(function (p) {
                path = p;
                console.log("Got Path", path);
                path.path.should.not.equal('/bjn/tmp/persistent1');
                path.path.should.startWith('/bjn/tmp/persistent1');
            }).then(function () {
                return client.removeAsync(path.path);
            }).delay(100).then(function () {
                return client.getDataAsync(path.path)
                    .then(function (data) {
                        should(data).be.undefined();
                    });
            }).then(function () {
                return client.removeAsync(path.path);
            }).delay(100).then(function () {
                return client.getDataAsync(path.path)
                    .then(function (data) {
                        should(data).be.undefined();
                    });
            }).then(function () {
                path.close();
                done();
            }).catch(done);
    });

        it("creates sequential ephemeral on the same path after it is explicitly removed", function(done) {
        var path;
        console.log("creating new node");
        pen(client).create('/bjn/tmp/persistent1', 'persistentephemeralnode', zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL)
            .then(function (p) {
                path = p;
                console.log("Got Path", path);
                path.path.should.not.equal('/bjn/tmp/persistent1');
                path.path.should.startWith('/bjn/tmp/persistent1');
            }).then(function () {
                return client.removeAsync(path.path);
            }).delay(100).then(function () {
                return client.getDataAsync(path.path)
                    .then(function (data) {
                        should(data.toString('utf-8')).be.equal('persistentephemeralnode');
                    });
            }).then(function () {
                path.close();
                done();
            }).catch(done);
    });

    it("creates sequential ephemeral on the same path after it is explicitly removed multiple times ", function(done) {
        var path;
        console.log("creating new node");
        pen(client).create('/bjn/tmp/persistent1', 'persistentephemeralnode', zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL)
            .then(function (p) {
                path = p;
                console.log("Got Path", path);
                path.path.should.not.equal('/bjn/tmp/persistent1');
                path.path.should.startWith('/bjn/tmp/persistent1');
            }).then(function () {
                return client.removeAsync(path.path);
            }).delay(100).then(function () {
                return client.getDataAsync(path.path)
                    .then(function (data) {
                        should(data.toString('utf-8')).be.equal('persistentephemeralnode');
                    });
            }).then(function () {
                return client.removeAsync(path.path);
            }).delay(100).then(function () {
                return client.getDataAsync(path.path)
                    .then(function (data) {
                        should(data.toString('utf-8')).be.equal('persistentephemeralnode');
                    });
            }).then(function () {
                path.close();
                done();
            }).catch(done);
    });
    // following need to be tested but not really is.
    //    * Session timeouts
    // * short disconnect from Zookeeper
    // Zookeeper down (for a long time/short time)

});
