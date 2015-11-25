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
        pen(client).create('/bjn/tmp/persistent1','hello',  function (err, path) {
            if(err) {
                throw err;
            }
            path.path.should.equal('/bjn/tmp/persistent1');
            return client.getDataAsync('/bjn/tmp/persistent1').spread(function  (data, stat) {
                should(data.toString('utf-8')).be.equal('hello');
                path.close();
                done();
            }).catch(done);
        });

    });
    xit("creates a new node without any data", function(done) {
        console.log("creating new node");
        pen(client).create('/bjn/tmp/persistent1', function (err, path) {
            if(err) {
                throw err;
            }
            return client.getDataAsync('/bjn/tmp/persistent1').spread(function (data, stat) {
                console.log("Got Path", path);
                should(data).be.not.be.ok();
                path.path.should.equal('/bjn/tmp/persistent1');
                path.close();
                done();
            }).catch(done);
        });
    });


    xit("creates a new ephemeral sequntial node without any data", function(done) {
        console.log("creating new node");
        pen(client).create('/bjn/tmp/persistent1',
                           null,
                           zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL ,
                           function (err, path) {
                               if(err) {
                                   throw err;
                               }
                               console.log("Got Path", path);
                               return client.getDataAsync(path.path).spread(function (data, stat) {
                                   should(data).be.not.be.ok();
                                   path.path.should.not.equal('/bjn/tmp/persistent1');
                                   path.close();
                                   console.log("-----------");
                                   done();
                               }).catch(done);
                           });
    });

    xit("creates a new ephemeral sequntial node without any data", function(done) {
        console.log("creating new node");
        pen(client).create('/bjn/tmp/persistent1',
                           'foobarbaz',
                           zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL ,
                           function (err, path) {
                               if(err) {
                                   throw err;
                               }
                               console.log("Got Path", path);
                               return client.getDataAsync(path.path).spread(function (data, stat) {
                                   should(data.toString('utf-8')).be.equal('foobarbaz');
                                   path.path.should.not.equal('/bjn/tmp/persistent1');
                                   path.close();
                                   console.log("-----------");
                                   done();
                               }).catch(done);
                           });
    });

    xit("creates itself after it is explicitly removed", function(done) {
        var path;
        console.log("creating new node");
        pen(client).create('/bjn/tmp/persistent1').then(function (p) {
            path = p;
            console.log("Got Path", path);
            path.path.should.equal('/bjn/tmp/persistent1');
            return client.removeAsync('/bjn/tmp/persistent1');
        }).delay(1000).then(function () {
            return client.existsAsync('/bjn/tmp/persistent1').get(0).then(function (stat) {
                console.log("-----",stat);
                should(stat).be.ok();
            });
        }).then(function () {
            path.close();
            done();

        }).catch(done);
    });
});
