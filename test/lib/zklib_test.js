"use strict";
var Promise = require('bluebird');
var zookeeper = require("node-zookeeper-client");
describe("zk library", function() {
    describe("watchChildren", function () {
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
        afterEach(function () {
            zkLib.rmr(testRoot);
            console.log("Closing Zk Connection");
            client.close();
        });

        it("watches no children of empty node", function(done) {
            client.create(testRoot, new Buffer("test"), CreateMode.EPHEMERAL, function (err, path) {
                zkLib.watchAllChildren(testRoot, {recursive: false, times: 1, added:false, deleted:false}, function  watcher() {
                    done("were not expecting a watch invocation");
                }).then(function (children) {
                    console.log("---------------------");
                    children.should.equal([]);
                    return Promise.delay(1000);
                }).then(done);
            });
        });
    })
});
