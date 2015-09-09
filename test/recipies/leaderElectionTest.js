"use strict";
var zookeeper = require("node-zookeeper-client");
var leadership = require("../../lib/recipies/leaderElection");
var Promise = require('bluebird');

describe("leaderelection", function() {
    var client;
    var zkLib;

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
        console.log("Closing Zk Connection");
        client.close();
    });

    it("takes leadership when only one node", function(done) {
        var le = leadership(client);
        le.requestLeadership("/bjn/tests/leadership", function onLeader(event) {
            console.log("Became Leader");
            done();
        });
    });
});
