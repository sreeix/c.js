"use strict";
var zookeeper = require('node-zookeeper-client');

var pen = require('../../lib/recipies/persistentEphemeralNode');
describe("persistentEphemeralNode", function() {
    var client, zkLib;
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


    it("creates a new node", function(done) {
        pen(client).create('/bjn/tmp/persistent1', function (err, path) {
            console.log("Got Path", path);
            path.path.should.equal('/bjn/tmp/persistent1');
            path.close();
            console.log("xxxx");
            done(err, path);
        });

    });
});
