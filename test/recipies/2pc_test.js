'use strict'
var twoPhaseCommit = require('../../lib/recipies/2pc');
var zkPath = require('../../lib/zkPath');
var zookeeper = require('node-zookeeper-client');
var Promise = require('bluebird');
describe('2 pc', function  () {
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

    it('should successfully execute', function  (done) {
        var tpc = twoPhaseCommit(client);
        var path = '/bjn/tests/2pc-tests';
        var coordinator = tpc.coordinator(path);
        var site = tpc.site(zkPath.join(path, 'transaction'), 'site1', function  (command, cb) {
            console.log("setting up intents of commiting the transaction.", command);
            setTimeout(function  () {
                console.log("Wrting to Transaction log");
                cb(null, 'success');
            }, 200);
        });
        coordinator.execute('update', {sites: [site], quorum: true}, function  (err, result) {
            console.log("Execution complete", arguments);
            return done(err, result);
        });
    });
});
