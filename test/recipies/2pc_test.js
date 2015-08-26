"use strict";
var twoPhaseCommit = require('../../lib/recipies/2pc');
var zkPath = require('../../lib/zkPath');
var zookeeper = require('node-zookeeper-client');
var Promise = require('bluebird');
describe('2 pc', function  () {
    var client;
    var zkLib;
    var tpc, coordinator;
    var path = '/bjn/tests/2pc-tests';
    function random(range) {
        return Math.floor(Math.random() * range);
    }

    var comittingSite = function (name, timeout) {
        timeout = timeout || random(100);
        name = name || "comitting-site-"+random(10000);
        return tpc.site(zkPath.join(path, 'transaction'), name, function  (command, cb) {
                setTimeout(function  () {
                    console.log("Wrting to Transaction log");
                    cb(null, 'success');
                }, timeout);
            });
    };

    var abortingSite = function  ( name, timeout) {
        timeout = timeout || random(100);
        name = name || "aborting-site-"+random(10000);

        return tpc.site(zkPath.join(path, 'transaction'), name, function  (command, cb) {
                setTimeout(function  () {
                    console.log("Wrting to Transaction log");
                    cb('failed');
                }, timeout);
        });
    };


    beforeEach(function(done) {
        Promise.longStackTraces();
        client = zookeeper.createClient('localhost:2181');
        client.connect();

        client.once('connected', function () {
            console.log("Connected to zookeeper");
            zkLib = require('../../lib/zkLib')(client);
            tpc = twoPhaseCommit(client);
            coordinator = tpc.coordinator(path);
            done();
        });
    });

    afterEach(function () {
        console.log("Closing Zk Connection");
        client.close();
    });

    describe("simple one site tests", function  () {
        it('should successfully execute', function  (done) {
            coordinator.execute('update', {sites: [comittingSite()], quorum: true}, function  (err, result) {
                console.log("Execution complete", arguments);
                return done(err, result);
            });
        });

        it('should abort', function  (done) {
            coordinator.execute('update', {sites: [abortingSite()], quorum: true}, function  (err, result) {
                if(err){
                    // we were expecting this. should be abort.
                    return done();
                }
                return done("Unexpected. We were expecting an abort. As writing to tlog failed");
            });
        });
    });

    describe("For 2 sites", function  () {
        it('should abort with quorum when 1 site aborts', function  (done) {
            coordinator.execute('update', {sites: [abortingSite(), comittingSite()], quorum: true}, function  (err, result) {
                if(err){
                    // we were expecting this. should be abort.
                    return done();
                }
                return done("Unexpected. We were expecting an abort. As writing to tlog failed");
            });
        });

        it('should abort with quorum of when 1 site aborts and 1 commits', function  (done) {
            coordinator.execute('update', {sites: [abortingSite(), comittingSite()], quorum: true}, function  (err, result) {
                if(err){
                    // we were expecting this. should be abort.
                    return done();
                }
                return done("Unexpected. We were expecting an abort. As writing to tlog failed");
            });
        });

        it('should abort with quorum of when 1 site aborts and 1 commits', function  (done) {
            coordinator.execute('update', {sites: [abortingSite(), comittingSite()], quorum: false}, function  (err, result) {
                if(err){
                    // we were expecting this. should be abort.
                    return done();
                }
                return done("Unexpected. We were expecting an abort. As writing to tlog failed");
            });
        });
    });
});
