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
                    console.log("Wrting %s to Transaction log", command);
                    cb(null, 'success');
                }, timeout);
            });
    };

    var abortingSite = function  ( name, timeout) {
        timeout = timeout || random(100);
        name = name || "aborting-site-"+random(10000);

        return tpc.site(zkPath.join(path, 'transaction'), name, function  (command, cb) {
                setTimeout(function  () {
                    console.log("Wrting %s to Transaction log failed", command);
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
            coordinator.execute('update', {sites: [comittingSite()], quorum: true}, done);
        });
        it('should successfully execute with quorum=1', function  (done) {
            coordinator.execute('update', {sites: [comittingSite()], quorum: 1}, done);
        });

        it('should abort on default timeout(5000 ms)', function  (done) {
            coordinator.execute('update', {sites: [comittingSite('slow-comitting', 8000)], quorum: true}, function  (err, result) {
                if(err){
                    // we were expecting this. should be abort.
                    return done();
                }
                return done("Unexpected. We were expecting an abort. As writing to tlog failed");
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
        it('should commit with quorum when 2 sites commit', function  (done) {
            coordinator.execute('update', {sites: [comittingSite(), comittingSite()], quorum: true}, done);
        });

        it('should abort with quorum when 1 site aborts', function  (done) {
            coordinator.execute('update', {sites: [abortingSite(), comittingSite()], quorum: true}, function  (err, result) {
                if(err){
                    // we were expecting this. should be abort.
                    return done();
                }
                return done("Unexpected. We were expecting an abort. As writing to tlog failed");
            });
        });
        it('should abort on default timeout(5000 ms)', function  (done) {
            coordinator.execute('update', {sites: [comittingSite('slow-comitting', 8000), comittingSite('slow-comitting', 3000)], quorum: true}, function  (err, result) {
                if(err){
                    // we were expecting this. should be abort.
                    return done();
                }
                return done("Unexpected. We were expecting an abort. As writing to tlog failed");
            });
        });

        it('should commit with quorum = 1 when first site says abort', function  (done) {
            coordinator.execute('update', {sites: [abortingSite('slow-site-'+random(10000), 10), comittingSite('fast-site-'+random(10000), 1000)], quorum: 1}, done);
        });

        it('should commit with quorum = 1 when first site says commit and second abort', function (done) {
            coordinator.execute('update', {sites: [abortingSite('slow-site-'+random(10000), 300), comittingSite('fast-site-'+random(10000), 10)], quorum: 1}, done);
        });

        it('should abort with quorum, when 1 site aborts even if first is success', function  (done) {
            coordinator.execute('update', {sites: [abortingSite('slow-site', 300), comittingSite('fast-site', 10)], quorum: true}, function  (err, result) {
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
