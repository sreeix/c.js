"use strict";
var Promise = require('bluebird');
var zookeeper = require("node-zookeeper-client");

var should = require('should');
var _ = require('underscore');

describe("zk library", function() {
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

        })
;
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
                                                                      console.log("Did we find the stat", stat);
                                                                      should(stat).be.null(); // stat ok means that node exists, after rmr it should be gone
                                                                      return done();
                                                                  })

                                                              }).catch(done);
                                                          });
                                 });

        });

        it("removes multilevel node", function(done) {

            return client.mkdirp(testRoot+'/is/a/very/long/chain/so/child/is/very/far',
                                 new Buffer("test"), zookeeper.CreateMode.PERSISTENT,
                                 function (err, path) {
                                     if(err) {return done(err);}
                                     return zkLib.rmr(testRoot).then(function () {
                                         client.exists(testRoot, function (err, stat) {
                                             if(err) {return done(err);}
                                             console.log("Did we find the stat", stat);
                                             should(stat).be.null(); // stat ok means that node exists, after rmr it should be gone
                                             return done();
                                         })

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


        xit("watches no children of empty node", function(done) {
            client.create(testRoot, new Buffer("test"), zookeeper.CreateMode.EPHEMERAL, function (err, path) {
                zkLib.watchAllChildren(testRoot, {recursive: false, times: 1, added:false, deleted:false}, function  watcher() {
                    done("were not expecting a watch invocation");
                }).then(function (children) {
                    children.length.should.equal(0);
                    return Promise.delay(1000);
                }).then(function (value) {
                    done();
                }).catch(done);
            });
        });
    })
});
