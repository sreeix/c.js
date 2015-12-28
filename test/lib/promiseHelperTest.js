"use strict";
var promiseHelper = require('../../lib/promiseHelper.js');
var Promise = require('bluebird');

describe(" promiseHelper", function() {
    describe(" retry", function() {
        describe("linear", function () {
            it("does not retry on success", function(done) {
                var callCount = 0;
                promiseHelper.withRetry(function  () {
                    callCount++;
                    return Promise.resolve(10);
                }, {type: 'linear'}).then(function () {
                    callCount.should.equal(1);
                    done();
                });
            });

            it("does retries on failure", function(done) {
                var callCount = 0;
                promiseHelper.withRetry(function  () {
                    callCount++;
                    return Promise.reject(10);
                }, {type: 'linear', retryCount: 8}).catch(function (x) {
                    callCount.should.equal(8);
                    x.should.equal(10);
                    done();
                });
            });

            it("does retries on failure with defaults", function(done) {
                var callCount = 0;
                promiseHelper.withRetry(function  () {
                    callCount++;
                    return Promise.reject(10);
                }, {type: 'linear'}).catch(function (x) {
                    callCount.should.equal(3);
                    x.should.equal(10);
                    done();
                });
            });

            it("succeeds if retry works", function(done) {
                var callCount = 0;
                promiseHelper.withRetry(function  () {
                    callCount++;
                    if( callCount < 2){
                        return Promise.reject(10);
                    } else {
                        return Promise.resolve(8);
                    }
                }, {type: 'linear'}).then(function (x) {
                    callCount.should.equal(2);
                    x.should.equal(8);
                    done();
                });
            });
            it("succeeds if retry works and retryFIlter is true", function(done) {
                var callCount = 0;
                promiseHelper.withRetry(function  () {
                    callCount++;
                    if( callCount < 2){
                        return Promise.reject(10);
                    } else {
                        return Promise.resolve(8);
                    }
                }, {type: 'linear', errorFilter: function  (err) {
                    return true;
                }}).then(function (x) {
                    callCount.should.equal(2);
                    x.should.equal(8);
                    done();
                });
            });

            it("will not retry if error filter fails", function(done) {
                var callCount = 0;
                promiseHelper.withRetry(function  () {
                    callCount++;
                    return Promise.reject(10);
                }, {type: 'linear', errorFilter: function (err) {
                    console.log('Returning false from error filter');
                    return false;
                }}).then(function () {
                    return done("Success was not expected");
                }).catch(function (err) {
                    callCount.should.equal(1);
                    err.should.equal(10);
                    done();
                });
            });

        });
    });

});
