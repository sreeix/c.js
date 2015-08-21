'use strict';
var zookeeper = require('node-zookeeper-client');
var locker = require('../../lib/recipies/lock');

describe('Lock', function () {
    var client;
    before(function() {
        client = zookeeper.createClient('localhost:2181');
    });


    describe("lock", function () {
        it('should create locks', function (done) {
            var lock = locker().at('/bjn/tests/test-locks');
            lock.lock(done);
        });

    });
})
