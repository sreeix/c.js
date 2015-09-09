var _ = require('underscore');
var Promise = require('bluebird');
var linear = function(options){
    options = _.defaults(options, {retryCount: 3, spinFor: 1000});
    return {
        next: function () {
            options.retryCount--;
            return Promise.delay(options.spinFor);
        },
        shouldRetry: function (err) {
            return options.retryCount > 1;
        }
    };
};

var exponentialBackoff = function (options) {
    options  = _.defaults(options, {initialDelay: 1000, maxDelay: 100000});
    return {
        next: function () {
            if(!options.currentDelay) {
                options.currentDelay = options.initialDelay;
            } else {
                options.currentDelay = options.currentDelay * 2;
            }
            return Promise.delay(options.currentDelay);
        },
        shouldRetry: function (err) {
            return options.currentDelay > options.maxDelay;
        }
    };
};
var randomBackoff = function (options) {
    options  = _.defaults(options, {retryCount: 3, maxDelay: 100000, minDelay: 1000});
    return {
        next: function () {
            return Promise.delay(_.random(options.minDelay, options.maxDelay));
        },
        shouldRetry: function (err) {
            return options.retryCount > 0;
        }
    };
};

var retryMethod = {
    'none': function () {
        return {
            shouldRetry: function (err) {
                return false;
            }};
    },
    'linear': linear,
    'exponential': exponentialBackoff,
    'random': randomBackoff,
    fromOptions: function (opts) {
        var res = retryMethod[opts.type] === null ? retryMethod.none: retryMethod[opts.type](_.omit(opts, 'type'));
        return _.extend({}, opts, res);
    }
};

// error filters allows us the ability to add context based exception handling. Ie specific error codes that can be retried, probably a hack but probably not.
// by default everything is retried.
var shouldRetryThisError = function (retryContext, err) {
    if (_.isFunction(retryContext.errorFilter)) {
        console.log("Error %s matches the error filter %s", err, retryContext.errorFilter(err));
        return retryContext.errorFilter(err);
    }
    return true;
}
function execPromise(promiseFn, args, retryContext) {
    return promiseFn(args).catch(function (err) {
        console.log("Error executing the promise fn.", err);
        if(retryContext.shouldRetry(err) && shouldRetryThisError(retryContext, err)) {
            console.log("retrying....");
            return Promise.resolve(retryContext.next())
                .then(_.partial(execPromise, promiseFn, args, retryContext));

        }
        return Promise.reject(err);
    });
}

module.exports = {
    returning : function (arg) {
        return function () {
            return arg;
        };
    },
    withRetry: function r(promiseFn, opts, restOfTheArgs) {
        var args = _.rest(_.toArray(arguments), 2);
        return execPromise(promiseFn, args, retryMethod.fromOptions(opts));
    }
};
