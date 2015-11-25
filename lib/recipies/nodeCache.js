module.exports = function (client) {
    var zkLib = require('../zkLib')(client);

    return {
        cache: function (path, opts) {
            var options = _.defaults({}, opts, {depth: Infinity} )
            return zkLib.watchAllChildren(path, options);
        }
    }
};
