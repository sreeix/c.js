"use strict";
var _ = require('underscore');
module.exports = {
    join: function (basePath, manyArgs) {
        return _.toArray(arguments).join('/');
    },
    sequenceNumber: function seqFromPath(path) {
        return parseInt(_.last(path.split('-')), 10);
    },
    parent: function (basePath) {
        return _.initial(basePath.split('/')).join('/');
    },
    childNode: function  (path) {
        return _.last(path.split('/'));
    }
};
