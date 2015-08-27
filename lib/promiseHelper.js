module.exports = {
    returning : function (arg) {
        return function () {
            return arg;
        };
    }
};
