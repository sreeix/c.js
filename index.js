module.exports = {
    lock: require('recipies/lock'),
    twoPhaseCommit: require('recipies/2pc'),
    leaderElection: require('recipies/leaderElection'),
    persistentEphemeralNode: require('recipies/persistentEphemeralNode')
};
