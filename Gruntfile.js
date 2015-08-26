module.exports = function(grunt) {

  // Project configuration.
  grunt.initConfig({
      pkg: grunt.file.readJSON('package.json'),
       jshint: {
           all: ['Gruntfile.js', 'lib/**/*.js', 'test/**/*.js'],
           options: {
               reporter: require('jshint-stylish'),
               jshintrc:'jshintrc'
           }
       },
      simplemocha: {
          options: {
              globals: ['should'],
              timeout: 3000,
              ignoreLeaks: false,
              grep: '.*',
              ui: 'bdd',
              reporter: 'tap'
          },
          all: { src: ['test/**/*.js'] }
      }
  });

    // Load the plugin that provides the "uglify" task.
    grunt.loadNpmTasks('grunt-contrib-jshint');
    grunt.loadNpmTasks('grunt-simple-mocha');
  // Default task(s).
    grunt.registerTask('default', ['simplemocha']);

};
