// qb_kafa_tests.js
// require('longjohn')

var _ = require('lodash')
  , kafka = require('kafka-node')

var qbPkg = require('qb')
  , QB = qbPkg.backend(require('../lib/backend'))

var qb1, qb2;

var connectionString = "localhost:2181"
//var connectionString = "dev.raafl.com:2181"

// If we are getting a test.done complaint, turn this on. It helps find errors
process.on('uncaughtException', function (err) {
  console.error(err.stack);
});
process.setMaxListeners(100);

var tests = exports.tests = {};

tests.setUp = function (cb) {
  qb1 = QB(
    { instance_id: 0
    , num_instances: 1
    , num_partitions: 1
    , commitInterval: 1
    , connection_string: connectionString
    , task_options: { foobar: { topic: "foobar"
                              , consumer_group: "foobarers"
                              }
                    }
    })

  cb()
}

tests.tearDown = function (cb) {
  qb1 && qb1.end(cb)
}

tests.basic = function basic (test) {
  test.expect(5)
  var called = false;
  qb1.on('error', test.ifError)
     .can('foobar', function (task, done) {
       test.equal(task.foo, 'bar');
       called = true;
       done();
     })
     .post('process')
       .use(function (type, task, next) {
         test.equal(type, 'foobar');
         test.equal(task.foo, 'bar');
         test.equal(called, true);
         next();
       })
     .on('finish', function (type, task, next) {
       setImmediate(test.done);
     })
     .start()

     .on('ready', function () {
       qb1.push('foobar', {foo: 'bar'}, test.ifError);
     })
}
