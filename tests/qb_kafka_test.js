// qb_kafa_tests.js
require('longjohn')

var _ = require('lodash')
  , kafka = require('kafka-node')

var qbPkg = require('qb')
  , QB = qbPkg.backend(require('../lib/backend'))

var qb1, qb2;

//var connectionString = "localhost:2181"
var connectionString = "dev.raafl.com:2181"

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
                              //, key: "foo"
                              }
                    , foobar2:  { topic: "foobar"
                                , consumer_group: "foobar2ers"
                                , key: "foo"
                                }
                    }
    })

  cb()
}

tests.tearDown = function (cb) {
  qb1 && qb1.end(cb)
}

tests.basic = function basic (test) {
  test.expect(8)
  var calledFoobar = false;
  var calledFoobar2 = false;
  qb1.on('error', test.ifError)
     .can('foobar', function (task, done) {
       test.equal(task.foo, 'bar');
       calledFoobar = true;
       done();
     })
     .can('foobar2', function (task, done) {
       test.equal(task.foo, 'bar');
       calledFoobar2 = true;
       done();
     })
     .post('process')
       .use(function (type, task, next) {
         test.equal(task.foo, 'bar');
         test.equal((type == 'foobar' ? calledFoobar : calledFoobar2), true);
         next();
       })
     .on('finish', function (type, task, next) {
       if (calledFoobar && calledFoobar2) {
        setImmediate(test.done);
       }
     })
     .on('ready', function () {
       qb1.push('foobar', {foo: 'bar'}, test.ifError);
       qb1.push('foobar2', {foo: 'bar'}, test.ifError)
     })
     .start()
}
