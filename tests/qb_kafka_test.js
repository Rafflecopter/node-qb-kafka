// qb_kafa_tests.js
// If we are getting a test.done complaint, turn this on. It helps find errors
process.on('uncaughtException', function(err) {
  console.error(err.stack);
});
process.setMaxListeners(0);
require('longjohn')

var async = require('async')

var QB = require('qb').backend(require('../index').backend)

var qb1, qb2, qb3;

var connectionString = process.env.ZK_HOST

var tests = exports.tests = {};

tests.setUp = function (cb) {
  qb1 = QB(
    { instance_id: 0
    , end_timeout: 5000
    , num_instances: 1
    , num_partitions: 1
    , commit_interval: 1
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
    , prefix: 'qbBasic'
    })

  qb2 = QB(
    { instance_id: 0
    , num_instances: 2
    , num_partitions: 32
    , commit_interval: 1
    , connection_string: connectionString
    , task_options: { foobar: { topic: "foobar32"
                              , consumer_group: "foobar32ers"
                              //, key: "foo"
                              }
                    }
    , prefix: 'qbMulti1'
    })

  qb3 = QB(
    { instance_id: 1
    , num_instances: 2
    , num_partitions: 32
    , commit_interval: 1
    , connection_string: connectionString
    , task_options: { foobar: { topic: "foobar32"
                              , consumer_group: "foobar32ers"
                              //, key: "foo"
                              }
                    }
    , prefix: 'qbMulti2'
    })

  cb()
}

tests.tearDown = function (cb) {
  var qbs = [qb1, qb2, qb3]

  async.each(qbs, function (qb, next) {
    qb.end(next.bind(null, null))
  }, cb)
}

// starting with a "clean" topic (no offset lag) push and process 2 messages
tests.basic = function (test) {
  //test.expect(10)
  var calledFoobar = 0
    , calledFoobar2 = 0
    , m = Math.random().toString(36).replace(/[^a-z]+/g, '')

  qb1
    .on('error', test.ifError)
    .can('foobar', function (task, done) {
      test.equal(task.foo, m)
      calledFoobar++
      setImmediate(done)
    })
    .can('foobar2', function (task, done) {
      test.equal(task.foo, m)
      calledFoobar2++
      setImmediate(done)
    })
    .on('finish', function (type, task, next) {
      if (calledFoobar === 2 && calledFoobar2 === 2) {
        test.done()
      }
    })
    .on('ready', function () {
      qb1.push('foobar', {foo: m}, test.ifError)
      qb1.push('foobar2', {foo: m}, test.ifError)
    })
    .start()
}

// In order for this test to pass, a topic called foobar32 
// needs to be set up with 32 partitions
tests.multiPartition = function multiPartition(test) {
  var numProcessed = 0
    , numToSend = 64
    , sentmsgs = []

  function _process(task, done) {
    numProcessed++

    test.ok(!!~sentmsgs.indexOf(task.foo), task.foo + " not found in sent messages")

    done()
  }

  function _checkFinish(type, task, next) {
    if (numProcessed === numToSend) {
      return setTimeout(function () {
        test.equal(numToSend, numProcessed)
        test.done()
      }, 500)
    } else {
      return next()
    }
  }

  qb2
    .on('error', test.ifError)
    .can('foobar', _process)
    .on('finish', _checkFinish)
    .on('ready', test.ifError)
    .start()

  qb3
    .on('error', test.ifError)
    .can('foobar', _process)
    .on('ready', function () {
      for (var k = 0; k < numToSend; k++) {
        var m = Math.random().toString(36).replace(/[^a-z]+/g, '')

        sentmsgs.push(m)  

        qb2.push('foobar', {foo: m}, test.ifError)
      }
    })
    .on('finish', _checkFinish)
    .start()
}
