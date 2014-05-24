var async = require('async')

var QB = require('qb').backend(require('../index').backend)

// some settings n shit
var connectionString = process.env.ZK_HOST || "localhost:2181"
  , topic = "benchmark"
  , group = "benchmarkers"
  , numMsgsToSend = 10000
  , avgBytes = 20

// some state n shit
var qb = QB({ instance_id: 0
            , num_instances: 1
            , num_partitions: 32
            , commit_interval: 100
            , connection_string: connectionString
            , task_options: { benchmark:  { topic: topic
                                          , consumer_group: group
                                          }
                            }
            , prefix: "benchmark"
            })
  , numMsgsProcessed = 0
  , startTime = Date.now()
  , ending = false


qb
.on('error', qb.log.error.bind(qb))

.can('benchmark', function (task, done) {
  numMsgsProcessed++
  done()
})

.on('finish', function (type, task, next) {
  if ((numMsgsProcessed >= numMsgsToSend) && !ending) {
    var timeElapsed = Date.now() - startTime
      , rate = Math.round( numMsgsProcessed * avgBytes / timeElapsed * 1000.0 / (1024) )

    ending = true

    qb.log.info('Processed ' + numMsgsProcessed + ' messages')
    qb.log.info('In ' + timeElapsed + ' ms')
    qb.log.info('At an average rate of ' + rate + ' kB/s')

    return qb.end()
  }

  next()
})

.on('ready', function () {
  var q = async.queue(qb.push.bind(qb, 'benchmark'), 30)

  q.drain = function () {
    qb.log.info('Pushed ' + numMsgsToSend + ' messages')
  }

  for (var k=0; k < numMsgsToSend; k++) {
    var bar = Math.random().toString(36).replace(/[^a-z]+/g, '')

    q.push({foo: bar}, function (err) { 
      err && qb.log.error(err)
    })
  }
})

.start()
