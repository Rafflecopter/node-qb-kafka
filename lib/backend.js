// lib/backend.js
// kafka backend for qb
var async = require('async')
  , _ = require('lodash')
  , kafka = require('kafka-node')

var dialect = require('./dialect')
  , kafkaTypes = require('./kafkaTypes')

var DEFAULTS =
{ instance_id: 0        // This same code is run in parallel on multiple processes. Which one is this?
, num_instances: 1      // How many processes are runing this code globally?
, num_partitions: 32    // How many partitions does each kafka topic have? We decided to keep this number static for all topics.
, connection_string: "localhost:2181" // ZooKeeper connection string (i.e. "host1:port1,host2:port2,etc")
, retryInterval: 5000 // ms to wait before retrying a failed task
, commitInterval: 20 // Number of messages to process before sending commit request to Kafka
, task_options: { type1:  { topic: "test"
                          , consumer_group: "testers"
                          , key: undefined // optional field (dot notation) in the task to partition by (i.e. entry.entrant.email)
                          }
                }
}


module.exports = function (options, qb) {
  options = _.defaults( (options || {}), DEFAULTS)

  var retryInterval = options.retryInterval

  qb.speaks(dialect, options)
    .on('queue-start', function (type, next) {
      var client = new kafka.Client(options.connection_string)

        , topic = options.task_options[type].topic
        , group = options.task_options[type].consumer_group

        , consumer = kafkaTypes.consumer(client, topic, group,
                          { fetch_max_ms: options.fetch_max_ms
                          , fetch_min_bytes: options.fetch_min_bytes
                          , instance_id: options.instance_id
                          , num_instances: options.num_instances
                          , num_partitions: options.num_partitions
                          })

      qb.on('queues-end', function ($done) {
        async.series([consumer.close, client.close], $done)
      })

      client.on('error', function (err) {
        qb.log.error(new Error(err))
      })

      client.on('brokersChanged', function () {
        client.refreshMetadata([topic], function (err) {
          return err && qb.log.error(new Error(err))
        })
      })

      consumer.on('error', function (err) {
        qb.log.error(new Error(err))

        setTimeout(function () {
          consumer.read()
        }, retryInterval)
      })

      consumer.on('data', _emitTask.bind(null, type))

      consumer.read()

      next()
    })

    .on('push', function (type, task, next) {
      qb.dialect('kafka').push(type, task, next)
    })


  // This implements the following flow:
  // 1. Emit task for processing
  // 2. Finish processing task
  // 3. If error while processing, repeatedly retry until success
  // 4. Commit the offset if commit interval is reached
  // 5. If error while committing, repeatedly retry commit until success
  // 6. Move on to next task
  function _emitTask(type, message, next) {
    var task = message.toTask()
      , offset = message.get('offset')
      , partition = message.get('partition')
      , topic = message.get('topic')

      , __catchErr = retryIfErr.bind(null, retryInterval, __qbemit)

    __qbemit()

    function __qbemit() {
      qb.emit('process', type, task, __catchErr(__commit(next)))
    }

    function __commit($next) {
      return function () {
        if ((offset % options.commitInterval) !== 0) { return $next() }

        var cb = retryIfErr(retryInterval, __commit, $next)

        consumer.commit(partition, offset, cb)
      }
    }

    function retryIfErr(retryInterval, action, next) {
      return function (err) {
        if (err) {
          return setTimeout(function () {
            action(next)
          }, retryInterval)
        }

        next()
      }
    }
  }
}
