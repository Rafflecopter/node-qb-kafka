// lib/backend.js
// kafka backend for qb
var async = require('async')
  , kafka = require('kafka-node')

var dialect = require('./dialect')
  , kafkaTypes = require('./kafkaTypes')

var DEFAULTS =
{ instance_id: 0        // This same code is run in parallel on multiple processes. Which one is this?
, num_instances: 1      // How many processes are runing this code globally?
, num_partitions: 32    // How many partitions does each kafka topic have? We decided to keep this number static for all topics.
, connection_string: "localhost:2181" // ZooKeeper connection string (i.e. "host1:port1,host2:port2,etc")
, retry_interval: 5000 // ms to wait before retrying a failed task
, fetch_max_wait_ms: 1000 // tell kafka to wait to send a fetch response until [ms] have passed or...
, fetch_min_bytes: 1 // tell kafka to wait to send a fetch response until there are [bytes] ready to send to the client
, fetch_max_bytes: 1024 * 1024
, commit_interval: 20 // Number of messages to process before sending commit request to Kafka
, task_options: { type1:  { topic: "test"
                          , consumer_group: "testers"
                          , key: undefined // optional field (dot notation) in the task to partition by (i.e. entry.entrant.email)
                          }
                }
}


module.exports = function (options, qb) {
  var opts = {}
  for (var i in options) {
    opts[i] = { value: options[i]
              , enumerable: true
              , writeable: true
              , configurable: true
              } 
  }
  options = Object.create(DEFAULTS, opts);

  var consumers = []

  qb.speaks(dialect, options)

    .on('queues-end', function (next) {
      async.each(consumers, function (consumer, _next) {
        consumer.close(_next)
      }, function (err) {
        if (err) { qb.log.error(new Error(err)) }

        next()
      })
    })

    .on('queue-start', function (type, next) {
      var topic = options.task_options[type].topic
        , group = options.task_options[type].consumer_group
        , retryInterval = options.retry_interval
        , commitInterval = options.commit_interval

        , client = new kafka.Client(options.connection_string)
        , consumer = kafkaTypes.consumer(client, topic, group,
                          { fetch_max_wait_ms: options.fetch_max_wait_ms
                          , fetch_min_bytes: options.fetch_min_bytes
                          , fetch_max_bytes: options.fetch_max_bytes
                          , instance_id: options.instance_id
                          , num_instances: options.num_instances
                          , num_partitions: options.num_partitions
                          })

      // push to list to close later
      consumers.push(consumer)

      client.on('error', function (err) {
        qb.log.error(new Error(err))
      })

      client.on('brokersChanged', function () {
        client.refreshMetadata([topic], function (err) {
          return err && qb.log.error(new Error(err))
        })
      })

      client.once('connect', function () {
        consumer.read()

        next()
      })

      consumer.on('error', function (err) {
        qb.log.error(new Error(err))
        setTimeout(consumer.read, retryInterval)
      })

      consumer.on('data', 
        _emitTask.bind(null, qb, consumer, type, commitInterval, retryInterval))
    })

    .on('push', function (type, task, next) {
      qb.dialect('kafka').push(type, task, next)
    })
}

// This implements the following flow:
// 1. Emit task for processing
// 2. Finish processing task
// 3. If error while processing, repeatedly retry until success
// 4. Commit the offset if commit interval is reached
// 5. If error while committing, repeatedly retry commit until success
// 6. Move on to next task
function _emitTask(qb, consumer, type, commitInterval, retryInterval, message, next) {
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
      if ((offset % commitInterval) !== 0) { return setImmediate($next) }

      var cb = retryIfErr(retryInterval, __commit, $next)

      consumer.commit(partition, offset, cb)
    }
  }

  function retryIfErr(retryInterval, action, next) {
    return function (err) {
      if (err) {
        qb.log.error(new Error(err))
        return setTimeout(function () {
          action(next)
        }, retryInterval)
      }

      next()
    }
  }
}
