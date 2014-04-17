// lib/backend.js
// kafka backend for qb
var async = require('async')
  , _ = require('lodash')
  , kafka = require('kafka-node')

var dialect = require('./dialect')
  , Q = require('./Queue')
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

  // ye ol' abandoned global state variables
  // where's a monad when you need one?
  var consumersToClose = []
    , shuttingDown = false

  qb.speaks(dialect, options)
    .on('queue-start', function (type, next) {
      var client = new kafka.Client(options.connection_string)
        , offsetter = new kafka.Offset(client)

        , topic = options.task_options[type].topic
        , group = options.task_options[type].consumer_group

        , payload = kafkaTypes.consumerPayload(
                      topic,
                      options.instance_id,
                      options.num_instances,
                      options.num_partitions)

        , consumer = new kafka.Consumer(client, payload,
            { autoCommit: false
            , fromOffset: false
            , groupId: group
            })

        , qs = []

      consumer.on('error', function (err) {
        qb.log.error(new Error(err))
      })

      consumersToClose.push(consumer)

      // Set up the buffer queues
      for (var k = 0; k < payload.length; k++) {
        qs.push(new Q())
      }

      // Buffer those messages in a queue for each partition
      // such ordering guarantee very hack
      consumer.on('message', function (obj) {
        var msg = kafkaTypes.incomingMessage(obj)
          , p = msg.get('partition')

        return qs[p].enqueue(msg)
      })

      // Start emitting these motherfuckin' tasks from these motherfuckin queues
      qs.forEach(function (q) {
        var emit = _emitTask.bind(null, q, type, group, offsetter)
          , checkShuttingDown = function () { return shuttingDown }

        async.until(checkShuttingDown, emit, function (err) {
          // It shouldn't ever come to this because we retry tasks until they
          // succeed
          if (err) { throw new Error(err) }
        })
      })

      next()
    })

    .on('push', function (type, task, next) {
      qb.dialect('kafka').push(type, task, next)
    })

    .on('queues-end', function (next) {
      shuttingDown = true

      async.each(consumersToClose, function (consumer, $done) {
        if (consumer.ready) {
          consumer.close($done)
        } else {
          consumer.on('ready', consumer.close.bind(consumer, $done))
        }
      }, next)
    })

  // This implements the following flow:
  // 1. Emit task from partition for processing
  // 2. Finish processing task
  // 3. If error while processing, repeatedly retry until success
  // 4. Commit the offset if commit interval is reached
  // 5. If error while committing, repeatedly retry commit until success
  // 6. Move on to next task in partition
  function _emitTask(q, type, group, offsetter, next) {
    var message = q.dequeue()

    if (!message) { return setImmediate(next) }

    var task = message.toTask()
      , offset = message.get('offset')
      , partition = message.get('partition')
      , topic = message.get('topic')

    __qbemit()

    function __qbemit() {
      qb.emit('process', type, task, __catchErr(__commit(next)))
    }

    function __catchErr($next) {
      return function (err) {
        if (err) {
          return setTimeout(function () {
            __qbemit()
          }, options.retryInterval)
        }

        $next()
      }
    }

    function __commit($next) {
      return function () {
        if ((offset % options.commitInterval) !== 0) { return $next() }

        offsetter.commit(group, 
          [ { topic: topic
            , partition: partition
            , offset: offset
            }
          ], function (err) {
            if (err) {
              return setTimeout(function () {
                __commit($next)
              }, options.retryInterval)
            }

            $next()
          })
      }
    }
  }
}
