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

        , consumer = new kafka.Consumer(client, payload.get(),
            { autoCommit: false
            , fromOffset: false
            , groupId: group
            })

        , qColl = kafkaTypes.qCollection(payload.getPartitions())

      // Function to run when popping a kafkaTypes.incomingMessage off a queue
      qColl.onMessage(_emitTask.bind(null, type, group, offsetter))

      consumer.on('error', function (err) {
        qb.log.error(new Error(err))
      })

      // Buffer those messages in a queue for each partition
      // such ordering guarantee very hack
      consumer.on('message', _.compose( qColl.enqueue
                                      , kafkaTypes.incomingMessage
                                      ))

      qb.on('queues-end', function ($done) {
        setImmediate(function () {
          async.parallel([qColl.end, consumer.close.bind(consumer)], $done)
        })
      })

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
  function _emitTask(type, group, offsetter, message, next) {
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
