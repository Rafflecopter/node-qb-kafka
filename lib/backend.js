// lib/backend.js
// kafka backend for qb
var async = require('async')

var dialect = require('./dialect')
  , Q = require('./Queue')

module.exports = function (options, qb) {
  option._kafka_backend = true

  qb.speaks(dialect, options)
    .on('queue-start', function (type, next) {
      var client = new kafka.Client(options.connection_string)
        , offsetter = new kafka.Offset(client)

        , topic = options.workers[type].topic
        , group = options.workers[type].consumer_group

        , payload = kafkaTypes.consumerPayload(
                      topic,
                      options.instance_id,
                      options.n_instances,
                      options.n_partitions)

        , consumer = new kafka.Consumer(payload,
            { autoCommit: false
            , groupId: group
            })

        , qs = []

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
        var emit = emitTask.bind(null, q, topic, group, offsetter)

        async.forever(emit, function (err) {
          // It shouldn't ever come to this because we retry tasks until they
          // succeed
          throw new Error(err)
        })
      })
    })

    .on('push', function (type, task, next) {
      qb.dialect('kafka').push(type, task, next)
    })

    .on('queues-end', function (next) {
      next()
    })

  // This implements the following flow:
  // 1. Emit task from partition for processing
  // 2. Finish processing task
  // 3. If error while processing, repeatedly retry until success
  // 4. Commit the offset if commit interval is reached
  // 5. If error while committing, repeatedly retry commit until success
  // 6. Move on to next task in partition
  function _emitTask(q, topic, group, offsetter, next) {
    var message = q.dequeue()

    if (!message) { return next() }

    var task = message.toTask()
      , offset = message.get('offset')
      , partition = message.get('partition')


    function __qbemit() {
      qb.emit('process', topic, task, __catchErr(__commit(next)))
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
          { topic: topic
          , partition: partition
          , offset: offset
          }, function (err) {
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
