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

        , payload = kafkaTypes.consumerPayload(
                      options.workers[type].topic,
                      options.instance_id,
                      options.n_instances,
                      options.n_partitions)

        , consumer = new kafka.Client(payload,
            { autoCommit: false
            , groupId: options.workers[type].consumer_group
            })

        , q = new Queue()

      consumer.on('message', function (message) {
        q.enqueue(kafkaTypes.incomingMessage(message))
      })

      async.forever(function (next) {
        var message = q.dequeue()

        if (!message) { return next() }

        qb.emit('process', topic, message.toTask(), commit(next))
      }, function (err) {
      })
    })

    .on('push', function (type, task, next) {
      qb.dialect('kafka').push(type, task, next)
    })

    .on('queues-end', function (next) {
      next()
    })
}
