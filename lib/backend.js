// lib/backend.js
// kafka backend for qb

var dialect = require('./dialect')

module.exports = function (options, qb) {
  var client = null //new KafkaClient
  option._kafka_backend = true

  qb.speaks(dialect, options)
    .on('queue-start', function (type, next) {

      // client.on('message', function () { qb.emit('process', type, task) })

      next()
    })
    .on('push', function (type, task, next) {
      qb.dialect('kafka').push(type, task, next)
    })
    .on('queues-end', function (next) {

      // client.end(next)

      next()
    })
}