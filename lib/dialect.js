// lib/dialect.js
// kafka dialect for qb

var kafka = require('kafka-node')

var Queue = require('./Queue')


module.exports = {
  name: 'kafka',
  type: 'rpc',
  startup: startup,
}


function startup (qb, options) {
  var client = new kafka.Client(options.connection_string)
    , producer = new kafka.Producer(client)

  var types = {}

  if (!options._kafka_backend) {
    listen()
  }

  return {
    can: can,
    push: push,
    end: end
  }

  function can () {
    Array.prototype.slice.call(arguments).forEach(function (type) {
      if (!types[type]) {
        if (!options._kafka_backend) {
          listen(type)
        }
        types[type] = true
      }
    })
  }

  function push (type, task, callback) {
    var message = kafkaTypes.outgoingMessage(task)

      , payload = kafkaTypes.producerPayload(
                    options.workers[type].topic,
                    message,
                    options.workers[type].key,
                    options.n_partitions)

    if (producer.ready) {
      producer.send(payload, callback)
    } else {
      producer.on('ready', function() {
        producer.send(payload, callback)
      })
    }
  }

  function end (callback) {
    //cli.end()
  }

  function listen (topic) {
  }
}
