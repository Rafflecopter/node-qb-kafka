// lib/dialect.js
// kafka dialect for qb

var kafka = require('kafka-node')
  , kafkaTypes = require('./kafkaTypes')


module.exports = {
  name: 'kafka',
  type: 'rpc',
  startup: startup,
}


function startup (qb, options) {
  var client = new kafka.Client(options.connection_string)
    , producer = new kafka.Producer(client)

  return {
    can: function () { return },
    push: push,
    end: end
  }

  function push (type, task, callback) {
    var message = kafkaTypes.outgoingMessage(task)

    var payload = kafkaTypes.producerPayload(
                    options.task_options[type].topic,
                    message,
                    options.num_partitions,
                    options.task_options[type].key)

    var _send = producer.send.bind(producer, payload.get(), callback)

    if (producer.ready) {
      _send()
    } else {
      producer.on('ready', _send)
    }
  }

  function end(callback) {
    if (client.ready) {
      client.close(callback)
    } else {
      client.on('ready', client.close.bind(client, callback))
    }
  }
}
