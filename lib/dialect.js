// lib/dialect.js
// kafka dialect for qb

var kafka = require('kafka-node')


module.exports = {
  name: 'kafka',
  type: 'rpc',
  startup: startup,
}


function startup (qb, options) {
  var client = new kafka.Client(options.connection_string)
    , producer = new kafka.Producer(client)

  return {
    can: function (cb) { return cb() },
    push: push,
    end: end
  }

  function push (type, task, callback) {
    var message = kafkaTypes.outgoingMessage(task)

      , payload = kafkaTypes.producerPayload(
                    options.task_options[type].topic,
                    message,
                    options.num_partitions,
                    options.task_options[type].key)

    if (producer.ready) {
      producer.send(payload, callback)
    } else {
      producer.on('ready', function() {
        producer.send(payload, callback)
      })
    }
  }

  function end (callback) {
    client.close(callback)
  }
}
