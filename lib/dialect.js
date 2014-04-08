// lib/dialect.js
// kafka dialect for qb

module.exports = {
  name: 'kafka',
  type: 'rpc',
  startup: startup,
}

function startup (qb, options) {
  // var client = new KafkaClient
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

  function push (topic, task, callback) {
    //cli.produce(topic, task, callback)
  }

  function end (callback) {
    //cli.end()
  }

  function listen (topic) {
    //cli.for(topic).on('message', function (task) { qb.push(topic, task) })
  }
}