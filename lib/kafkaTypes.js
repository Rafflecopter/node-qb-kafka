var crypto = require('crypto')

var async = require('async')

var Q = require('./Queue')

module.exports =  { producerPayload: producerPayload
                  , consumerPayload: consumerPayload
                  , outgoingMessage: outgoingMessage
                  , incomingMessage: incomingMessage
                  , qCollection: qCollection
                  }

function producerPayload(topic, message, nPartitions, key) {
  return  [ { topic: topic
            , partition: message.mapToPartition(nPartitions, key)
            , messages: message.toKafkaMessage()
            }
          ]
}

// nInstances is the number of processes running this code globally
// instanceId is an int 0,1,2,3...
function consumerPayload(topic, instanceId, nInstances, nPartitions) {
  var partitions = []
  var payloads = []

  for (var k=0; k < nPartitions; k++) {
    if ((k + instanceId) % nInstances === 0) partitions.push(k)
  }

  payloads = partitions.map(function (p) {
    return  { topic: topic
            , partition: p
            }
  })

  return  { get: function () { return payloads }
          , getPartitions: function () { return partitions }
          }
}

function outgoingMessage(task) {
  function _getKeyVal(key) {
    var arr = key.split('.')
      , mutator = task

    while (arr.length) { mutator = mutator[arr.shift()] }

    return mutator
  }

  // Takes a key in dot notation to access a field in "task"
  // Hashes that field to obtain number between 0.. nPartitions-1
  function _mapToPartition(nPartitions, key) {
    function __mapHash(key, nPartitions) {
      return parseInt(crypto.createHash('sha1')
                            .update(JSON.stringify(_getKeyVal(key)))
                            .digest('hex')
                            .slice(0,6), 16) % nPartitions
    }

    // If key is undefined, map to a random partition
    return (key !== undefined ? __mapHash(key, nPartitions) 
                              : Math.floor(Math.random() * nPartitions))
  }

  function _toKafkaMessage() {
    return JSON.stringify(task)
  }

  return  { mapToPartition: _mapToPartition
          , toKafkaMessage: _toKafkaMessage
          }
}

function incomingMessage(message) {
  function _get(field) { return message[field] }

  function _toTask() {
    return JSON.parse(message.value)
  }

  return  { get: _get
          , toTask: _toTask
          }
}

// creates a queue for each partition in partitions
// messages from these queues are concurrently processed
function qCollection(partitions) {
  var qs = []
    , shuttingDown = false

  // Set up the buffer queues
  for (var k = 0; k < partitions.length; k++) {
    qs.push(new Q())
  }

  function _enqueue(message) {
    return qs[partitions.indexOf(message.get('partition'))].enqueue(message)
  }

  function _onMessage(cb) {
    var checkIfShutDown = function () { return shuttingDown }

    qs.forEach(function (q) {
      async.until(checkIfShutDown, function ($next) {
        var msg = q.dequeue()

        if (!msg) { return setImmediate($next)}

        cb(msg, $next)
      }, function (err) {
        // this needs to not happen unless something really bad happens
        if (err) { throw new Error(err) }
      })
    })
  }

  function _end($done) {
    shuttingDown = true
    return setImmediate($done)
  }

  return  { enqueue: _enqueue
          , onMessage: _onMessage
          , end: _end
          }
}
