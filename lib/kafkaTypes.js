var crypto = require('crypto')

module.exports =  { producerPayload: producerPayload
                  , consumerPayload: consumerPayload
                  , outgoingMessage: outgoingMessage
                  , incomingMessage: incomingMessage
                  }

function producerPayload(topic, message, key, nPartitions) {
  return  [ { topic: topic
            , partition: message.mapToPartition(key, nPartitions)
            , messages: message.toKafkaMessage()
            }
          ]
}

// nInstances is the number of processes running this code globally
// instanceId is an int 0,1,2,3...
function consumerPayload(topic, instanceId, nInstances, nPartitions) {
  var partitions = []

  for (var k=0; k < nPartitions; k++) {
    if ((k + instanceId) % nInstances === 0) partitions.push(k)
  }

  return partitions.map(function (p) {
    return  { topic: topic
            , partition: p + ''
            }
  })
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
  function _mapToPartition(key, nPartitions) {
    var val = _getKeyVal(key)
      , hash = crypto.createHash('sha1').update(val).digest('hex').slice(0,6)
    return parseInt(hash, 16) % nPartitions
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
