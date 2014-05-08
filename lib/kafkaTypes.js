var crypto = require('crypto')

var async = require('async')
  , _ = require('lodash')

var Q = require('./Queue')

module.exports =  { producerPayload: producerPayload
                  , consumerPayload: consumerPayload
                  , outgoingMessage: outgoingMessage
                  , incomingMessage: incomingMessage
                  , qCollection: qCollection
                  }

// A "face" is what you show the world and is a representation of who
// you think you are. Make sure you put on makeup first cause you ugly.

function producerPayload(topic, message, nPartitions, key) {
  var face = {}

  face.get = function () {
    return  [ { topic: topic
              , partition: message.mapToPartition(nPartitions, key)
              , messages: message.toKafkaMessage()
              }
            ]
  }

  return face
}

// nInstances is the number of processes running this code globally
// instanceId is an int 0,1,2,3...
function consumerPayload(offsetter, topic, group, instanceId, nInstances, nPartitions) {
  var partitions = []
    , payloads = []
    , face = {}

  for (var k=0; k < nPartitions; k++) {
    if ((k + instanceId) % nInstances === 0) partitions.push(k)
  }

  payloads = partitions.map(function (p) {
    return  { topic: topic
            , partition: p
            }
  })


  face.get = function (cb) {
    async.map(payloads, function (payload, _cb) {
      if (payload.offset) { return _cb() }
      
      offsetter.fetchCommits(group, payload, _cb)
    }, function (err, _payloads) {
      if (err) { return cb(err) }

      payloads = _payloads

      cb(null, payloads)
    })
  }

  face.getPartitions = function () { return partitions }

  face.getTopics = function () { 
    return  _.uniq(
              _.map(
                payloads, function (payload) { return payload.topic }
              )
            )
  }

  face.setOffset = function (partition, offset) {
    partition = parseInt(partition, 10)

    var idx = _.findIndex(payloads, function (payload) {
      return payload.partition === partition
    })

    return idx ? (payloads[idx].partition = partition) && true : false
  }

  return face
}

function outgoingMessage(task) {
  var face = {}

  function _getKeyVal(key) {
    var arr = key.split('.')
      , mutator = task

    while (arr.length) { mutator = mutator[arr.shift()] }

    return mutator
  }

  // Takes a key in dot notation to access a field in "task"
  // Hashes that field to obtain number between 0.. nPartitions-1
  face.mapToPartition = function (nPartitions, key) {
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

  face.toKafkaMessage = function () {
    return JSON.stringify(task)
  }

  return face
}

function incomingMessage(message) {
  var face = {}

  face.get = function (field) { return message[field] }

  face.toTask = function () {
    return JSON.parse(message.value)
  }

  return face
}

function consumer(client, offsetter, payloads, options) {
  var face = {}
    , shuttingDown = false
    , reading = false
    , noop = function () {}

    , fetchMaxMs = options.fetchMaxMs
    , fetchMinBytes = options.fetchMinBytes

    , ops = { data:   function () { throw new Error('No data handler supplied') }
            , error:  function (err) { throw new Error(err) }
            , end:    noop
            }

  face.on = function (evt, cb) {
    cb && (ops[evt] = cb)
  }

  face.read = function () {
    if (shuttingDown) { return }

    if (reading) { return }
    reading = true

    var retryDueToError = false
      , mbs = messageBins(payloads.getPartitions())
      , encoder = protocol.encodeFetchRequest(fetchMaxWaitMs, fetchMinBytes)
      , decoder = protocol.decodeFetchResponse(function (err, type, value) {
        if (shuttingDown) { return }

        if (retryDueToError) { return (reading = false) }

        if (err) { return (retryDueToError = true) && ops.error(err) }

        if (type === "message") {
          mbs.enqueue(incomingMessage(value))

        } else {
          mbs.process(_processMessage, _fin)

        }
      })

    _send(payloads, encoder, decoder, function (err) {
      if (err) { (reading = false) && ops.error(err) }
    })

    function _send(payloads, encoder, decoder, done) {
      var csend = function (payloads, cb) {
        return client.send(payloads, encoder, decoder, cb)
      }

      async.waterfall([payloads.get, csend], done)
    }

    function _processMsg(message, next) {
      if (shuttingDown) { return next() }

      ops.data(message, next)
    }

    function _fin(err) {
      if (err) { return ops.error(err) }

      face.read()
    }
  }

  face.commit = function (partition, offset, next) {
  }

  face.end = function (next) {
    shuttingDown = true

    return setImmediate(next)
  }
}

// creates a queue for each partition in partitions
// messages from these queues are concurrently processed
function messageBins(partitions) {
  var qs = []
    , face = {}

  // Set up the buffer queues
  for (var k = 0; k < partitions.length; k++) {
    qs.push([])
  }

  face.enqueue = function (message) {
    return qs[partitions.indexOf(message.get('partition'))].push(message)
  }

  face.process = function (handler, done) {
    return qs.forEach(function (q) {
      async.eachSeries(q, handler, done)
    })
  }

  return face
}
