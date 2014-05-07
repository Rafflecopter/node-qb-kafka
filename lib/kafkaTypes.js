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

function consumer(client, payloads, options) {
  var face = {}
    , shuttingDown = false
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

    var mbs = messageBins(payloads.getPartitions())
      , encoder = protocol.encodeFetchRequest(fetchMaxWaitMs, fetchMinBytes)
      , decoder = protocol.decodeFetchResponse(function (err, type, value) {
        if (shuttingDown) { return }

        if (err) { return ops.error(err) }

        if (type === "message") {
          mbs.enqueue(incomingMessage(value))
        } else {
          mbs.process(function (message, next) {
            if (shuttingDown) { return next() }

            ops.data(message, next)
          }, function (err) {
            err && ops.error(err)

            return (!shuttingDown && face.read())
          })
        }
      })

    client.send(payloads, encoder, decoder, function (err) { 
      err && ops.error(err)
    })
  }

  face.end = function (next) {
    shuttingDown = true

    ops.end()

    return setImmediate(next)
  }
}

// creates a queue for each partition in partitions
// messages from these queues are concurrently processed
function messageBins(partitions) {
  var qs = []
    , face = {}
    //, shuttingDown = false

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

  //face.onMessage = function (cb) {
  //  var checkIfShutDown = function () { return shuttingDown }

  //  qs.forEach(function (q) {
  //    async.until(checkIfShutDown, function ($next) {
  //      var msg = q.dequeue()

  //      if (!msg) { return setImmediate($next)}

  //      cb(msg, $next)
  //    }, function (err) {
  //      // this needs to not happen unless something really bad happens
  //      if (err) { throw new Error(err) }
  //    })
  //  })
  //}

  //face.end = function ($done) {
  //  shuttingDown = true
  //  return setImmediate($done)
  //}

  return face
}
