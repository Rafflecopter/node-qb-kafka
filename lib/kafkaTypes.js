var crypto = require('crypto')

var async = require('async')
  , kafka = require('kafka-node')

module.exports =  
{ producerPayload:  producerPayload
, outgoingMessage:  outgoingMessage
, consumer:         consumer
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
function consumerPayloads(topic, instanceId, nInstances, nPartitions) {
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


  face.getPartitionOffsetMap = function (offsetter, cb) {
    async.map(payloads, function (payload, _cb) {
      offsetter.fetchCommits(group, payload, function (err, data) {
        if (err) { return _cb(err) }

        var partition = parseInt(payload.partition, 10)
          , offset = data[payload.topic][payload.partition]

        _cb(null, [partition, offset])
      })

    }, function (err, pairs) {
      if (err) { return cb(err) }

      var map = pairs.reduce(function (acc, pair) {
        return (acc[pair[0]] = pair[1]) && acc
      })

      cb(null, map)
    })
  }

  face.getPartitions = function () { return partitions }

  face.get = function (partitionToOffsetMap) {
    return payloads.map(function (payload) {
      payload.offset = partitionToOffsetMap[payload.partition]
      return payload
    })
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

function consumer(client, topic, group, options) {
  var face = {}
    , noop = function () {}

    // OMFG WATCH OUT FOR STATE o_0
    // Consumer has a lot of ferking state
    , shuttingDown = false
    , reading = false
    , offsets = {} // partition to offset map
    , ops = { data:   function () { throw new Error('No data handler supplied') }
            , error:  function (err) { throw new Error(err) }
            , end:    noop
            }

    , fetchMaxMs = options.fetch_max_ms
    , fetchMinBytes = options.fetch_min_bytes

    , offsetter = new kafka.Offset(client)
    , payloads = consumerPayloads(topic,
                                  options.instance_id,
                                  options.num_instances,
                                  options.num_partitions)

  var read = function () {
    if (shuttingDown) { return }

    if (reading) { return }
    reading = true

    var retryDueToError = false
      , mbs = messageBins(payloads.getPartitions())
      , encoder = protocol.encodeFetchRequest(fetchMaxWaitMs, fetchMinBytes)
      , decoder = protocol.decodeFetchResponse(_decoderCb)

    client.send(payloads.get(offsets), encoder, decoder, function (err) {
      if (err) { return (reading = false) && ops.error(err) }
    })

    function _decoderCb(err, type, value) {
      if (shuttingDown) { return }

      if (retryDueToError) { return (reading = false) }

      if (err) { return (retryDueToError = true) && ops.error(err) }

      if (type === "message") {
        mbs.enqueue(incomingMessage(value))

      } else {
        mbs.process(_processMessage, _fin)

      }
    }

    function _processMsg(message, next) {
      if (shuttingDown) { return next() }

      ops.data(message, function (err) {
        if (err) { return next(err) }

        var partition = message.get('partition')
          , offset = message.get('offset')

        // update consumer state and continue
        return (offsets[partition] = offset + 1) && next()
      })
    }

    function _fin(err) {
      if (err) { return ops.error(err) }

      face.read()
    }
  }

  var ensureOffsets = function (next) {
    if (offsets.length) { return next() }

    payloads.getPartitionOffsetMap(offsetter, function (err, map) {
      if (err) { return ops.error(err) }

      // update consumer state
      offsets = map

      return next()
    })
  }

  face.read = ensureOffsets.bind(null, read)

  face.on = function (evt, cb) {
    cb && (ops[evt] = cb)
  }

  face.commit = function (partition, offset, next) {
    return offsetter.commit(group, 
        { topic: topic
        , partition: partition
        , offset: offset
        }, next)
  }

  face.end = function (next) {
    shuttingDown = true

    return setImmediate(next)
  }

  return face
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
