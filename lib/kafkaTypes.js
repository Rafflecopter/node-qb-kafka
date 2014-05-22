var crypto = require('crypto')

var async = require('async')
  , kafka = require('kafka-node')
  , protocol = require('../node_modules/kafka-node/lib/protocol')

module.exports =  
{ producerPayload:  producerPayload
, outgoingMessage:  outgoingMessage
, consumer:         consumer
}

var compose = function () {
  var fns = arguments

  return function (result) {
    for (var i = fns.length - 1; i > -1; i--) {
      result = fns[i].call(this, result)
    }

    return result
  }
}

var catchErr = function (left, right) {
  return function (err) {
    return err 
            ? left(err) 
            : right.apply(null, Array.prototype.slice.call(arguments, 1))
  }
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
function consumerPayloads(topic, fetchMaxBytes, instanceId, nInstances, nPartitions) {
  var partitions = []
    , payloads = []
    , face = {}

  for (var k=0; k < nPartitions; k++) {
    if ((k + instanceId) % nInstances === 0) partitions.push(k)
  }

  payloads = partitions.map(function (p) {
    return  { topic: topic
            , partition: p
            , maxBytes: fetchMaxBytes
            }
  })

  var toMap = function (results) {
    return results.reduce(function (acc, result) {
      var topic = Object.keys(result)[0]
        , partition = Object.keys(result[topic])[0]
        , offset = result[topic][partition]

      acc[partition] = offset < 0 ? 0 : offset

      return acc
    }, {})
  }

  face.getPartitionOffsetMap = function (client, group, cb) {
    var _payloads = payloads.map(function (payload) { return [payload] })

    async.map(_payloads, 
              client.sendOffsetFetchRequest.bind(client, group), 
              catchErr(cb, compose(cb.bind(null, null), toMap)))
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

    , fetchMaxWaitMs = options.fetch_max_wait_ms
    , fetchMinBytes = options.fetch_min_bytes

    , payloads = consumerPayloads(topic,
                                  options.fetch_max_bytes,
                                  options.instance_id,
                                  options.num_instances,
                                  options.num_partitions)

  var read = function () {
    if (shuttingDown || reading) { return }

    reading = true
    
    var retryDueToError = false
      , mbs = messageBins(payloads.getPartitions())
      , encoder = protocol.encodeFetchRequest(fetchMaxWaitMs, fetchMinBytes)
      , decoder = protocol
                    .decodeFetchResponse(
                        catchErr( compose(ops.error, _setRetry), 
                                  _decoderCb))

      , _send = client.send.bind( client,
                                  payloads.get(offsets),
                                  encoder,
                                  decoder,
                                  catchErr(_onSenderErr, noop))

    if (client.ready) {
      _send()
    } else {
      client.on('ready', _send)
    }

    function _onSenderErr(err) { return (reading = false) && ops.error(err) }
    
    function _setRetry(err) {
      retryDueToError = true
      ready = false
      return err 
    }

    function _decoderCb(type, value) {
      if (shuttingDown || retryDueToError) { return }

      if (type === "message") {
        mbs.enqueue(incomingMessage(value))

      } else {
        mbs.process(_processMsg, _fin)

      }
    }

    function _processMsg(message, next) {
      if (shuttingDown) { return next() }

      ops.data(message, catchErr( next, 
                                  compose(next, 
                                          _updateOffsets.bind(null, message))))
    }

    function _updateOffsets(message) {
      var partition = message.get('partition')
        , offset = message.get('offset')

      return (offsets[partition] = offset + 1) && null
    }

    var _fin = compose(
        catchErr(ops.error, face.read),
        function (err) { return (reading = false) && err })
  }

  var ensureOffsets = function (next) {
    if (Object.keys(offsets).length) { return next() }

    payloads
      .getPartitionOffsetMap( 
          client, 
          group,
          catchErr( ops.error, 
                    function(map) { (offsets = map) && next() }))
  }

  face.read = ensureOffsets.bind(null, read)

  face.on = function (evt, cb) {
    cb && (ops[evt] = cb)
  }

  face.commit = function (partition, offset, next) {
    return client.sendOffsetCommitRequest(group, 
        [ { topic: topic
          , partition: partition
          , offset: offset + 1
          , metadata: 'm'
          }
        ], next)
  }

  face.close = function (next) {
    shuttingDown = true

    ops.end()

    return setImmediate(client.close.bind(client, next))
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
    return async.each(qs, function (q, cb) {
      async.eachSeries(q, handler, cb)
    }, done)
  }

  return face
}
