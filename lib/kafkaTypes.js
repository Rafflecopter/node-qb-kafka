var crypto = require('crypto')

var async = require('async')
  , kafka = require('kafka-node')

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
  return function () {
    var err = arguments[0]

    if (err) { return left(errr) }

    return right(arguments.slice(1))
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

  var toMap = function (results) {
    return pairs.reduce(function (acc, result) {
      var topic = Object.keys(result)[0]
        , partition = Object.keys(result[topic])[0]
        , offset = result[topic][partition]

      return (acc[partition] = pair[offset]) && acc
    }, {})
  }

  face.getPartitionOffsetMap = function (offsetter, group, cb) {
    var _payloads = payloads.map(function (payload) { return [payload] })
    async.map(_payloads, 
              offsetter.fetchCommits.bind(offsetter, group), 
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

    , fetchMaxMs = options.fetch_max_ms
    , fetchMinBytes = options.fetch_min_bytes

    , offsetter = new kafka.Offset(client)
    , payloads = consumerPayloads(topic,
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

    client.send(payloads.get(offsets), 
                encoder, 
                decoder, 
                catchErr(_onSenderErr, noop))

    function _onSenderErr(err) { return (reading = false) && ops.error(err) }
    
    function _setRetry(err) { return (retryDueToError = true) && err }

    function _decoderCb(type, value) {
      if (shuttingDown) { return }

      if (retryDueToError) { return (reading = false) }

      if (type === "message") {
        mbs.enqueue(incomingMessage(value))

      } else {
        mbs.process(_processMessage, _fin)

      }
    }

    function _processMsg(message, next) {
      if (shuttingDown) { return next() }

      ops.data(message, catchErr( next, 
                                  compose(next, 
                                          updateOffsets.bind(null, message))))
    }

    function _updateOffsets(message) {
      var partition = message.get('partition')
        , offset = message.get('offset')

      return (offsets[partition] = offset + 1)
    }

    var _fin = catchErr(ops.error, face.read)
  }

  var ensureOffsets = function (next) {
    if (offsets.length) { return next() }

    payloads
      .getPartitionOffsetMap( 
          offsetter, 
          group,
          catchErr( ops.error, 
                    function(map) { (offsets = map) && next() }))
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
