var kafka = require('kafka-node')
  , async = require('async')

  , client = new kafka.Client(process.env.ZK_HOST || "localhost:2181")
  , producer = new kafka.Producer(client)

  , topic = "benchmark"
  , group = "benchmarkers"
  , numMsgsToSend = 10000

  , partitions = Array.apply(null, { length: 32 }).map(Number.call, Number)
  , consumerPayloads = partitions.map(function (p) { return { topic: topic, partition: p } })
  , consumerOpts =  { groupId: group
                    , autoCommit: false
                    , autoCommitIntervalMs: 5000
                    , autoCommitMsgCount: 100
                    , fromOffset: false
                    }
  , consumer = new kafka.Consumer(client, consumerPayloads, consumerOpts)


// some state
var numMsgsProcessed = 0
  , numMsgsSent = 0
  , startTime


consumer.on('error', console.error).on('message', countMsg)

if (producer.ready) {
  send()
} else {
  producer.on('ready', send)
}


function countMsg(message) {
  if (++numMsgsProcessed === numMsgsToSend) {
    console.log(numMsgsProcessed 
                + " messages processed in " 
                + (Date.now() - startTime) 
                + " ms")
  }
}

function send() {
  startTime = Date.now()

  var q = async.queue(function (task, cb) {
    producer.send([task], cb)
  }, 30)

  q.drain = function () {
    console.log('Pushed ' + numMsgsSent + ' messages')
  }

  for (var k=0; k < numMsgsToSend; k++) {
    var m = Math.random().toString(36).replace(/[^a-z]+/g, '')
      , p = Math.floor(Math.random() * partitions.length)
      , payload = {messages: m, partition: p, topic: topic}

    q.push(payload, function (err) { 
      numMsgsSent++

      err && console.error(err)
    })
  }
}
