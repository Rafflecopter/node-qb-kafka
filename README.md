# qb-kafka [![Build Status][1]][2]

A [kafka](https://kafka.apache.org) backend and dialect for [qb](https://github.com/Rafflecopter/node-qb). A single backend must be selected to be used with qb. This backend uses kafka's nature and guarentees to provide a work queue on receiving of messages to be processed. In addition, kafka can be used as the dialect to skip the rpc work queue pushthat is necessary for other dialects (because they do not share backends).

## Usage

```
npm install qb qb-kafka --save
```

```javascript
var QB = require('qb').backend(require('qb-kafka').backend)
  , qb = new QB(options)

qb.speaks(require('qb-kafka').dialect, kafkaDialectOptions)
```

## License

MIT in LICENSE file

[1]: https://travis-ci.org/Rafflecopter/node-qb-kafka.png?branch=master
[2]: http://travis-ci.org/Rafflecopter/node-qb-kafka
