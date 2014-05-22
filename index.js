// qb-kafka/index.js
// Provide kafka backend and dialect for qb

module.exports =
{ backend: require('./lib/backend')
, dialect: require('./lib/dialect')
}
