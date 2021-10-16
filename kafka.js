const { Kafka } = require('kafkajs')
const { CompressionTypes, CompressionCodecs } = require('kafkajs')
const LZ4 = require('kafkajs-lz4')

const { KAFKA_USERNAME: username, KAFKA_PASSWORD: password } = process.env

//const sasl = username && password ? { username, password, mechanism: 'plain' } : null
//const ssl = !!sasl

CompressionCodecs[CompressionTypes.LZ4] = new LZ4().codec

// This creates a client instance that is configured to connect to the Kafka broker provided by
// the environment variable KAFKA_BOOTSTRAP_SERVER
const kafka = new Kafka({
  clientId: 'iftach-demo',
  brokers: ['b-2.demo-cluster-1.ddj69n.c13.kafka.us-east-1.amazonaws.com:9092'],
})


module.exports = kafka