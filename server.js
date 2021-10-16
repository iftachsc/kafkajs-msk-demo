const { CompressionTypes } = require('kafkajs')
const kafka = require('./kafka')
const topic = process.env.TOPIC
const numMessages = process.env.NUM_MESSAGES


function sleep(millis) {
  return new Promise(resolve => setTimeout(resolve, millis));
}

function singleKbMessage() {
  return JSON.stringify({
    given: 'iftach',
    mid: 'johny',
    surnane: 'schonbaum',
    address: '10th helsinki str. tel aviv.',
    country: 'israel',
    age: 123,
    profession: 'shoe maker',
    family: [
      {
        given: 'yoav',
        mid: 'johny',
        surnane: 'schonbaum',
        address: '10th helsinki str. tel aviv.',
        country: 'israel',
        age: 123,
        profession: 'shoe maker'
      },
      {
        given: 'roy',
        mid: 'johny',
        surnane: 'schonbaum',
        address: '19th helsinki str. tel aviv.',
        country: 'israel',
        age: 123,
        profession: 'shoe maker'
      },
      {
        given: 'margaret',
        mid: 'johny',
        surnane: 'schonbaum',
        address: '19th helsinki str. tel aviv.',
        country: 'israel',
        age: 123,
        profession: 'shoe maker'
      },
      {
        given: 'shlomo',
        mid: 'johny',
        surnane: 'schonbaum',
        address: '19th helsinki str. tel aviv.',
        country: 'israel',
        age: 123,
        profession: 'shoe maker'
      },
      {
        given: 'shlomo',
        mid: 'johny',
        surnane: 'schonbaum',
        address: '19th helsinki str. tel aviv.',
        country: 'israel',
        age: 123,
        profession: 'shoe maker'
      },
      {
        given: 'shlomo',
        mid: 'johny',
        surnane: 'schonbaum',
        address: '19th helsinki str. tel aviv.',
        country: 'israel',
        age: 123,
        profession: 'shoe maker'
      }
    ]
  })
}
function genMessage() {
  return {
    topic: topic,
    messages: Array.from({length: parseInt(numMessages)}, (x,i) => {
      return {
      // Name of the published package as key, to make sure that we process events in order
        key: (i%2).toString(),

      // The message value is just bytes to Kafka, so we need to serialize our JavaScript
      // object to a JSON string. Other serialization methods like Avro are available.
        value: singleKbMessage()
      }
    }),
    acks: 0,
    compression: CompressionTypes.None
  }
}

function bytesLength(str) {
  return Buffer.byteLength(str, "utf-8");
}

function genBatch() {
  return {
    topicMessages: Array.from({length: parseInt(numMessages)}, (x,i) => {
      return {
        topic: topic,
        messages: [{
          key: (i%2).toString(),
          value: singleKbMessage()
        }]
      }
    }),
    acks: 0,
    compression: CompressionTypes.None
  }
}

const main = async () => {
    try {
        const producer = kafka.producer()
        const admin = kafka.admin()

        console.log("admin starts")
        await producer.connect()
        await admin.connect()
        
        // const topics = await admin.listTopics()
        // console.log(topics)
        // console.log(topics.includes(topic))
        // if(topics.includes(topic)){
        //   console.log("deleting")
        //   await admin.deleteTopics({
        //     topics: [topic],
        //     timeout: 10000,
        //   })
        // }
        // await sleep(5000)
        
        
        // await admin.createTopics({
        //     validateOnly: false,
        //     waitForLeaders: true,
        //     timeout: 5000, //default 5000
        //     topics: [{
        //       topic: topic,
        //       numPartitions: 2, // default: 1
        //       replicationFactor: 1, //default: 1
        //       replicaAssignment: [],  // Example: [{ partition: 0, replicas: [0,1,2] }] - default: []
        //       configEntries: []    // Example: [{ name: 'cleanup.policy', value: 'compact' }] - default: []
        //   }],
        // })
        // admin.disconnect()

        
        console.log("admin finished")
        console.log("sending "+numMessages+" message/s each request")

        while(true) {
          await producer.send(genMessage())
          await sleep(50)
        }
    
        console.log(responses.length)
        console.log('Published message', { responses })
        producer.disconnect();
      } catch (error) {
        console.error('Error publishing message', error)
      }
  }
  
  main().catch(error => {
    console.error(error)
    process.exit(1)
  })

  //KAFKA_BOOTSTRAP_SERVER="b-2.demo-cluster-1.ddj69n.c13.kafka.us-east-1.amazonaws.com:9094,b-1.demo-cluster-1.ddj69n.c13.kafka.us-east-1.amazonaws.com:9094" TOPIC="npm-package-published"
  //TOPIC="people"