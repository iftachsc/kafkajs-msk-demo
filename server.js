const { CompressionTypes } = require('kafkajs')
const kafka = require('./kafka')
const topic = process.env.TOPIC

const main = async () => {
    try {
        const producer = kafka.producer()
        const admin = kafka.admin()

        console.log("admin starts")
        await producer.connect()
        await admin.connect()
        
        const topics = await admin.listTopics()
        console.log(topics)
        if(topics.includes(topic)){
          await admin.deleteTopics({
            topics: [topic],
            timeout: 10000,
          })
        }
        
        await admin.createTopics({
            validateOnly: false,
            waitForLeaders: true,
            timeout: 5000, //default 5000
            topics: [{
              topic: topic,
              numPartitions: 2, // default: 1
              replicationFactor: 1, //default: 1
              replicaAssignment: [],  // Example: [{ partition: 0, replicas: [0,1,2] }] - default: []
              configEntries: []    // Example: [{ name: 'cleanup.policy', value: 'compact' }] - default: []
          }],
        })
        admin.disconnect()

        console.log("admin finished")
        const responses = await producer.send({
          topic: topic,
          messages: Array.from({length: 50}, (x,i) => {
            return {
            // Name of the published package as key, to make sure that we process events in order
              key: (i%2).toString(),
    
            // The message value is just bytes to Kafka, so we need to serialize our JavaScript
            // object to a JSON string. Other serialization methods like Avro are available.
              value: JSON.stringify({
                given: 'iftach',
                mid: 'johny',
                surnane: 'schonbaum'
              })
            }
          }),
          acks: 0,
          compression: CompressionTypes.None
        })
    
        console.log('Published message', { responses })
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