const kafka = require('./kafka')

const main = async () => {
    try {
        const producer = kafka.producer()
        await producer.connect()
        const responses = await producer.send({
          topic: process.env.TOPIC,
          messages: [{
            // Name of the published package as key, to make sure that we process events in order
            key: 'key',
    
            // The message value is just bytes to Kafka, so we need to serialize our JavaScript
            // object to a JSON string. Other serialization methods like Avro are available.
            value: JSON.stringify({
              given: 'iftach',
              surnane: 'schonbaum'
            })
          }]
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