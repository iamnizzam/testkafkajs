// const { Kafka } = require('kafkajs')

// const kafka = new Kafka({
//   clientId: 'my-app',
//   brokers: ['127.0.0.1:9092']
// })

// const producer = kafka.producer()
// const consumer = kafka.consumer({ groupId: 'test-group' })

// const run = async () => {
//   // Producing
//   await producer.connect()
//   setInterval( async () => {
//     await producer.send({
//         topic: 'test-topic',
//         messages: [
//         { value: 'Hello KafkaJS user!' },
//         ],
//     })
// }, 2000);

//   // Consuming
// //   await consumer.connect()
// //   await consumer.subscribe({ topic: 'test-topic', fromBeginning: true })

// //   await consumer.run({
// //     eachMessage: async ({ topic, partition, message }) => {
// //       console.log({
// //         partition,
// //         offset: message.offset,
// //         value: message.value.toString(),
// //       })
// //     },
// //   })
// }

// run().catch(console.error)


const fs = require('fs')
const ip = require('ip')

const { Kafka, CompressionTypes, logLevel } = require('kafkajs')

const host = process.env.HOST_IP || ip.address()

const kafka = new Kafka({
  logLevel: logLevel.DEBUG,
  brokers: [`${host}:9092`],
  clientId: 'example-producer',
})

const topic = 'topic-test'
const producer = kafka.producer()

const getRandomNumber = () => Math.round(Math.random(10) * 1000)
const createMessage = num => ({
  key: `key-${num}`,
  value: `value-${num}-${new Date().toISOString()}`,
})

const sendMessage = () => {
  return producer
    .send({
      topic,
      compression: CompressionTypes.GZIP,
      messages: Array(getRandomNumber())
        .fill()
        .map(_ => createMessage(getRandomNumber())),
    })
    .then(console.log)
    .catch(e => console.error(`[example/producer] ${e.message}`, e))
}

const run = async () => {
  await producer.connect()
  setInterval(sendMessage, 3000)
}

run().catch(e => console.error(`[example/producer] ${e.message}`, e))

const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2']

errorTypes.map(type => {
  process.on(type, async () => {
    try {
      console.log(`process.on ${type}`)
      await producer.disconnect()
      process.exit(0)
    } catch (_) {
      process.exit(1)
    }
  })
})

signalTraps.map(type => {
  process.once(type, async () => {
    try {
      await producer.disconnect()
    } finally {
      process.kill(process.pid, type)
    }
  })
})




