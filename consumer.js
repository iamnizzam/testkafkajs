// const { Kafka, CompressionTypes, logLevel } = require('kafkajs')

// const kafka = new Kafka({
//   clientId: 'my-app',
//   logLevel: logLevel.DEBUG,
//   brokers: ['127.0.0.1:9092']
// })

// const producer = kafka.producer()
// const consumer = kafka.consumer({ groupId: 'test-group' })

// const run = async () => {
//   // Producing
// //   await producer.connect()
// //   setInterval(() => {
// //     await producer.send({
// //         topic: 'test-topic',
// //         messages: [
// //         { value: 'Hello KafkaJS user!' },
// //         ],
// //     })
// // }, 2000);

//   // Consuming
//   await consumer.connect()
//   await consumer.subscribe({ topic: 'test-topic', fromBeginning: true })

//   await consumer.run({
//     eachMessage: async ({ topic, partition, message }) => {
//       console.log({
//         partition,
//         offset: message.offset,
//         value: message.value.toString(),
//       })
//     },
//   })
// }

// run().catch(console.error)

const fs = require('fs')
const ip = require('ip')

const { Kafka, logLevel } = require('kafkajs')

const host = process.env.HOST_IP || ip.address()

const kafka = new Kafka({
  logLevel: logLevel.INFO,
  brokers: [`${host}:9092`],
  clientId: 'example-consumer',
})

const topic = 'topic-test'
const consumer = kafka.consumer({ groupId: 'test-group' })

const run = async () => {
  await consumer.connect()
  await consumer.subscribe({ topic, fromBeginning: true })
  await consumer.run({
    // eachBatch: async ({ batch }) => {
    //   console.log(batch)
    // },
    eachMessage: async ({ topic, partition, message }) => {
      const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`
      console.log(`- ${prefix} ${message.key}#${message.value}`)
    },
  })
}

run().catch(e => console.error(`[example/consumer] ${e.message}`, e))

const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2']

errorTypes.map(type => {
  process.on(type, async e => {
    try {
      console.log(`process.on ${type}`)
      console.error(e)
      await consumer.disconnect()
      process.exit(0)
    } catch (_) {
      process.exit(1)
    }
  })
})

signalTraps.map(type => {
  process.once(type, async () => {
    try {
      await consumer.disconnect()
    } finally {
      process.kill(process.pid, type)
    }
  })
})




