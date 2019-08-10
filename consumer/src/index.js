const { Kafka, logLevel } = require("kafkajs");

const kafka = new Kafka({
  clientId: "consumer",
  brokers: ["localhost:9092"],
  logLevel: logLevel.WARN
});

const topic = "id-generator";
const consumer = kafka.consumer({ groupId: "consumer-group" });

async function start() {
  await consumer.connect();
  await consumer.subscribe({ topic });

  consumer.on("consumer.connect", () => {
    console.log("ready");
  });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      // const prefix = `${topic}[${partition} | ${message.offset}] / ${
      //   message.timestamp
      // }`;
      // console.log(`- ${prefix} ${message.key}#${message.value}`);

      const payload = JSON.parse(message.value);

      console.log(`Name: ${payload.user.name}, id-generated: ${Date.now()}`);
    }
  });
}

start().catch(console.error);
