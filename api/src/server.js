const express = require("express");
const { Kafka, logLevel, CompressionTypes } = require("kafkajs");

const app = express();

const kafka = new Kafka({
  clientId: "api",
  brokers: ["localhost:9092"],
  logLevel: logLevel.ERROR,
  retry: {
    initialRetryTime: 300,
    retries: 10
  }
});

const producer = kafka.producer();

/**
 * Add producer to all routes
 */
app.use((req, res, next) => {
  req.producer = producer;
  next();
});

/**
 * Rotas
 */
app.get("/generator", async (req, res) => {
  const message = {
    user: { id: 1, name: "Marco Jardim" },
    course: "Kafka with Node.js",
    grade: 10
  };

  await req.producer.send({
    topic: "id-generator",
    compression: CompressionTypes.GZIP,
    messages: [
      { value: JSON.stringify(message) },
      { value: JSON.stringify(message) }
    ]
  });

  res.json({ message: "ok" });
});

async function start() {
  await producer.connect();

  app.listen(3333, () => {
    console.log("Server is running");
  });
}

start().catch(console.error);
