import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "my-app-producer",
  brokers: ["localhost:9092"],
  requestTimeout: 30000,
  connectionTimeout: 30000,
  retry: {
    initialRetryTime: 1000,
    retries: 10,
  },
});

const producer = kafka.producer();

const sendMessage = async () => {
  const currentTime = new Date().toISOString();
  await producer.send({
    topic: "test-topic",
    messages: [{ value: `Hello KafkaJS user! ${currentTime}` }],
  });
};

async function run() {
  await producer.connect();

  setInterval(async () => {
    await sendMessage();
  }, 1000);
}

run().catch(console.error);
