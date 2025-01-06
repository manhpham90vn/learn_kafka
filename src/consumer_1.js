import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "my-app-consumer",
  brokers: ["localhost:9092"],
  requestTimeout: 30000,
  connectionTimeout: 30000,
  retry: {
    initialRetryTime: 1000,
    retries: 10,
  },
});

const consumer = kafka.consumer({ groupId: "test-group" });

async function run() {
  await consumer.connect();
  await consumer.subscribe({
    topic: "wikimedia.recentchange",
    fromBeginning: true,
  });
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        value: message.value.toString(),
      });
    },
  });
}

run().catch(console.error);
