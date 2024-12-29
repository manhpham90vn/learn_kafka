import { Kafka } from "kafkajs";
import { Client } from "@opensearch-project/opensearch";

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

const consumer = kafka.consumer({
  groupId: "test-group",
});
const client = new Client({ node: "http://localhost:9200" });

const ensureIndexExists = async (indexName) => {
  const isIndexExists = await client.indices.exists({ index: indexName });

  if (!isIndexExists.body) {
    await client.indices.create({
      index: indexName,
      body: {
        mappings: {
          properties: {
            title_url: { type: "text" },
            user: { type: "text" },
            comment: { type: "text" },
            timestamp: { type: "date" },
          },
        },
      },
    });
  }
};

const bulkPushToOpenSearch = async (indexName, messages) => {
  const bulkBody = [];

  messages.forEach((message) => {
    bulkBody.push({ index: { _index: indexName } });
    bulkBody.push({
      title_url: message.title_url,
      user: message.user,
      comment: message.comment,
      timestamp: message.timestamp,
    });
  });

  if (bulkBody.length > 0) {
    const response = await client.bulk({
      body: bulkBody,
    });

    if (response.body.errors) {
      console.error(
        "Errors occurred during bulk operation:",
        response.body.items
      );
    } else {
      console.log(`Successfully indexed ${messages.length} messages`);
    }
  }
};

async function run() {
  const indexName = "wikimedia-recentchange";
  const topicName = "wikimedia.recentchange";
  await ensureIndexExists(indexName);
  await consumer.connect();
  await consumer.subscribe({
    topic: topicName,
    fromBeginning: true,
  });

  const messageBuffer = [];
  const bufferLimit = 1000;
  const bufferTimeout = 5000;
  let bufferTimeoutId;

  const flushBuffer = async () => {
    if (messageBuffer.length > 0) {
      const messagesToProcess = [...messageBuffer];
      messageBuffer.length = 0;
      clearTimeout(bufferTimeoutId);
      await bulkPushToOpenSearch(indexName, messagesToProcess);
    }
  };

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const messageValue = JSON.parse(message.value.toString());
      messageBuffer.push(messageValue);

      if (messageBuffer.length >= bufferLimit) {
        await flushBuffer();
      } else {
        clearTimeout(bufferTimeoutId);
        bufferTimeoutId = setTimeout(flushBuffer, bufferTimeout);
      }
    },
  });
}

run().catch(console.error);
