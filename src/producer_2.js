import { Kafka } from "kafkajs";
import { EventSource } from "eventsource";

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

const fetchRecentChanges = async () => {
  const url = "https://stream.wikimedia.org/v2/stream/recentchange";
  const eventSource = new EventSource(url);

  eventSource.onmessage = async (event) => {
    try {
      const message = JSON.parse(event.data);
      console.log("Received message:", message);
      await sendMessage(message);
    } catch (error) {
      console.error("Error processing message:", error);
    }
  };

  eventSource.onerror = (error) => {
    console.error("Error with EventSource connection:", error);
    eventSource.close();
    setTimeout(fetchRecentChanges, 5000);
  };
};

const sendMessage = async (message) => {
  await producer.send({
    topic: "wikimedia.recentchange",
    messages: [{ value: JSON.stringify(message) }],
  });
};

async function run() {
  await producer.connect();
  console.log("Producer connected to Kafka");

  while (true) {
    await fetchRecentChanges();

    console.log("Waiting for new data...");
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }
}

run().catch(console.error);
