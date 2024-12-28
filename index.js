import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "my-app-producer",
  brokers: ["localhost:9092"],
  requestTimeout: 30000,
  connectionTimeout: 30000,
  retry: {
    initialRetryTime: 100,
    retries: 8,
  },
});

const producer = kafka.producer();

const fetchRecentChanges = async () => {
  const url = "https://stream.wikimedia.org/v2/stream/recentchange";
  const response = await fetch(url);
  const reader = response.body.getReader();
  const textDecoder = new TextDecoder();

  while (true) {
    const { done, value } = await reader.read();
    if (done) {
      console.log("No more data to read, reconnecting...");
      break;
    }

    if (value) {
      const chunk = textDecoder.decode(value, { stream: true });

      const lines = chunk.split("\n");
      for (let i = 0; i < lines.length - 1; i++) {
        try {
          if (lines[i].trim() === "") {
            continue;
          }
          if (lines[i].startsWith("data:")) {
            const message = JSON.parse(lines[i].substring(6));
            await sendMessage(message);
          }
        } catch (error) {
          console.error("Error parsing JSON: ", error);
        }
      }
    }
  }
};

const sendMessage = async (message) => {
  const currentTime = new Date().toISOString();
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
