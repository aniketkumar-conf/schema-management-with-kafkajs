const { Kafka } = require('kafkajs');
const { SchemaRegistry } = require('@kafkajs/confluent-schema-registry');

const config = {
  subject: '<subject-name>'
};

const kafka = new Kafka({
  clientId: 'dynamic-schema-producer',
  brokers: ['<bootstrap-url>'], // Confluent Cloud broker endpoint
  ssl: true,
  sasl: {
    mechanism: 'plain',
    username: '<api-key>',
    password: '<api-secret>',
  },
});

const consumer = kafka.consumer({ groupId: 'my-consumer-group' });

const schemaRegistry = new SchemaRegistry({
  host: '<sr-host-url>',
  auth: {
    username: "<sr-api-key>",
    password: "<sr-api-secret>"
  }
}); // Confluent Cloud Schema Registry endpoint

const consumeMessages = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: '<topic-name>', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      //   const schemaId = message.headers['schemaId'];

      const schemaId = await schemaRegistry.getLatestSchemaId(config.subject)
      // console.log("schemaId =>", schemaId)

      const decodedMessage = await schemaRegistry.decode(message.value); // Decode the payload
      console.log("decodedMessage =>", decodedMessage);
    },
  });
};

consumeMessages().catch(console.error);