const { Kafka, Partitioners } = require('kafkajs');
const { SchemaRegistry } = require('@kafkajs/confluent-schema-registry');

const message = {
    key: '123',
    value: { fullName: 'John Doe', age: 1 },
    timestamp: Date.now(),
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

const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });

const schemaRegistry = new SchemaRegistry({
    host: '<sr-host-url>',
    auth: {
        username: "<sr-api-key>",
        password: "<sr-api-secret>"
    }
}); // Confluent Cloud Schema Registry endpoint

const produceMessage = async (message) => {
    await producer.connect();

    const topic = '<topic-name>';

    const registeredSchemaId = await schemaRegistry.getLatestSchemaId(message.subject)

    // console.log("registeredSchemaId =>", registeredSchemaId)

    const encodedMessage = {
        ...message,
        value: await schemaRegistry.encode(registeredSchemaId, message.value),
        headers: {
            'schemaId': registeredSchemaId.toString(),
        }
    } // Encode using the uploaded schema

    await producer.send({
        topic,
        messages: [encodedMessage],
        acks: 1
    });

    await producer.disconnect();
};

produceMessage(message).catch(console.error);