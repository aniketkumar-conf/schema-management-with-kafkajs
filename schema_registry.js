const { SchemaRegistry, SchemaType, readAVSCAsync } = require('@kafkajs/confluent-schema-registry')
const axios = require('axios');

const config = {
    host: '<sr-host-url>',
    auth: {
        username: "<sr-api-key>",
        password: "<sr-api-secret>"
    }
}

const registry = new SchemaRegistry(config)

const schemaRegistryRegistration = async function () {
    // Upload a schema to the registry
    const schema = await readAVSCAsync('schemas/schema-2.avsc')

    const { id } = await registry.register({
        type: SchemaType.AVRO,
        schema: JSON.stringify(schema)
    }, {
        subject: '<subject-name>'
    })

    console.log("Schema Registry id =>", id)

    // Get the subject-version pairs identified by the input ID.
    axios({
        method: 'get',
        // url: config.host + '/schemas/ids/' + id + '/versions',
        url: config.host + '/subjects/test_topic-value/versions',
        headers: {
            'accept': 'application/vnd.schemaregistry.v1+json'
        },
        auth: config.auth
    }).then(function (res) {
        console.log("List of subject-versions pair =>", res.data)
    }).catch(function (err) {
        console.log(err.message)
    })
}

schemaRegistryRegistration()