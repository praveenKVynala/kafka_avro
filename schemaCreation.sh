sh java -jar avro-tools-1.8.2.jar compile schema workplace/KafkaAVRO-PC-POC/src/resources/avro/Employee.avsc workplace/KafkaAVRO-PC-POC/src/

#kafka-avro-console-producer--broker-list localhost:9092--topic topic2--property schema.registry.url=localhost:8081--property value.schema='{"type":"record","name":"myrecord","fields":[{"name":"n","type":"string"}]}'


