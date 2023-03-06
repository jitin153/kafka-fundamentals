package com.jitin.kafka.demo.c.avro.serializer;

import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.jitin.kafka.demo.c.avro.Order;

import io.confluent.kafka.serializers.KafkaAvroSerializer;

public class OrderProducerWithAvroSerializerGeneric {
	/*
	 * Run mvn generate-sources or mvn install command to generate java claases form .avsc schema files.
	 */
	public static void main(String[] args) {
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("key.serializer", KafkaAvroSerializer.class.getName());
		properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
		/*
		 * Producer will schema(order) to the schema registry.
		 * You can verify this by hitting http://localhost:8081
		 */
		properties.setProperty("schema.registry.url","http://localhost:8081");
		
		KafkaProducer<String, GenericRecord> orderProducer = new KafkaProducer<>(properties);
		Parser parser = new Parser();
		Schema schema = parser.parse("{\r\n"
				+ "	\"namespace\": \"com.jitin.kafka.avro\",\r\n"
				+ "	\"type\": \"record\",\r\n"
				+ "	\"name\": \"Order\",\r\n"
				+ "	\"fields\": [\r\n"
				+ "		{\"name\": \"customerName\", \"type\": \"string\"},\r\n"
				+ "		{\"name\": \"product\", \"type\": \"string\"},\r\n"
				+ "		{\"name\": \"quantity\", \"type\": \"int\"}\r\n"
				+ "	]\r\n"
				+ "}");
		GenericRecord order = new GenericData.Record(schema);
		order.put("customerName", "Test Customer");
		order.put("product", "iPhone");
		order.put("quantity", 5);
		ProducerRecord<String, GenericRecord> record = new ProducerRecord<>("OrderAvroTopic-Generic", order.get("customerName").toString(), order);
		try {
			orderProducer.send(record);
			System.out.println("Message sent!");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			orderProducer.close();
		}
	}

}
