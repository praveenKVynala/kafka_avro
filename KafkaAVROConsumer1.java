package com.citi;
//import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import com.citi.Employee;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;



public class KafkaAVROConsumer1 {
	
	
    final static List<String> TOPICS = Arrays.asList("topicx");
    final static Properties props = new Properties();

	public static void main(String[] args) throws Exception  {
		
    	//properties for Consumer
    	props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092" );
    	props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"test-group4" );
//    	props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.LongDeserializer" );
//    	props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer" );
    	props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    	props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
		
		
    	//AVRO  Deserializer
    	
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.setProperty("schema.registry.url", "http://127.0.0.1:8081");
        props.setProperty("specific.avro.reader", "true");
    	
        Consumer<String, Employee> consumer = new KafkaConsumer<String, Employee>(props);
        consumer.subscribe(TOPICS);
        
        try{
        	while (true){
            ConsumerRecords<String, Employee> records = consumer.poll(1000);
            	if (records !=null) {
		            for (ConsumerRecord<String, Employee> record : records){
		            	
	
		                System.out.println(record.value());
		            }

	        	
	            consumer.commitAsync();
            	}
        	} 
        }
        catch (Exception e){
            System.out.println(e.getMessage());
        }
        finally {
            consumer.close();
            System.out.println("Done");
        }
	}
}	
	