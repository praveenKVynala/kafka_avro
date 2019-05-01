package com.citi;
import com.citi.Employee;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class KafkaAVROProducer1 {
	
	final static Properties props = new Properties();
	final static String TOPIC_NAME = "topicx";


	public static void main(String[] args) throws Exception  {
		
        //properties for producer
		
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.LongSerializer");
//		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
		props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		props.setProperty(ProducerConfig.RETRIES_CONFIG,"10");
//		props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
//		props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
//		props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
//		props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "10");
//		props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		
		
		//AVRO Configurations
		
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.setProperty("schema.registry.url", "http://127.0.0.1:8081");
		
        Producer<String, Employee> producer = new KafkaProducer<String, Employee>(props);
        long key=0l;
        

        try {
        	while(true) {
        	String js="{\"empid\":\"1007581\",\"first_name\":\"RAM\",\"last_name\":\"Raj\",\"age\":\"25\",\"passport\":\"true\"}";
    		
  		  //JSON parser object to parse read file
          JSONParser jsonParser = new JSONParser();
  
  			//Read JSON file
              Object obj = jsonParser.parse(js);
              JSONObject employee=(JSONObject) obj ;
            //Get employee first name
              String firstName = (String) employee.get("first_name");   
               
              //Get employee last name
              String lastName = (String) employee.get("last_name");
               
              //Get employee Number
              String strempid = (String) employee.get("empid");
              int empid=Integer.parseInt(strempid);
              
              String strage = (String) employee.get("age");
              int age=Integer.parseInt(strage);
              
              String strpassport = (String) employee.get("passport");
              boolean passport=Boolean.parseBoolean(strpassport);
              
              
              
              
          	Employee value = Employee.newBuilder()
                    .setAge(age)
                    .setEmpid(empid)
                    .setFirstName(firstName)
                    .setLastName(lastName)
                    .setPassport(passport)
                    .build(); 
        
        	

    	    		  
    	              if (value!= null){
    		            ProducerRecord<String, Employee> record = new ProducerRecord<String, Employee>(TOPIC_NAME,Long.toString(key),value);
    		            producer.send(record, new Callback() {
    						@Override
    						public void onCompletion(RecordMetadata recordMetaData, Exception e) {
    							if (e== null) {
    					           	
    					           	System.out.printf("\n Messag Sent Successfully to Kafka: \n");
    					           	System.out.printf("\n Message Meta Data \t: %s",recordMetaData.toString());
    					           	System.out.printf("\n Topic Name \t: %s",recordMetaData.topic());
    					           	System.out.printf("\n Partition Number \t: %d", recordMetaData.partition());
    					           	System.out.printf("\n Offset \t: %d",recordMetaData.offset());
    					           	System.out.printf("\n TimeStamp \t: %s", recordMetaData.timestamp());
    					           								 
    							}
    							else {
    								System.err.printf("Error while sending Message :%s \n",e);
    							}
    						}
    					});
    	              }
    		            producer.flush();
//    		            System.out.print(producer.);
    		            key++;
    		            	
    	              }
    	    	  	
    	          }	      	 
    	      finally 
    	      {
    	          producer.close();
    	          System.out.println("Producer Stopped");
    	      }
            
            
            
    		}
    	 
    }