package log_consumer;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.message.MessageAndMetadata;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import java.time.*;;
public class log_consumer {
	 private static final int NUM_THREADS = 1;
	 private static Calendar date;
	 static boolean isTiming=false;
	 static int time=0,MAX_ALLOWED=60;
	  static String topic = "tail";
	  
	 static boolean calcTime() {
	     if(date.getTime().getTime()-time>MAX_ALLOWED) {//variable time holds the last time it saved. get time will get the current time and will check if its above or below the threshhold
	         return true;
	     } else {
	         return false;
	     }
	 }
	 
	 static boolean checkTime() {//main method that will be called for all movements
	    
		 if(isTiming) {//will check if it has already started timing
	     boolean aboveThreshold=calcTime();//will then call a method that will return true or false dpending on how long the user took
	    

	    if(aboveThreshold) {//if he took too long
	     //here the user will be prompted why he hasnt been working for the time or whatever
	     //시간이 넘으면
	    	return false;
	    } else {
	     //do nothing it was within the time limit
	    // 시간이 넘지 않으면 
	    	return true;
	    }


	 } else {//it wasnt already timing
		date =Calendar.getInstance();
	     time=(int) date.getTime().getTime();
	     isTiming=true;
	 }
		 return false;
	 }//end of method
	
	 
	 public static void main(String[] args) throws Exception {
		 
			Properties props = new Properties();
	       	props.put("group.id", "test-group");
	       	props.put("zookeeper.connect", "kafka1:2181,kafka2:2181,kafka3:2181");
	        props.put("auto.commit.interval.ms", "1000");
	        ConsumerConfig consumerConfig = new ConsumerConfig(props);
	        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(consumerConfig);
	        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
	          topic += args[0];
	          topicCountMap.put(topic, NUM_THREADS);
	    	Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);

	    	List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
	        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
	          int k=0;
	        checkTime();
	        if(checkTime())
	        {
	        	final KafkaStream<byte[], byte[]> stream = streams.get(k);//?
	        	MessageAndMetadata<byte[], byte[]> messageAndMetadata = null;//?
	        	String tmp = new String(messageAndMetadata.message());
	        	
	        	
	        }else
	        {
	        	
	        }
	        
	 }
}
