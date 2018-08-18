package kafka.SelfCommit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.Scanner;

/**
 * Created by sunilpatil on 12/28/15.
 */
public class ConsumerNormal {
	
	private static final Logger logger = LoggerFactory.getLogger(ConsumerNormal.class);
	
    private static Scanner in;
    
    public static boolean isCommit = false;

    public static void main(String[] argv)throws Exception{
        in = new Scanner(System.in);
        
        String topicName = "trytry";
        String groupId = "1";

        ConsumerThread consumerRunnable = new ConsumerThread(topicName,groupId);
        consumerRunnable.start();
        String line = "";
        while (!line.equals("exit")) {
            line = in.next();
        }
        consumerRunnable.getKafkaConsumer().wakeup();
        System.out.println("Stopping consumer .....");
        consumerRunnable.join();
    }

    private static class ConsumerThread extends Thread{
        private String topicName;
        private String groupId;
        private KafkaConsumer<Integer,String> kafkaConsumer;

        public ConsumerThread(String topicName, String groupId){
            this.topicName = topicName;
            this.groupId = groupId;
        }
        public void run() {
            Properties configProperties = new Properties();
            configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.161.161:9092");
            configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
            configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            configProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "simple");

            //Figure out where to start processing messages from
            kafkaConsumer = new KafkaConsumer<Integer, String>(configProperties);
            
            
    		ConsumerRebalanceListener listener = new ConsumerRebalanceListener() {

    			@Override
    			//在rebalance操作之前调用，用于我们提交消费者偏移
    			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    				
    				logger.info("消费者集群有变化了");
    				
    				kafkaConsumer.commitSync();
    			}

    			@Override
    			//在rebalance操作之后调用，用于我们拉取新的分配区的偏移量
    			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    				for (TopicPartition tp : partitions) {
    					OffsetAndMetadata offsetAndMetaData = kafkaConsumer.committed(tp);
    					long startOffset = offsetAndMetaData != null ? offsetAndMetaData.offset() : -1L;

    					if(startOffset >= 0) {
    						
    						logger.info("topic为"+tp.topic()+"的第"+tp.partition()+"个分区有需要seek的偏移量");
    						
    						kafkaConsumer.seek(tp, startOffset);
    					}
    				}
    			}
    		};
            
            kafkaConsumer.subscribe(Arrays.asList(topicName),listener);
            //Start processing messages
            try {
                while (true) {
                		
                		ConsumerRecords<Integer, String> records = kafkaConsumer.poll(100);
                    
	                    for (ConsumerRecord<Integer, String> record : records) {
	                    	System.out.println(record.key()+"..."+record.value());
	                    }
	                    
	                    //如果执行完毕拉取的任务才继续拉取
                    
                    	kafkaConsumer.commitSync(); 
                }
            }catch(WakeupException ex){
                System.out.println("Exception caught " + ex.getMessage());
            }finally{
                kafkaConsumer.close();
                System.out.println("After closing KafkaConsumer");
            }
        }
        public KafkaConsumer<Integer,String> getKafkaConsumer(){
           return this.kafkaConsumer;
        }
    }
}

