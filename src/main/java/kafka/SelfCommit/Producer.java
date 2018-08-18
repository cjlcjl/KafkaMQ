package kafka.SelfCommit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Scanner;

/**
 * Created by sunilpatil on 12/28/15.
 */
public class Producer {
    private static Scanner in;
    public static void main(String[] argv)throws Exception {
    	
        String topicName = "trytry";
        in = new Scanner(System.in);
        System.out.println("Enter message(type exit to quit)");

        //Configure the Producer
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"172.16.161.161:9092");
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.IntegerSerializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        
        configProperties.put(ProducerConfig.ACKS_CONFIG,"1");
        configProperties.put(ProducerConfig.RETRIES_CONFIG,"3");

        org.apache.kafka.clients.producer.Producer producer = new KafkaProducer(configProperties);
        String line = in.nextLine();
        
        while(!line.equals("exit")) {
            //TODO: Make sure to use the ProducerRecord constructor that does not take parition Id
            ProducerRecord<Integer, String> rec = new ProducerRecord<Integer, String>(topicName,line);
            producer.send(rec);
            
            line = in.nextLine();
        }
        in.close();
        producer.close();
    }
}
