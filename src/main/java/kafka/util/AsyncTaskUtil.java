package kafka.util;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import kafka.SelfCommit.ConsumerTask;

public class AsyncTaskUtil {
	
	/**
	 * 模拟执行异步任务
	 */
	public static void asyncTask(ConsumerTask kafkaConsumer,ConsumerRecords<Integer, String> records) {
		
		kafkaConsumer.isCommit = false;
		
		Thread s = new Thread(new Runnable() {
			
			@Override
			public void run() {
				
				StringBuffer sb = new StringBuffer();
				
				for (ConsumerRecord<Integer, String> record : records) {
					sb.append(record.value()).append(",");
                }
				
				System.out.println("开始执行"+sb.substring(0, sb.length()-1).toString()+"的耗时任务");
				//浪费一些时间
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				System.out.println("耗时任务"+sb.toString()+"执行完毕");
				
				//任务执行完毕，唤醒kafka
				kafkaConsumer.isCommit = true;
			}
		});
		
		s.start();
	}
}
