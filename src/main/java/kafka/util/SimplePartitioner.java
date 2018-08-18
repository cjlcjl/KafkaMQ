/**
 * Copyright (c) 2015, Shanghai World Fund Co.,Ltd All Rights Reserved.
 *
 * 包路径:cn.com.spider.mq.kafka
 *
 * 当前类名称:SimplePartitioner.java
 *
 * @author   wanguohui
 *  
 *    2015~2016 上海万丰文化传播有限公司-版权所有
 *
 */
package kafka.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.partition.Consumer0;

/**
 * 简单分区类
 *
 * @author wanguohui
 *
 *         2015年11月3日 下午1:38:19
 * 
 */
public class SimplePartitioner implements Partitioner {

	private static final Logger logger = LoggerFactory.getLogger(SimplePartitioner.class);
	
	@Override
	public void configure(Map<String, ?> configs) {
		logger.info("调用configure()");
	}


	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
		// TODO Auto-generated method stub
		
		List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
		
        //不传key的话，按照value来分区
		if(keyBytes == null) {
			logger.info("无key，用value来分区");
			return value.hashCode()%numPartitions;
        }else {
        	logger.info("用key来分区");
        	return (Integer)key;
        }
	}

	@Override
	public void close() {
		logger.info("调用close()");
	}
	
	
}
