package com.spirit.cdja.util;

import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * kafka工具类
 * 
 * @author lihe
 *
 */
public class KafkaUtils {
	private static final Logger logger = LoggerFactory.getLogger(KafkaUtils.class);
	private static Producer<String, Object> producer = null;
	private static  Consumer<String, Object> consumer= null;
	/**
	 * 生产者属性
	 */
	static {
		Properties props_p = new Properties();
		props_p.put("bootstrap.servers", "");// 服务器ip:端口号，集群用逗号分隔
		// acks说明：
		// 0，表示producer不会等待broker的响应，所以，producer无法知道消息是否发送成功，这样有可能会导致数据丢失，但同时，acks值为0会得到最大的系统吞吐量。
		// 此选项提供最低的延迟但最弱的耐久性保证，因为其没有任何确认机制。
		// 1，表示producer会在leader
		// partition收到消息时得到broker的一个确认，这样会有更好的可靠性，因为客户端会等待直到broker确认收到消息。
		// all或-1，producer会在所有备份的partition收到消息时得到broker的确认，这个设置可以得到最高的可靠性保证。
		props_p.put("acks", "1");
		props_p.put("retries", 1);// 重试次数
		props_p.put("batch.size", 16384);
		props_p.put("linger.ms", 2);// 间隔时间
		props_p.put("buffer.memory", 33554432);
		props_p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props_p.put("value.serializer", "journeysafer.framework.core.utils.EncodeingKafka");

		producer = new KafkaProducer<String, Object>(props_p);
	}
	/**
	 * 消费者属性
	 */
	static {
		Properties props_c = new Properties();
		props_c.put("bootstrap.servers", "");// 服务器ip:端口号，集群用逗号分隔
		
		props_c.put("group.id", "test-consumer-group");  
		props_c.put("enable.auto.commit", "true");   
		props_c.put("auto.commit.interval.ms", "1000");  
		props_c.put("session.timeout.ms", "30000");  
	          
	    //要发送自定义对象，需要指定对象的反序列化类  
		props_c.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");  
		props_c.put("value.deserializer", "com.spirit.cdja.util.DecodeingKafka");
		consumer = new KafkaConsumer<String, Object>(props_c); 
	}

	/**
	 * 生产者
	 * 
	 * @param fixedFrequencyData
	 *            数据源
	 * @param kafkaTopic
	 *            （topic）
	 * @throws JsonProcessingException
	 */
	public static boolean sendMsgToKafka(Object baseVO){
		producer.send(new ProducerRecord<String, Object>("kafkaTopic", baseVO),
				new Callback() {

					@SuppressWarnings("deprecation")
					@Override
					public void onCompletion(RecordMetadata metadata, Exception exception) {
						if (metadata != null) {
							logger.info("发送成功：" + "checksum: " + metadata.checksum() + " offset: " + metadata.offset()
									+ " partition: " + metadata.partition() + " topic: " + metadata.topic());
						}
						if (exception != null) {
							logger.info("异常：" + exception.getMessage());
						}
					}
				});
		return true;
	}
	
	
    public static void getMsgFromKafka(){  
        while(true){
        	 ConsumerRecords<String, Object> records = KafkaUtils.getKafkaConsumer().poll(100);  
            if (records.count() > 0) {  
                for (ConsumerRecord<String, Object> record : records) {  
                	Object obj = record.value();
                	logger.info("从kafka接收到的消息是："+obj.toString());
//                	TODO
//                	下面针对得到的object进行业务处理，可以一个线程处理也可以利用线程池并行处理
                }  
            }  
        }  
    } 
    
    public static Consumer<String, Object> getKafkaConsumer() {  
        return consumer;  
    }  

	public static void closeKafkaProducer() {
		producer.close();
	}
	
	public static void closeKafkaConsumer() {
        consumer.close();  
    }  

}