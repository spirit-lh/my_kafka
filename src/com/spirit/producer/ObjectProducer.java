package com.spirit.producer;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.spirit.vo.MemberVO; 
public class ObjectProducer {
	public static void main(String[] args) throws Exception{  
        
        String topicName = "test";  
        Properties props = new Properties();  
        props.put("bootstrap.servers", "192.168.1.250:9092");  
        props.put("acks", "all");  
        props.put("retries", 0);  
        props.put("metadata.fetch.timeout.ms", 30000);  
        props.put("batch.size", 16384);  
        props.put("linger.ms", 1);  
        props.put("buffer.memory", 33554432);  
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");  
        props.put("value.serializer", "com.spirit.cdja.util.EncodeingKafka");
//      props.put("partitioner.class", "�̳���Partition���࣬ʵ�ֵ��Ǹ���ָ�����㷨����Ϣ���͵�ָ���ķ�����com.ys.test.SpringBoot.zktest.util.MyPartition");  
          
        Producer<String, Object> producer = new KafkaProducer<String, Object>(props);  
      long startTimes = System.currentTimeMillis();  
        System.out.println();  
          
        for(int i = 0; i < 2; i++){  
              
            final int index = i;  
            MemberVO memberVO = new MemberVO();  
            memberVO.setAge(i);  
            memberVO.setName("�Ŵ�");
              
            List<MemberVO> asList = Arrays.asList(memberVO,memberVO);  
//          producer.send(new ProducerRecord<String, Object>(topicName,Integer.toString(i),asList));  
//          producer.send(new ProducerRecord<String, Object>(topicName, Integer.toString(i), perSon));  
            producer.send(new ProducerRecord<String, Object>(topicName, Integer.toString(i), asList), new Callback() {  
                
              @Override  
              public void onCompletion(RecordMetadata metadata, Exception exception) {  
                  if (metadata != null) {  
                      System.out.println(index+"  ���ͳɹ���"+"checksum: "+metadata.checksum()+" offset: "+metadata.offset()+" partition: "+metadata.partition()+" topic: "+metadata.topic());  
                  }  
                  if (exception != null) {  
                      System.out.println(index+"�쳣��"+exception.getMessage());  
                  }  
              }  
          });  
        }  
        producer.close();  
     }  
}  