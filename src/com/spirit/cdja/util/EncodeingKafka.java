package com.spirit.cdja.util;

import java.util.Map;  
import org.apache.kafka.common.serialization.Serializer;  
public class EncodeingKafka implements Serializer<Object> {  
    @Override  
    public void configure(Map configs, boolean isKey) {  
          
    }  
    @Override  
    public byte[] serialize(String topic, Object data) {  
        return BeanUtils.bean2Byte(data);  
    }  
    /* 
     * producer调用close()方法是调用 
     */  
    @Override  
    public void close() {  
        System.out.println("EncodeingKafka is close");  
    }  
}  
