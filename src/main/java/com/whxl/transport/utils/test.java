package com.whxl.transport.utils;


import com.alibaba.fastjson.JSON;
import com.whxl.transport.utils.asynchronous.FileLazyTask;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;


import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class test {


    public static void main(String[] args) {

//        Properties props = new Properties();
//        props.put("bootstrap.servers", "115.156.128.241:10192");
//        props.put("acks", "all");
//        props.put("retries", 0);
//        props.put("batch.size", 16384);
//        props.put("linger.ms", 1);
//        props.put("buffer.memory", 33554432);
//        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        Producer<String, String> producer = new KafkaProducer<String, String>(props);
//        int i = 0;
//        while (true) {
//
//            try {
//                Thread.sleep(1000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            producer.send(new ProducerRecord<String, String>("first", String.valueOf(i), "ttttttttttttttttttttttt"));
//            System.out.println(i);
//            ++i;
//        }

    }
}
