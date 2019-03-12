package com.whxl.transport.utils.asynchronous;

import com.jcraft.jsch.JSchException;
import com.whxl.transport.exception.EtlException;
import com.whxl.transport.utils.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;


public class MyKafkaComsumer implements Runnable {
    private final Log logger = LogFactory.getLog(getClass());
    private String ip;
    private String port;
    private String topic;
    private String mappings;
    private volatile boolean flag = true;
    private String deviceType;
    private String code = "40020";
    private Properties properties;

    public MyKafkaComsumer(String ip, String port, String topic, String mappings, String deviceType) {
        this.ip = ip;
        this.port = port;
        this.topic = topic;
        this.mappings = mappings;
        this.deviceType = deviceType;
        properties = new Properties();
        properties.put("bootstrap.servers", ip + ":" + port);
        properties.put("group.id", "kafkaStream");

        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");//latest,earliest
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        properties.put("zookeeper.connect", "115.156.128.241:2181");
        properties.put("zookeeper.connection.timeout.ms", "6000");

        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    }

    @Override
    public void run() {
        logger.debug("开始stream任务.....");
        ConsumerRecords<String, String> consumerRecords = null;
        Consumer<String, String> consumer = null;
        try {

            FileUtils.stream(mappings, deviceType);
            consumer = new KafkaConsumer<>(properties);
            // 消费者订阅topic
            consumer.subscribe(Arrays.asList(topic));
        } catch (Exception e) {
            if (e instanceof JSchException) {
                logger.debug("ssh连接服务器失败");
                code = "40008";
            }
            if (e instanceof IOException) {
                logger.debug("40009执行remote shell命令错误");
                code = "40009";
            }
            if (e instanceof EtlException) {
                logger.debug("自定义异常!");
                code = ((EtlException) e).getCode();
            }
            flag = false;
        }

        logger.debug("开始接受数据流.");
        while (flag) {
            consumerRecords = consumer.poll(1000);
            // 遍历每一条记录
            for (ConsumerRecord<String, String> record : consumerRecords) {
                String value = record.value();
                logger.debug("value : " + value);
                System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at partition " + record.partition() + " offset " + record.offset());
                // String  to hdfs

            }

        }
        logger.debug("stop success !");
    }

    public static void main(String[] args) {
        MyKafkaComsumer kafkaConsumer = new MyKafkaComsumer("115.156.128.241", "10192", "topic", "sss", "");
        kafkaConsumer.run();
    }


    public void stop() {
        logger.debug("try to stop");
        code = "40024";
        flag = false;
    }


    public String getIp() {
        return ip;
    }

    public String getPort() {
        return port;
    }

    public String getTopic() {
        return topic;
    }

    public String getMappings() {
        return mappings;
    }

    public String getCode() {
        return code;
    }

    public String getDeviceType() {
        return deviceType;
    }

    @Override
    public String toString() {
        return "ip : "+getIp()+", port : "+getPort()+",topic : "+getTopic()+
                ",mappings : "+getMappings()+",code : "+getCode()+",deviceType : "+getDeviceType();
    }

}
