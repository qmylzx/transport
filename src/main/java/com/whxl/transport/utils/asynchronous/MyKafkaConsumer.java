package com.whxl.transport.utils.asynchronous;

import com.jcraft.jsch.JSchException;
import com.whxl.transport.exception.EtlException;
import com.whxl.transport.utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IOUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MyKafkaConsumer implements Runnable {
    private final Log logger = LogFactory.getLog(getClass());
    private String ip;
    private String port;
    private String topic;
    private String mappings;
    private String deviceType;
    private String deviceId;
    private volatile boolean flag = true;
    private String code = "40020";
    private Properties properties;


    public MyKafkaConsumer(String ip, String port, String topic, String mappings, String deviceType, String deviceId) {
        this.ip = ip;
        this.port = port;
        this.topic = topic;
        this.mappings = mappings;
        this.deviceType = deviceType;
        this.deviceId = deviceId;
        properties = new Properties();
        properties.setProperty("bootstrap.servers", ip + ":" + port);
        properties.setProperty("group.id", "kafkaStream");


        properties.setProperty("zookeeper.connect", "115.156.128.241:2181");
        properties.setProperty("zookeeper.connection.timeout.ms", "6000");

        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    }

    @Override
    public void run() {
        logger.debug("尝试建表：");
        try {
            FileUtils.stream(mappings, deviceType, deviceId);
        } catch (EtlException etl) {
            logger.debug("自定义异常!");
            code = etl.getCode();
        } catch (JSchException je) {
            logger.debug("ssh连接服务器失败!");
            code = "40008";
        } catch (IOException ioe) {
            logger.debug("执行remote shell命令错误");
            code = "40009";
        }
        logger.debug("开始接受数据流.topic: " + topic);
        Consumer<String, String> consumer = new KafkaConsumer<>(properties);
        Collection<String> topics = Arrays.asList(topic);    //topic
        // 消费者订阅topic
        consumer.subscribe(topics);
        ConsumerRecords<String, String> consumerRecords;
        String destination = "/result2/" + deviceType + "/data.csv";
        logger.debug("开始任务... ,destination : " + destination);
        StringBuilder sb;
        while (flag) {
            consumerRecords = consumer.poll(1000);
            sb = new StringBuilder();
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                String value = consumerRecord.value();
                sb.append(value);
                sb.append("\n");
            }
            logger.debug("batch data :" + sb.toString());
            InputStream in = new ByteArrayInputStream(sb.toString().getBytes(StandardCharsets.UTF_8));
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://bdai.node1:9000");  //
            try {
                FileSystem fs = FileSystem.get(URI.create(destination), conf);
                OutputStream out = fs.append(new Path(destination));
                IOUtils.copyBytes(in, out, 4096, true);
                logger.debug("this task is done");
            } catch (IOException e) {
                code = "40023";
            }
        }
        logger.debug("stop success !");
    }

    public void stop() {
        logger.debug("stop");
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

    public String getDeviceId() {
        return deviceId;
    }

    @Override
    public String toString() {
        return "ip:" + getIp() + "\nport: " + getPort() + "\ntopic = "
                + getTopic() + "\nmappings = " + getMappings() + "\ndeviceType = "
                +getDeviceType() + "\ncode = " + getCode() + "\ndeviceId = " + getDeviceId();
    }


    //    public static void main(String[] args) {
//        MyKafkaConsumer m = new MyKafkaConsumer("115.156.128.241", "10192",
//                "zaq", "{\n" +
//                "    \"1\": \"3\",\n" +
//                " \"2\": \"2\",\n" +
//                " \"3\": \"1\",\n" +
//                "    \"4\": \"5\",\n" +
//                " \"5\": \"4\",\n" +
//                " \"6\": \"6\"\n" +
//                "}\n", "test");
//        m.run();
//    }
}
