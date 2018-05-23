package com.diyiliu;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Description: ProductDemo
 * Author: DIYILIU
 * Update: 2018-05-23 16:39
 */
public class ProductDemo {

    /**
     * 主题名称
     **/
    private static final String TOPIC = "dyl-test";

    /**
     * Kafka 集群
     **/
    private static final String BROKER_LIST = "dyl-171:9092,dyl-172:9092,dyl-173:9092";


    public static void main(String[] args) {
        Properties props = new Properties();
        // kafka broker 列表
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        // 设置序列化类
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer(props);
        for (int i = 0; i < 1000; i++) {
            String key = i + "";
            String value = "hello" + i + "";

            ProducerRecord<String, String> record = new ProducerRecord(TOPIC, key, value);
            producer.send(record);

            System.out.println("Send to Kafka: [" + key + ", " + value + "]");
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
