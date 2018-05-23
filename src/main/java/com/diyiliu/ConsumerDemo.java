package com.diyiliu;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Description: ConsumerDemo
 * Author: DIYILIU
 * Update: 2018-05-23 16:50
 */
public class ConsumerDemo {

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
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test3");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "test3");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // latest 表示消费最新消息,earliest 表示从头开始消费,none表示抛出异常,默认latest
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer(props);
        consumer.subscribe(Arrays.asList(TOPIC));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);

            for (ConsumerRecord<String, String> record : records) {
                System.out.println("Receive from Kafka: [" + record.key() + ", " + record.value() + "]");
            }
        }
    }
}
