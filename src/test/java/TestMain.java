import kafka.admin.AdminUtils;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.utils.ZkUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;
import scala.collection.Seq;
import scala.collection.convert.Wrappers;

import java.util.*;

/**
 * Description: TestMain
 * Author: DIYILIU
 * Update: 2018-05-23 14:06
 */
public class TestMain {

    /**  主题名称 **/
    private static final String TOPIC = "dyl-test";
    /**  Kafka 集群 **/
    private static final String BROKER_LIST = "dyl-171:9092,dyl-172:9092,dyl-173:9092";


    /**  Zookeeper 集群 **/
    private static final String ZK_CONNECT = "dyl-171:2181,dyl-172:2181,dyl-173:2181";

    /**  Session 过期时间 **/
    private static final int SESSION_TIMEOUT = 30000;

    /**  连接超时时间 **/
    private static final int CONNECT_TIMEOUT = 30000;


    private static KafkaProducer<String, String> producer = null;

    static {
        // kafkaProducer 配置信息
        Properties configs = initConfig();
        // 初始化
        producer = new KafkaProducer(configs);
    }

    private static Properties initConfig(){
        Properties properties = new Properties();

        // kafka broker 列表
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);

        // 设置序列化类
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }


    @Test
    public void test(){
        Date key  = new Date();
        String value = "123";

        ProducerRecord<String, String> record = new ProducerRecord(TOPIC, null, key, value);
        producer.send(record);
    }

    /**
     * 老版本的消费者
     */
    @Test
    public void test1(){
        String topic = "dyl-test";

        Properties props = new Properties();
        props.put("zookeeper.connect", ZK_CONNECT);
        // 指定消费组名
        props.put("group.id", "group2");
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        // 设置消费位置
        props.put("auto.offset.reset", "smallest");

        ConsumerConfig consumerConfig = new ConsumerConfig(props);
        ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(consumerConfig);

        Map<String, Integer> topicCountMap = new HashMap();
        topicCountMap.put(topic, new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            System.out.println("Received message: " + new String(it.next().message()));
        }
    }

    /**
     * 创建主题
     *
     */
    @Test
    public void test3(){
        String topic = "dyl-test";
        int partition = 3;
        int replica = 3;

        ZkUtils zkUtils = null;
        try {
            zkUtils = ZkUtils.apply(ZK_CONNECT, SESSION_TIMEOUT, CONNECT_TIMEOUT, JaasUtils.isZkSecurityEnabled());

            if (!AdminUtils.topicExists(zkUtils, topic)){

                AdminUtils.createTopic(zkUtils, topic, partition, replica, AdminUtils.createTopic$default$5(), AdminUtils.createTopic$default$6());

                System.out.println(topic +  " 创建成功!");
            }else {

                System.out.println("topic 已存在!");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (zkUtils != null){

                zkUtils.close();
            }
        }
    }

    /**
     * 查询所有topic
     */
    @Test
    public void test4() {
        ZkUtils zkUtils = null;
        try {
            zkUtils = ZkUtils.apply(ZK_CONNECT, SESSION_TIMEOUT, CONNECT_TIMEOUT, JaasUtils.isZkSecurityEnabled());
            Seq<String> topics =  zkUtils.getAllTopics();
            Wrappers.JListWrapper listWrapper = (Wrappers.JListWrapper) topics;
            List<String> list = listWrapper.underlying();

            for (String topic : list) {
                System.out.println(topic);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (zkUtils != null){

                zkUtils.close();
            }
        }
    }
}
