package com.sym.flink.util;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zhanglanchao
 * \* Date: 2021/4/20
 * \* Time: 1:05 下午
 * \* Description:
 * \
 */
public class KafkaConsumer {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.enableCheckpointing(5000);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        //设置消费组
        properties.setProperty("group.id", "group_test");

        /**
         * 打开动态分区发现功能
         * 每隔 10ms 会动态获取 Topic 的元数据，对于新增的 Partition 会自动从最早的位点开始消费数据。
         * 防止新增的分区没有被及时发现导致数据丢失，消费者必须要感知 Partition 的动态变化
         */
        properties.setProperty(FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS, "30");

        //动态地发现 Topic，可以指定 Topic 的正则表达式
//        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
//                Pattern.compile("^test_([A-Za-z0-9]*)$"),
//                new SimpleStringSchema(),
//                properties);

        //消费单个 Topic
        //默认的消息的序列化方式为 SimpleStringSchema 的时候，返回的结果中只有原数据，没有 topic、parition 等信息
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "steam_data",
                new SimpleStringSchema(),
                properties);

        //消费多个 Topic
//        List<String> topics = new LinkedList<>();
//        topics.add("test_A");
//        topics.add("test_B");
//        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
//                topics,
//                new SimpleStringSchema(),
//                properties);


        //设置从最早的offset消费
        consumer.setStartFromEarliest();
        DataStream<String> stream = env.addSource(consumer);
        stream.print();



        env.execute("start consumer...");
    }
}