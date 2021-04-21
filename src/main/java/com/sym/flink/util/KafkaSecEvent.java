package com.sym.flink.util;


import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.TimeCharacteristic;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Properties;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zhanglanchao
 * \* Date: 2021/4/21
 * \* Time: 1:19 下午
 * \* Description:
 * \
 */
public class KafkaSecEvent {

    public static void main(String[] args) throws Exception {


            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            // 连接kafka数据流
            Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", "localhost:9092");
            FlinkKafkaConsumer<SecEvent> consumer = new FlinkKafkaConsumer<>("steam_data", new SecEventJSONSerializer(), properties);
            //从最早开始消费
            //consumer.setStartFromEarliest();
            DataStream<SecEvent> stream = env.addSource(consumer);
            stream.process(new ProcessFunction<SecEvent, String>() {
                @Override
                public void processElement(SecEvent event, Context ctx, Collector<String> out) throws Exception {
                    out.collect(event.toString());
                    System.out.println(event.toString());
                }
            }).print();

            env.execute();



    }

    public static class SecEvent {

        public String eventType;
        public String imageName;
        public long timestamp;
        public String instanceId;

        public SecEvent() {
        }

        public SecEvent(String eventType, String imageName, long timestamp, String instanceId) {
            this.eventType = eventType;
            this.imageName = imageName;
            this.timestamp = timestamp;
            this.instanceId = instanceId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            } else if (o != null && getClass() == o.getClass()) {
                SecEvent that = (SecEvent) o;

                return timestamp == that.timestamp &&
                        eventType.equals(that.eventType) &&
                        imageName.equals(that.imageName) &&
                        instanceId.equals(that.instanceId);
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            int result = eventType != null ? eventType.hashCode() : 0;
            result = 31 * result + (imageName != null ? imageName.hashCode() : 0);
            result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
            result = 31 * result + (instanceId != null ? instanceId.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            String formattedTimestamp = format.format(timestamp);

            return "{\n" +
                    "  \"eventType\": \"" + eventType + "\"\n," +
                    "  \"imageName\": \"" + imageName + "\"\n," +
                    "  \"timestamp\": \"" + formattedTimestamp + "\"\n," +
                    "  \"instanceId\": \"" + instanceId + "\"\n" +
                    "}";
        }
    }

    //序列化
    public static class SecEventJSONSerializer
            implements SerializationSchema<SecEvent>, DeserializationSchema<SecEvent> {

        private final ObjectMapper mapper = new ObjectMapper();

        @Override
        public byte[] serialize(KafkaSecEvent.SecEvent secEvent) {
            try {
                return mapper.writeValueAsBytes(secEvent);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public SecEvent deserialize(byte[] bytes) throws IOException {
            System.out.println("hello");
            return mapper.readValue(bytes, SecEvent.class);
        }

        @Override
        public boolean isEndOfStream(SecEvent secEvent) {
            return false;
        }

        @Override
        public TypeInformation<SecEvent> getProducedType() {
            return TypeExtractor.getForClass(SecEvent.class);
        }
    }
}