package com.sym.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.scala.DataStream;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zhanglanchao
 * \* Date: 2021/4/8
 * \* Time: 11:16 上午
 * \* Description:
 * \
 */
public class WordCountStream {

        public static void main(String[] args) throws Exception {
            //创建环境
            org.apache.flink.streaming.api.environment.StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            //用parameter tool工具从程序启动参数中提取配置项
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            String host = parameterTool.get("host");
            Integer port = parameterTool.getInt("port");

            DataStreamSource<String> source = env.socketTextStream(host, port);
            SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = source.flatMap(new MyFlatMapper())
                    .keyBy(0).sum(1);
            //打印输出
            resultStream.print();
            //执行
            env.execute();


        }

    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] words = s.split(" ");
            for (String word : words) {
                collector.collect(new Tuple2(word, 1));
            }
        }
    }


}