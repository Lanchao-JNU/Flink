package com.sym.flink;

import com.sym.flink.connector.MysqlWriter;
import com.sym.flink.dto.SteamDTO;
import com.sym.flink.dto.UserBehaviorDTO;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zhanglanchao
 * \* Date: 2021/4/13
 * \* Time: 10:18 上午
 * \* Description:
 * \
 */
public class SteamData {
    public static void main(String[] args) throws Exception {
    //流处理环境

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    URL fileUrl = SteamData.class.getClassLoader().getResource("Steam.csv");
    Path filePath = Path.fromLocalFile(new File(fileUrl.toURI()));
    PojoTypeInfo<SteamDTO> pojoType = (PojoTypeInfo<SteamDTO>) TypeExtractor.createTypeInfo(SteamDTO.class);
    String[] fieldOrder = new String[]{"id", "name", "price", "comment","link", "release_date","os","etl_time"};

    // 创建 PojoCsvInputFormat
    PojoCsvInputFormat<SteamDTO> csvInput = new PojoCsvInputFormat(filePath, pojoType, fieldOrder);
    DataStream<SteamDTO> dataSource = env.createInput(csvInput, pojoType);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<SteamDTO> timedData = dataSource
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SteamDTO>() {
                    @Override
                    public long extractAscendingTimestamp(SteamDTO Steamdto) {
                        return 0;
                    }
                });


    DataStream<SteamDTO> pvData = timedData
            .filter(new FilterFunction<SteamDTO>() {
                @Override
                public boolean filter(SteamDTO steamdto) throws Exception {
                    // 过滤出只有点击的数据
                    return !steamdto.release_date.equals("unknown");
                }
            });

        DataStream<SteamDTO> yeadData = pvData.map(new MapFunction<SteamDTO, SteamDTO>() {
            @Override
            public SteamDTO map(SteamDTO steamDTO) throws Exception {
                steamDTO.release_date = steamDTO.release_date.substring(0,4);
                return steamDTO;
            }
        });




        DataStream<SteamData.ItemViewCount> windowedData = yeadData
            .keyBy("release_date")
            .timeWindow(Time.minutes(60), Time.minutes(60))
            .aggregate(new SteamData.CountAgg(), new SteamData.WindowResultFunction());



    DataStream<Tuple3<String,String,Long>> topItems = windowedData
            .keyBy("windowEnd")
            .process(new SteamData.GroupBydate());
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env,fsSettings);
//
//        topItems.print();
        tEnv.registerDataStream("tableName",topItems,"No,Year,num");
        Table table = tEnv.scan("tableName");
        tEnv.executeSql("select * from tableName") ;
        //写MySQL
        topItems.addSink(new MysqlWriter());

        env.execute("Hot Items Job");
}

    /** 发售年份统计(窗口操作的输出类型) */
    public static class ItemViewCount {
        public String release_date;     // 发售年份
        public long windowEnd;  // 窗口结束时间戳
        public long game;  // 发售游戏数

        public static ItemViewCount of(String release_date, long windowEnd, long game) {
            SteamData.ItemViewCount result = new SteamData.ItemViewCount();
            result.release_date = release_date;
            result.windowEnd = windowEnd;
            result.game = game;
            return result;
        }
    }

    /** COUNT 统计的聚合函数实现，每出现一条记录加一 */
private static class CountAgg implements AggregateFunction<SteamDTO, Long, Long> {

    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(SteamDTO Steamdto, Long acc) {
        return acc + 1;
    }

    @Override
    public Long getResult(Long acc) {
        return acc;
    }

    @Override
    public Long merge(Long acc1, Long acc2) {
        return acc1 + acc2;
    }
}

/** 用于输出窗口的结果 */
public static class WindowResultFunction implements WindowFunction<Long, SteamData.ItemViewCount, Tuple, TimeWindow> {

    @Override
    public void apply(
            Tuple key,  // 窗口的主键，即 itemId
            TimeWindow window,  // 窗口
            Iterable<Long> aggregateResult, // 聚合函数的结果，即 count 值
            Collector<SteamData.ItemViewCount> collector  // 输出类型为 ItemViewCount
    ) throws Exception {
        String itemId = ((Tuple1<String>) key).f0;
        Long count = aggregateResult.iterator().next();
        collector.collect(SteamData.ItemViewCount.of(itemId, window.getEnd(), count));
    }
}


/** 求某个窗口中前 N 名的热门点击商品，key 为窗口时间戳，输出为 TopN 的结果字符串 */
public static class GroupBydate extends KeyedProcessFunction<Tuple, SteamData.ItemViewCount, Tuple3<String,String,Long>> {



    // 用于存储商品与点击数的状态，待收齐同一个窗口的数据后，再触发 TopN 计算
    private ListState<SteamData.ItemViewCount> itemState;

    public GroupBydate() {
    }



    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 状态的注册
        ListStateDescriptor<SteamData.ItemViewCount> itemsStateDesc = new ListStateDescriptor(
                "itemState-state",
                SteamData.ItemViewCount.class);
        itemState = getRuntimeContext().getListState(itemsStateDesc);
    }

    @Override
    public void processElement(
            SteamData.ItemViewCount input,
            Context context,
            Collector<Tuple3<String,String,Long>> collector) throws Exception {

        // 每条数据都保存到状态中
        itemState.add(input);
        // 注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收齐了属于windowEnd窗口的所有商品数据
        context.timerService().registerEventTimeTimer(input.windowEnd + 1);
    }

    @Override
    public void onTimer(
            long timestamp, KeyedProcessFunction<Tuple, SteamData.ItemViewCount, Tuple3<String,String,Long>>.OnTimerContext ctx, Collector<Tuple3<String,String,Long>> out) throws Exception {
        // 获取收到的所有商品点击量
        List<SteamData.ItemViewCount> allItems = new ArrayList();
        for (SteamData.ItemViewCount item : itemState.get()) {
            allItems.add(item);
        }
        // 提前清除状态中的数据，释放空间
        itemState.clear();
        // 按照点击量从大到小排序
        allItems.sort(new Comparator<SteamData.ItemViewCount>() {
            @Override
            public int compare(SteamData.ItemViewCount o1, SteamData.ItemViewCount o2) {
                return (int) (o2.game - o1.game);
            }
        });
        // 将排名信息格式化成 String, 便于打印
        StringBuilder result = new StringBuilder();
        Tuple3<String,String,Long> tuple = new Tuple3();
        ;

//
//        result.append("====================================\n");
//        result.append("时间: ").append(new Timestamp(timestamp-1)).append("\n");
        Integer size = allItems.size();
        for (int i=0;i<size;i++) {
            SteamData.ItemViewCount currentItem = allItems.get(i);
            // No1:  商品ID=12224  浏览量=2413
//            result.append("No").append(i).append(":")
//                    .append("  时间=").append(currentItem.release_date)
//                    .append("  游戏数=").append(currentItem.game)
//                    .append("\n");
//
            tuple.setFields("No"+i,currentItem.release_date,currentItem.game);
            out.collect(tuple);
        }

//        result.append("====================================\n\n");

//        out.collect(result.toString());
    }
}
}