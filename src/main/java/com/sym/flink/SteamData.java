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
 * \* Time: 10:18 ??????
 * \* Description:
 * \
 */
public class SteamData {
    public static void main(String[] args) throws Exception {
    //???????????????

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    URL fileUrl = SteamData.class.getClassLoader().getResource("Steam.csv");
    Path filePath = Path.fromLocalFile(new File(fileUrl.toURI()));
    PojoTypeInfo<SteamDTO> pojoType = (PojoTypeInfo<SteamDTO>) TypeExtractor.createTypeInfo(SteamDTO.class);
    String[] fieldOrder = new String[]{"id", "name", "price", "comment","link", "release_date","os","etl_time"};

    // ?????? PojoCsvInputFormat
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
                    // ??????????????????????????????
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
        //???MySQL
        topItems.addSink(new MysqlWriter());

        env.execute("Hot Items Job");
}

    /** ??????????????????(???????????????????????????) */
    public static class ItemViewCount {
        public String release_date;     // ????????????
        public long windowEnd;  // ?????????????????????
        public long game;  // ???????????????

        public static ItemViewCount of(String release_date, long windowEnd, long game) {
            SteamData.ItemViewCount result = new SteamData.ItemViewCount();
            result.release_date = release_date;
            result.windowEnd = windowEnd;
            result.game = game;
            return result;
        }
    }

    /** COUNT ????????????????????????????????????????????????????????? */
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

/** ??????????????????????????? */
public static class WindowResultFunction implements WindowFunction<Long, SteamData.ItemViewCount, Tuple, TimeWindow> {

    @Override
    public void apply(
            Tuple key,  // ????????????????????? itemId
            TimeWindow window,  // ??????
            Iterable<Long> aggregateResult, // ??????????????????????????? count ???
            Collector<SteamData.ItemViewCount> collector  // ??????????????? ItemViewCount
    ) throws Exception {
        String itemId = ((Tuple1<String>) key).f0;
        Long count = aggregateResult.iterator().next();
        collector.collect(SteamData.ItemViewCount.of(itemId, window.getEnd(), count));
    }
}


/** ????????????????????? N ???????????????????????????key ?????????????????????????????? TopN ?????????????????? */
public static class GroupBydate extends KeyedProcessFunction<Tuple, SteamData.ItemViewCount, Tuple3<String,String,Long>> {



    // ?????????????????????????????????????????????????????????????????????????????????????????? TopN ??????
    private ListState<SteamData.ItemViewCount> itemState;

    public GroupBydate() {
    }



    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // ???????????????
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

        // ?????????????????????????????????
        itemState.add(input);
        // ?????? windowEnd+1 ??? EventTime Timer, ????????????????????????????????????windowEnd???????????????????????????
        context.timerService().registerEventTimeTimer(input.windowEnd + 1);
    }

    @Override
    public void onTimer(
            long timestamp, KeyedProcessFunction<Tuple, SteamData.ItemViewCount, Tuple3<String,String,Long>>.OnTimerContext ctx, Collector<Tuple3<String,String,Long>> out) throws Exception {
        // ????????????????????????????????????
        List<SteamData.ItemViewCount> allItems = new ArrayList();
        for (SteamData.ItemViewCount item : itemState.get()) {
            allItems.add(item);
        }
        // ?????????????????????????????????????????????
        itemState.clear();
        // ?????????????????????????????????
        allItems.sort(new Comparator<SteamData.ItemViewCount>() {
            @Override
            public int compare(SteamData.ItemViewCount o1, SteamData.ItemViewCount o2) {
                return (int) (o2.game - o1.game);
            }
        });
        // ??????????????????????????? String, ????????????
        StringBuilder result = new StringBuilder();
        Tuple3<String,String,Long> tuple = new Tuple3();
        ;

//
//        result.append("====================================\n");
//        result.append("??????: ").append(new Timestamp(timestamp-1)).append("\n");
        Integer size = allItems.size();
        for (int i=0;i<size;i++) {
            SteamData.ItemViewCount currentItem = allItems.get(i);
            // No1:  ??????ID=12224  ?????????=2413
//            result.append("No").append(i).append(":")
//                    .append("  ??????=").append(currentItem.release_date)
//                    .append("  ?????????=").append(currentItem.game)
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