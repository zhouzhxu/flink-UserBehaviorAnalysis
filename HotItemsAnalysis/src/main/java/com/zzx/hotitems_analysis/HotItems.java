package com.zzx.hotitems_analysis;

import com.zzx.hotitems_analysis.beans.ItemViewCount;
import com.zzx.hotitems_analysis.beans.UserBehavior;
import com.zzx.hotitems_analysis.udf_source.UserBehaviorSource;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: wb-zcx696752
 * @description: 实时热门商品统计
 * @data: 2021/4/2 15:37 PM
 */
public class HotItems {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 将并行度设置为1 方便测试
        env.setParallelism(1);

        // 此参数不用设置，flink1.12版本默认是事件时间 EventTime
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 利用自定义数据源向 kafka 写入数据
        DataStreamSink<UserBehavior> outputDataStream = env
                .addSource(new UserBehaviorSource())
                .addSink(new FlinkKafkaProducer<UserBehavior>("localhost:9092",
                        "hotitems",
                        new TypeInformationSerializationSchema(TypeInformation.of(UserBehavior.class),
                                env.getConfig())));

        // 2.读取数据，创建DataStream ,并分配时间戳 和 Watermark
        /*DataStream<UserBehavior> dataStream = env
                .addSource(new UserBehaviorSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<UserBehavior>forMonotonousTimestamps()
                                .withTimestampAssigner((event, l) -> event.getTimestamp())
                );*/

        // kafka 连接配置
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        // 从 kafka 读取数据，创建 DataStream ,并分配时间戳 正常可以设置 forBoundedOutOfOrderness()限制允许乱序的时间
        DataStream<UserBehavior> dataStream = env
                .addSource(new FlinkKafkaConsumer<>("hotitems",
                        new TypeInformationSerializationSchema(TypeInformation.of(UserBehavior.class),
                                env.getConfig()),
                        props))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<UserBehavior>forMonotonousTimestamps()
                                .withTimestampAssigner((event, l) -> event.getTimestamp())); // flink 时区转换时和比原来时间小了8小时，在设置时加上

        // 3.分组开窗聚合，得到每个窗口内各个商品的count值
        DataStream<ItemViewCount> windowAggStream = dataStream
                .filter(userBehavior -> userBehavior.getBehavior().equals("pv")) // 过滤出pv行为
                .keyBy((KeySelector<UserBehavior, Long>) userBehavior -> userBehavior.getItemId()) // 按商品id进行分组
//                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5))) // 开滑动窗口，窗口大小 1hours，5minutes滑动一次
                .window(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(5))) // 开滑动窗口，窗口大小 minutes，5seconds滑动一次
                .aggregate(new ItemCountAgg(), new WindowItemCountResult());

        // 4.收集同一窗口所有商品的count值，排序输出TopN
        DataStream<String> resultStream = windowAggStream
                .keyBy((KeySelector<ItemViewCount, Long>) itemViewCount -> itemViewCount.getWindowEnd()) // 按照窗口进行分组
                .process(new TopNHotItems(5));// 用自定义处理函数，排序取前5

        resultStream.print();

        env.execute("Hot Items Analysis");
    }

    // 实现自定义增量聚合函数
    public static class ItemCountAgg implements AggregateFunction<UserBehavior, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior userBehavior, Long acc) {
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

    // 实现自定义全窗口函数 flink1.12版本推荐使用
    public static class WindowItemCountResult
            extends ProcessWindowFunction<Long, ItemViewCount, Long, TimeWindow> {

        @Override
        public void process(Long itemId,
                            Context context,
                            Iterable<Long> elements,
                            Collector<ItemViewCount> out) throws Exception {
            Iterator<Long> iterator = elements.iterator();
            Long count = 0L;
            while (iterator.hasNext()) {
                count += iterator.next();
            }
            Long windowEnd = context.window().getEnd();
            out.collect(new ItemViewCount(itemId, windowEnd, count));
        }
    }

    // 实现自定义全窗口函数 此函数是旧版的，新版的ProcessWindowFunction
    /*public static class WindowItemCountResult
            implements WindowFunction<Long, ItemViewCount, Long, TimeWindow> {

        @Override
        public void apply(Long itemId,
                          TimeWindow window,
                          Iterable<Long> input,
                          Collector<ItemViewCount> out) throws Exception {
            Iterator<Long> iterator = input.iterator();
            Long count = 0L;
            while (iterator.hasNext()) {
                count += iterator.next();
            }
            Long windowEnd = window.getEnd();
            out.collect(new ItemViewCount(itemId, windowEnd, count));
        }
    }*/

    // 实现自定义 KeyedProcessFunction
    public static class TopNHotItems extends KeyedProcessFunction<Long, ItemViewCount, String> {

        // 定义属性TopN的大小
        private Integer topN = 3;

        public TopNHotItems() {
        }

        public TopNHotItems(Integer topN) {
            this.topN = topN;
        }

        // 定义列表状态,保存当前窗口内所有输出的 ItemViewCount
        private ListState<ItemViewCount> itemViewCountListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ListStateDescriptor<ItemViewCount> descriptor = new ListStateDescriptor<ItemViewCount>("item-view-count", ItemViewCount.class);
            itemViewCountListState = getRuntimeContext().getListState(descriptor);
        }

        @Override
        public void close() throws Exception {
            itemViewCountListState.clear();
        }

        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
            // 每来一条数据，存入状态中
            itemViewCountListState.add(value);

            // 注册定时器
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1000);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发，证明当前已收集到所有数据，对拿到的数据进行排序输出
            List<ItemViewCount> itemViewCounts = Lists.newArrayList(itemViewCountListState.get().iterator());
            itemViewCounts.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return o2.getCount().compareTo(o1.getCount());
                }
            });

            // 将排名信息格式化成 String 方便输出
            StringBuilder result = new StringBuilder();
            result.append("======");
            result.append("窗口结束时间：")
                    .append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                            .format(ctx.getCurrentKey()))
                    .append("======\n");

            // 遍历列表 取 topN 输出
            for (int i = 0; i < Math.min(topN, itemViewCounts.size()); i++) {
                ItemViewCount itemViewCount = itemViewCounts.get(i);
                result
                        .append("NO.")
                        .append(i + 1)
                        .append(":")
                        .append(" 商品ID = ")
                        .append(itemViewCount.getItemId())
                        .append(" 热门度 : ")
                        .append(itemViewCount.getCount())
                        .append("\n");
            }
            result.append("===========================================\n\n");

            out.collect(result.toString());
        }
    }

}
