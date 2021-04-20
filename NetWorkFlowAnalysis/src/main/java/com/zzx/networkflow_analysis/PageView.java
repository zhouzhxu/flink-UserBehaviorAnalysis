package com.zzx.networkflow_analysis;

import com.zzx.networkflow_analysis.beans.PageViewCount;
import com.zzx.networkflow_analysis.beans.UserBehavior;
import com.zzx.networkflow_analysis.udf_source.UserBehaviorSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Iterator;
import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 *
 * @ProjectName: UserBehaviorAnalysis
 * @ClassName: PageView
 * @author: wb-zcx696752
 * @description: PV 页面浏览量，或点击量
 * @data: 2021/4/7 17:35 PM
 */
public class PageView {
    public static void main(String[] args) throws Exception {

        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 将并行度设置为1 方便测试
//        env.setParallelism(1);

        // 使用自定义数据源，创建 DataStream
        DataStream<UserBehavior> dataStream = env
                .addSource(new UserBehaviorSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<UserBehavior>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((event, l) -> event.getTimestamp())
                );


        // 分组开窗聚合，得到pv值
        /*SingleOutputStreamOperator<Tuple2<String, Long>> pvResultStream0 = dataStream
                .filter(userBehavior -> userBehavior.getBehavior().equals("pv"))
                .map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(UserBehavior userBehavior) throws Exception {
                        return new Tuple2<>("pv", 1L);
                    }
                })
                .keyBy((KeySelector<Tuple2<String, Long>, String>) tuple2 -> tuple2.f0)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .sum(1);*/

        // 并行任务改进，随机生成 key ，解决数据倾斜问题
        SingleOutputStreamOperator<PageViewCount> pvStream = dataStream
                .filter(userBehavior -> userBehavior.getBehavior().equals("pv"))
                .map(new MapFunction<UserBehavior, Tuple2<Integer, Long>>() {
                    @Override
                    public Tuple2<Integer, Long> map(UserBehavior userBehavior) throws Exception {
                        Random random = new Random();
                        int nextInt = random.nextInt(12);
                        return new Tuple2<>(nextInt, 1L);
                    }
                })
                .keyBy((KeySelector<Tuple2<Integer, Long>, Integer>) tuple2 -> tuple2.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new PvCountAgg(), new WindowPvCountResult());

        // 将各分区数据汇总起来
        DataStream<PageViewCount> pvResultStream = pvStream
                .keyBy(PageViewCount::getWindowEnd)
//                .sum("count");
                .process(new TotalPvCount());

        pvResultStream.print("pvResultStream");

        env.execute("PV Count Job");
    }

    // 实现自定义增量聚合函数
    public static class PvCountAgg implements AggregateFunction<Tuple2<Integer, Long>, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<Integer, Long> tuple2, Long acc) {
            return acc += tuple2.f1;
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

    // 实现自定义全窗口函数
    public static class WindowPvCountResult extends ProcessWindowFunction<Long, PageViewCount, Integer, TimeWindow> {

        @Override
        public void process(Integer integer, Context context, Iterable<Long> elements, Collector<PageViewCount> out) throws Exception {

            Iterator<Long> iterator = elements.iterator();
            Long count = 0L;
            while (iterator.hasNext()) {
                count += iterator.next();
            }
            long windowEnd = context.window().getEnd();
            out.collect(new PageViewCount(integer.toString(), windowEnd, count));
        }
    }

    // 实现自定义处理函数，把相同窗口分组统计的 count 值相加
    public static class TotalPvCount extends KeyedProcessFunction<Long, PageViewCount, PageViewCount> {

        // 定义状态，保存当前的总 count 值
        private ValueState<Long> countValueState;

        public TotalPvCount() {
            super();
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Long> valueStateDescriptor = new ValueStateDescriptor<>("total-count", TypeInformation.of(Long.class));
            countValueState = getRuntimeContext().getState(valueStateDescriptor);
        }

        @Override
        public void close() throws Exception {
            countValueState.clear();
        }

        @Override
        public void processElement(PageViewCount value, Context ctx, Collector<PageViewCount> out) throws Exception {
            if (countValueState.value() != null) {
                countValueState.update(countValueState.value() + value.getCount());
            } else {
                countValueState.update(value.getCount());
            }
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1000L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<PageViewCount> out) throws Exception {
            // 定时器触发，所有分组的 count 值都到齐，直接输出当前的总 count 数
            Long totalCount = countValueState.value();
            out.collect(new PageViewCount("pv", ctx.getCurrentKey(), totalCount));

            // 清空状态
            countValueState.clear();
        }
    }
}
