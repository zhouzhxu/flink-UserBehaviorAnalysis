package com.zzx.market_analysis;

import com.zzx.market_analysis.beans.ChannelPromotionCount;
import com.zzx.market_analysis.beans.MarketingUserBehavior;
import com.zzx.market_analysis.udf_source.SimulatedMarketingUserBehaviorSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 *
 * @ProjectName: UserBehaviorAnalysis
 * @ClassName: AppMarketingByChannel
 * @author: wb-zcx696752
 * @description: 市场渠道推广分析
 * @data: 2021/4/12 10:36 PM
 */
public class AppMarketingByChannel {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 从自定义数据源中读取数据
        DataStream<MarketingUserBehavior> dataStream = env
                .addSource(new SimulatedMarketingUserBehaviorSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<MarketingUserBehavior>forMonotonousTimestamps()
                                .withTimestampAssigner((event, l) -> event.getTimestamp())
                );

        // 2. 分渠道开窗统计
        SingleOutputStreamOperator<ChannelPromotionCount> resultStream = dataStream
                .filter(data -> !"UNINSTALL".equals(data.getBehavior())) // 过滤掉卸载数据
                .keyBy(new MyKeySelector())
                .window(TumblingEventTimeWindows.of(Time.seconds(10))) // 定义滚动事件时间窗口 10 s
                .aggregate(new MarketingCountAgg(), new WindowMarketingCountResult());

        resultStream.print();

        env.execute("APP Marketing By Channel Job");
    }
    // 自定义 KeySelector
    public static class MyKeySelector implements KeySelector<MarketingUserBehavior,Tuple2<String,String>>{

        @Override
        public Tuple2<String, String> getKey(MarketingUserBehavior marketingUserBehavior) throws Exception {
            return new Tuple2<>(marketingUserBehavior.getChannel(), marketingUserBehavior.getBehavior());
        }
    }

    // 实现自定义增量聚合函数
    public static class MarketingCountAgg
            implements AggregateFunction<MarketingUserBehavior, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(MarketingUserBehavior marketingUserBehavior, Long acc) {
            return acc + 1L;
        }

        @Override
        public Long getResult(Long acc) {
            return acc;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    // 实现自定义全窗口函数
    public static class WindowMarketingCountResult
            extends ProcessWindowFunction<Long, ChannelPromotionCount, Tuple2<String, String>, TimeWindow> {

        @Override
        public void process(Tuple2<String, String> tuple2,
                            Context context,
                            Iterable<Long> elements,
                            Collector<ChannelPromotionCount> out) throws Exception {
            String channel = tuple2.f0;
            String behavior = tuple2.f1;
            String windowEnd = new Timestamp(context.window().getEnd()).toString();
            Long count = 0L;
            Iterator<Long> iterator = elements.iterator();
            while (iterator.hasNext()) {
                count += iterator.next();
            }
            out.collect(new ChannelPromotionCount(
                    channel,
                    behavior,
                    windowEnd,
                    count
            ));
        }
    }
}
