package com.zzx.market_analysis;

import com.zzx.market_analysis.beans.MarketingUserBehavior;
import com.zzx.market_analysis.udf_source.SimulatedMarketingUserBehaviorSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Created with IntelliJ IDEA.
 *
 * @ProjectName: UserBehaviorAnalysis
 * @ClassName: AppMarketingStatistics
 * @author: wb-zcx696752
 * @description: 市场渠道推广总数
 * @data: 2021/4/12 11:36 PM
 */
public class AppMarketingStatistics {
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

        // 2. 开窗统计总量
        SingleOutputStreamOperator<Tuple2<String, Long>> resultStream = dataStream
                .filter(data -> !"UNINSTALL".equals(data.getBehavior())) // 过滤掉卸载数据
                .map(new MapFunction<MarketingUserBehavior, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(MarketingUserBehavior marketingUserBehavior) throws Exception {
                        return new Tuple2<>("total", 1L);
                    }
                })
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new MarketingStatisticsAgg());


        resultStream.print();

        env.execute("APP Marketing Statistics Job");
    }

    // 自定义增量聚合函数
    public static class MarketingStatisticsAgg
            implements AggregateFunction<Tuple2<String, Long>, Tuple2<String, Long>, Tuple2<String, Long>> {

        @Override
        public Tuple2<String, Long> createAccumulator() {
            return new Tuple2<>("", 0L);
        }

        @Override
        public Tuple2<String, Long> add(Tuple2<String, Long> tuple2,
                                        Tuple2<String, Long> acc) {
            acc.f0 = tuple2.f0;
            acc.f1 += tuple2.f1;
            return acc;
        }

        @Override
        public Tuple2<String, Long> getResult(Tuple2<String, Long> acc) {
            return acc;
        }

        @Override
        public Tuple2<String, Long> merge(Tuple2<String, Long> a, Tuple2<String, Long> b) {

            return new Tuple2<>(a.f0,a.f1+b.f1);
        }
    }
}
