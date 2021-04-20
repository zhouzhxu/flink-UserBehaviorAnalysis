package com.zzx.networkflow_analysis;

import com.zzx.networkflow_analysis.beans.UserBehavior;
import com.zzx.networkflow_analysis.udf_source.UserBehaviorSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * Created with IntelliJ IDEA.
 *
 * @ProjectName: UserBehaviorAnalysis
 * @ClassName: Test
 * @author: wb-zcx696752
 * @description:
 * @data: 2021/4/7 15:16 PM
 */
public class Test {
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

        dataStream
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .aggregate(new AggregateFunction<UserBehavior, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(UserBehavior userBehavior, Long acc) {
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
                })
                .print();

        env.execute();
    }
}
