package com.zzx.networkflow_analysis;

import com.zzx.networkflow_analysis.beans.PageViewCount;
import com.zzx.networkflow_analysis.beans.UserBehavior;
import com.zzx.networkflow_analysis.udf_source.UserBehaviorSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashSet;

/**
 * Created with IntelliJ IDEA.
 *
 * @ProjectName: UserBehaviorAnalysis
 * @ClassName: UniqueVisitor
 * @author: wb-zcx696752
 * @description: UV 访问某个站点或点击某条新闻的不同IP地址的人数
 * @data: 2021/4/8 17:08 PM
 */
public class UniqueVisitor {
    public static void main(String[] args) throws Exception {

        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 将并行度设置为1 方便测试
        env.setParallelism(1);

        // 使用自定义数据源，创建 DataStream
        DataStream<UserBehavior> dataStream = env
                .addSource(new UserBehaviorSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<UserBehavior>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((event, l) -> event.getTimestamp())
                );

        // 开窗统计 UV 值
        dataStream
                .filter(userBehavior -> userBehavior.getBehavior().equals("pv"))
                .windowAll(TumblingEventTimeWindows.of(Time.minutes(1)))
                .apply(new WindowUvCountResult())
                .print();

        env.execute("UV Count Job");
    }

    // 实现自定义全窗口函数
    public static class WindowUvCountResult implements AllWindowFunction<UserBehavior, PageViewCount, TimeWindow> {

        @Override
        public void apply(TimeWindow window, Iterable<UserBehavior> values, Collector<PageViewCount> out) throws Exception {
            // 定义一个 Set 结构，保存窗口中所有的 userId ，自动去重
            HashSet<Long> uidSet = new HashSet<>();
            for (UserBehavior userBehavior : values) {
                uidSet.add(userBehavior.getUserId());
            }
            out.collect(new PageViewCount("uv", window.getEnd(), Long.valueOf(uidSet.size())));
        }
    }
}
