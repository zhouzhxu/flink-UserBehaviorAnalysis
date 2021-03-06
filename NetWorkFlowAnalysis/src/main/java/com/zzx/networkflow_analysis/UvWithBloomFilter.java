package com.zzx.networkflow_analysis;

import com.zzx.networkflow_analysis.beans.PageViewCount;
import com.zzx.networkflow_analysis.beans.UserBehavior;
import com.zzx.networkflow_analysis.udf_source.UserBehaviorSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.time.Duration;

/**
 * Created with IntelliJ IDEA.
 *
 * @ProjectName: UserBehaviorAnalysis
 * @ClassName: UvWithBloomFilter
 * @author: wb-zcx696752
 * @description:
 * @data: 2021/4/9 17:22 PM
 */
public class UvWithBloomFilter {
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
                .trigger(new MyTrigger())
                .process(new WindowUvCountResultWithBloomFilter())
                .print();

        env.execute("UV Count With Bloom Filter Job");
    }

    // 自定义触发器
    public static class MyTrigger extends Trigger<UserBehavior, TimeWindow> {

        @Override
        public TriggerResult onElement(UserBehavior element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            // 每一条数据来到，直接触发窗口计算，并直接清空窗口
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

        }
    }

    // 自定义布隆过滤器
    public static class MyBloomFilter {
        // 定义位图的大小，一般需要定义为2的整次幂
        private Integer bitmap;

        public MyBloomFilter(Integer bitmap) {
            this.bitmap = bitmap;
        }

        // 实现一个哈希函数
        public Long hashCode(String value, Integer seed) {
            Long result = 0L;
            for (int i = 0; i < value.length(); i++) {
                result = result * seed + value.charAt(i);
            }
            return result & (bitmap - 1);
        }

    }

    // 自定义全窗口处理函数
    public static class WindowUvCountResultWithBloomFilter
            extends ProcessAllWindowFunction<UserBehavior, PageViewCount, TimeWindow> {
        // 定义 jedis 连接和布隆过滤器
        Jedis jedis;
        MyBloomFilter myBloomFilter;

        @Override
        public void open(Configuration parameters) throws Exception {
            jedis = new Jedis("localhost", 6379);
            myBloomFilter = new MyBloomFilter(1 << 29); // 要处理一亿个数据，用64MB大小的位图
        }

        @Override
        public void close() throws Exception {
            jedis.close();
        }

        @Override
        public void process(Context context,
                            Iterable<UserBehavior> elements,
                            Collector<PageViewCount> out) throws Exception {
            // 将位图和窗口 count 值全部存入 redis ，用 windowEnd 作为 key
            Long windowEnd = context.window().getEnd();
            String bitmapKey = windowEnd.toString();
            // 把 count 值存成一张 hash 表
            String countHashName = "uv_count";
            String countKey = windowEnd.toString();

            // 1. 取当前的 userId
            Long userId = elements.iterator().next().getUserId();

            // 2. 计算位图中的 offset
            Long offset = myBloomFilter.hashCode(userId.toString(), 61);

            // 3. 用 redis 的 getbit 命令，判断对应位置的值
            Boolean isExist = jedis.getbit(bitmapKey, offset);
            // 如果不存在，位图对应位置置为1
            if (!isExist) {
                jedis.setbit(bitmapKey, offset, true);
                // 更新 redis 中保存的 count 值
                Long uvCount = 0L; // 初始 count 值
                String uvCountString = jedis.hget(countHashName, countKey);
                if (uvCountString != null && !"".equals(uvCountString)) uvCount = Long.valueOf(uvCountString);
                jedis.hset(countHashName, countKey, String.valueOf(uvCount + 1));
            }
        }
    }
}
