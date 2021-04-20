package com.zzx.market_analysis;

import com.zzx.market_analysis.beans.AdClickEvent;
import com.zzx.market_analysis.udf_source.AdClickEventSource;
import com.zzx.market_analysis.beans.AdCountViewByProvince;
import com.zzx.market_analysis.beans.BlackListUserWarning;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;
import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 *
 * @ProjectName: UserBehaviorAnalysis
 * @ClassName: AdStatisticsByProvince
 * @author: wb-zcx696752
 * @description: 广告点击量统计
 * @data: 2021/4/12 14:59 PM
 */
public class AdStatisticsByProvince {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.从自定义数据源中获取测试数据
        SingleOutputStreamOperator<AdClickEvent> adClickEventStream = env
                .addSource(new AdClickEventSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<AdClickEvent>forMonotonousTimestamps()
                                .withTimestampAssigner((event, l) -> event.getTimestamp())
                );

        OutputTag<BlackListUserWarning> blacklist = new OutputTag<>("blacklist", TypeInformation.of(BlackListUserWarning.class));

        // 2. 对同一个用户点击同一个广告进行检测，如果超出阈值，则加入黑名单并报警
        SingleOutputStreamOperator<AdClickEvent> filterAdClickEventStream = adClickEventStream
                .keyBy(new KeySelector<AdClickEvent, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> getKey(AdClickEvent adClickEvent) throws Exception {
                        return new Tuple2<>(adClickEvent.getUserId(), adClickEvent.getAdId());
                    }
                }) // 基于用户id和广告id进行分组
                .process(new FilterBlackListUser(10));// 同一用户点击同一广告超过10次，加入黑名单
        filterAdClickEventStream.getSideOutput(blacklist).print("blacklist-user");

        // 3. 基于省份分组 开窗聚合
        SingleOutputStreamOperator<AdCountViewByProvince> adCountResultStream = filterAdClickEventStream
                .keyBy(AdClickEvent::getProvince)
                .window(SlidingEventTimeWindows.of(Time.seconds(20), Time.seconds(5))) // 定义滑动窗口，窗口大小20s，滑动步长5s
                .aggregate(new AdCountAgg(), new AdCountResult());

        adCountResultStream.print();

        env.execute("Ad Count By Province Job");
    }

    // 实现自定义处理函数
    public static class FilterBlackListUser
            extends KeyedProcessFunction<Tuple2<Long, Long>, AdClickEvent, AdClickEvent> {

        // 定义属性，点击次数上限
        private Integer clickCap;

        public FilterBlackListUser(Integer clickCap) {
            this.clickCap = clickCap;
        }

        // 定义状态，保存当前用户对于某一广告的点击次数
        ValueState<Long> clickCountState;

        // 定义一个标志状态，保存当前用户是否已经被发送到了黑名单
        ValueState<Boolean> isSendState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Long> click = new ValueStateDescriptor<>("click-state", TypeInformation.of(Long.TYPE));
            ValueStateDescriptor<Boolean> isSend = new ValueStateDescriptor<>("is-send-state", TypeInformation.of(Boolean.TYPE));

            clickCountState = getRuntimeContext().getState(click);
            isSendState = getRuntimeContext().getState(isSend);
        }

        @Override
        public void close() throws Exception {
            clickCountState.clear();
            isSendState.clear();
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdClickEvent> out) throws Exception {
            // 定时器触发，清空所有状态
            clickCountState.clear();
            isSendState.clear();
        }

        @Override
        public void processElement(AdClickEvent value,
                                   Context ctx,
                                   Collector<AdClickEvent> out) throws Exception {

            // 判断点击状态是否为 null 如果为 null ，则初始化状态
            if (clickCountState.value() == null) clickCountState.update(0L);
            // 判断是否发送状态是否为 null 如果为 null ，则初始化状态
            if (isSendState.value() == null) isSendState.update(false);

            Long countState = clickCountState.value();
            Boolean isSend = isSendState.value();

            // 1. 判断是否是当天的第一条数据，如果是，就注册一个第二天0点的定时器
            if (countState == 0) {
                // 获取第二天0点的时间戳
                Long ts = (ctx.timerService().currentProcessingTime() / (24 * 60 * 60 * 1000) + 1) * (24 * 60 * 60 * 1000) - 8 * 60 * 60 * 1000;
//                System.out.println(new Timestamp(ts).toString());
                ctx.timerService().registerProcessingTimeTimer(ts);
            }

            // 判断当前用户对同一广告的点击次数，如果没有超过阈值，就对 clickCountState 加1,并正常输出，如果达到上限，直接过滤掉，并侧输出流输出到黑名单报警
            // 2.判断是否报警
            if (countState > clickCap) {
                // 判断是否输出到黑名单过，如果没有就输出到侧输出流
                if (!isSend) {
                    isSendState.update(true); // 更新状态
                    ctx.output(new OutputTag<>("blacklist", TypeInformation.of(BlackListUserWarning.class)),
                            new BlackListUserWarning(value.getUserId(),
                                    value.getAdId(),
                                    "click over " + clickCap + "times."));
                }
                return; // 不再执行下面操作
            }

            // 如果没有返回，更新点击次数，正常输出当前数据到主流
            clickCountState.update(countState + 1L);
            out.collect(value);
        }
    }

    // 定义增量聚合函数
    public static class AdCountAgg
            implements AggregateFunction<AdClickEvent, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(AdClickEvent adClickEvent, Long acc) {
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

    // 自定义全窗口函数
    public static class AdCountResult
            extends ProcessWindowFunction<Long, AdCountViewByProvince, String, TimeWindow> {

        @Override
        public void process(String province,
                            Context context,
                            Iterable<Long> elements,
                            Collector<AdCountViewByProvince> out) throws Exception {
            Long count = 0L;
            Iterator<Long> iterator = elements.iterator();
            while (iterator.hasNext()) {
                count += iterator.next();
            }
            out.collect(new AdCountViewByProvince(
                    province,
                    new Timestamp(context.window().getEnd()).toString(),
                    count));
        }
    }
}
