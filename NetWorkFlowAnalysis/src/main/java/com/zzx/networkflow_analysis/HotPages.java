package com.zzx.networkflow_analysis;

import com.zzx.networkflow_analysis.beans.ApacheLogEvent;
import com.zzx.networkflow_analysis.udf_source.ApacheLogEventSource;
import com.zzx.networkflow_analysis.beans.PageViewCount;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 *
 * @ProjectName: UserBehaviorAnalysis
 * @ClassName: HotPages
 * @author: wb-zcx696752
 * @description: 热门热面
 * @data: 2021/4/7 11:24 PM
 */
public class HotPages {
    public static void main(String[] args) throws Exception {

        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 获取数据
        DataStream<ApacheLogEvent> dataStream = env
                .addSource(new ApacheLogEventSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<ApacheLogEvent>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                .withTimestampAssigner((event, l) -> event.getTimestamp())
                );

        // 定义侧输出流标签
        OutputTag<ApacheLogEvent> lateTag = new OutputTag<ApacheLogEvent>("late") {
        };

        // 分组开窗聚合
        SingleOutputStreamOperator<PageViewCount> windowAggStream = dataStream
                .filter(apacheLogEvent -> apacheLogEvent.getMethod().equals("GET")) // 过滤出 GET 请求
                .keyBy(ApacheLogEvent::getUrl) // 利用方法引用，根据请求的 url 进行分组
//                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5))) // 开滑动窗口，窗口大小 1hours，5minutes滑动一次
                .window(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(5))) // 开滑动窗口，窗口大小 minutes，5seconds滑动一次
                .allowedLateness(Time.minutes(1)) // 设置窗口的延迟关闭时间
                .sideOutputLateData(lateTag)
                .aggregate(new PagesCountAgg(), new WindowPagesCountResult());

        windowAggStream.getSideOutput(lateTag).print(); // 获取打印侧输出流

        // 收集同一窗口 count ，排序输出
        DataStream<String> resultStream = windowAggStream
                .keyBy(PageViewCount::getWindowEnd)
                .process(new TopNHotPages());

        resultStream.print();

        env.execute("Hot Pages Analysis");
    }

    // 实现自定义增量聚合函数
    public static class PagesCountAgg implements AggregateFunction<ApacheLogEvent, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLogEvent apacheLogEvent, Long acc) {
            return acc + 1L;
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
    public static class WindowPagesCountResult extends ProcessWindowFunction<Long, PageViewCount, String, TimeWindow> {

        @Override
        public void process(String url,
                            Context context,
                            Iterable<Long> elements,
                            Collector<PageViewCount> out) throws Exception {
            Iterator<Long> iterator = elements.iterator();
            Long count = 0L;
            while (iterator.hasNext()) {
                count += iterator.next();
            }
            long windowEnd = context.window().getEnd();
            out.collect(new PageViewCount(url, windowEnd, count));
        }
    }

    // 实现自定义 KeyedProcessFunction
    public static class TopNHotPages extends KeyedProcessFunction<Long, PageViewCount, String> {

        // 定义属性TopN的大小
        private Integer topN = 3;

        public TopNHotPages() {
        }

        public TopNHotPages(Integer topN) {
            this.topN = topN;
        }

        // 定义map 状态，保存窗口内所有输出的 PageViewCount ，为了迟到数据可以更新
        private MapState<String, Long> pageViewCountMapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<String, Long> mapStateDescriptor = new MapStateDescriptor<>("page-view-count",
                    TypeInformation
                            .of(String.class),
                    TypeInformation
                            .of(Long.class));

            pageViewCountMapState = getRuntimeContext().getMapState(mapStateDescriptor);
        }

        @Override
        public void close() throws Exception {
            pageViewCountMapState.clear();
        }

        @Override
        public void processElement(PageViewCount value, Context ctx, Collector<String> out) throws Exception {
            // 每来一条数据就存入状态中
            pageViewCountMapState.put(value.getUrl(), value.getCount());

            // 注册定时器
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1000);

            // 注册清理数据的定时器
//            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 60 * 1000L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 判断是否到了窗口关闭清理时间，如果是，直接清空状态返回
            /*if (timestamp == ctx.getCurrentKey() + 60 * 1000L) {
                pageViewCountMapState.clear();
                return;
            }*/

            // 定时器触发，证明当前已收集到所有数据，对拿到的数据进行排序输出
            List<Map.Entry<String, Long>> pageViewCounts = Lists.newArrayList(pageViewCountMapState.entries().iterator());
            pageViewCounts
                    .sort(new Comparator<Map.Entry<String, Long>>() {
                        @Override
                        public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                            return o2.getValue().compareTo(o1.getValue());
                        }
                    });
            // 将排名信息格式化成 String 方便输出
            StringBuilder result = new StringBuilder();
            result.append("======");
            result.append("窗口结束时间：")
                    .append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                            .format(ctx.getCurrentKey()))
                    .append("======\n");

//            System.out.println(new Timestamp(ctx.getCurrentKey()));

            // 遍历列表 取 topN 输出
            for (int i = 0; i < Math.min(topN, pageViewCounts.size()); i++) {
                Map.Entry<String, Long> pageViewCount = pageViewCounts.get(i);
                result
                        .append("NO.")
                        .append(i + 1)
                        .append(":")
                        .append(" 页面url : ")
                        .append(pageViewCount.getKey())
                        .append(" 浏览量 : ")
                        .append(pageViewCount.getValue())
                        .append("\n");
            }
            result.append("===========================================\n\n");

            out.collect(result.toString());
        }
    }
}
