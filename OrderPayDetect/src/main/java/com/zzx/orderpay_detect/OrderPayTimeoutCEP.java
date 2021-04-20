package com.zzx.orderpay_detect;

import com.zzx.orderpay_detect.beans.OrderEvent;
import com.zzx.orderpay_detect.beans.OrderResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.net.URL;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 *
 * @ProjectName: UserBehaviorAnalysis
 * @ClassName: OrderPayTimeoutCEP
 * @author: wb-zcx696752
 * @description: 订单支付超时检测
 * @data: 2021/4/13 15:18 PM
 */
public class OrderPayTimeoutCEP {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        /*env
                .addSource(new OrderEventSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<OrderEvent>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((event, l) -> event.getTimestamp())
                )
                .print();*/


        // 读取文件数据，并转换POJO类型
        URL resource = OrderPayTimeoutCEP.class.getResource("/OrderLog.csv");
        SingleOutputStreamOperator<OrderEvent> orderEventStream = env
                .readTextFile(resource.getPath())
                .map(line -> {
                    String[] split = line.split(",");
                    return new OrderEvent(
                            Long.valueOf(split[0]),
                            split[1],
                            split[2],
                            Long.valueOf(split[3])
                    );
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<OrderEvent>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((event, l) -> event.getTimestamp())
                );

        // 1. 定义一个带时间限制的匹配模式
        Pattern<OrderEvent, OrderEvent> orderPayPattern = Pattern
                .<OrderEvent>begin("create")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent orderEvent) throws Exception {
                        return "create".equals(orderEvent.getEventType());
                    }
                })
                .followedBy("pay")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent orderEvent) throws Exception {
                        return "pay".equals(orderEvent.getEventType());
                    }
                })
                .within(Time.minutes(15));

        // 2. 定义侧输出流标签，用来检测订单超时事件
        OutputTag<OrderResult> orderTimeoutTag = new OutputTag<>("order-timeout",
                TypeInformation.of(OrderResult.class));

        // 3. 将 pattern 应用到输出数据流上，得到 pattern stream
        PatternStream<OrderEvent> patternStream = CEP
                .pattern(orderEventStream.keyBy(OrderEvent::getOrderId), orderPayPattern);

        // 4. 调用 select 方法，实现对匹配复杂事件和超时复杂事件的提取和处理
        SingleOutputStreamOperator<OrderResult> resultStream = patternStream
                .select(orderTimeoutTag,
                        new OrderTimeoutSelect(),
                        new OrderPaySelect());

        resultStream.print("payed normally");
        resultStream.getSideOutput(orderTimeoutTag).print("payed timeout");

        env.execute("Order Timeout Detect With CEP Job");
    }

    // 实现自定义超时事件处理函数
    public static class OrderTimeoutSelect implements PatternTimeoutFunction<OrderEvent, OrderResult> {

        @Override
        public OrderResult timeout(Map<String, List<OrderEvent>> map, long timestamp) throws Exception {
            Long timeoutOrderId = map.get("create").iterator().next().getOrderId();
            Long createTimestamp = map.get("create").iterator().next().getTimestamp();
            return new OrderResult(timeoutOrderId,
                    "order create time : " + new Timestamp(createTimestamp) + " | " + " order timeout : " + new Timestamp(timestamp));
        }
    }

    // 实现自定义正常匹配事件处理函数
    public static class OrderPaySelect implements PatternSelectFunction<OrderEvent, OrderResult> {

        @Override
        public OrderResult select(Map<String, List<OrderEvent>> map) throws Exception {
            Long payedOrderId = map.get("pay").iterator().next().getOrderId();
            return new OrderResult(payedOrderId, "payed");
        }
    }
}
