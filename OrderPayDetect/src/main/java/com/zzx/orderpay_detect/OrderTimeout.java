package com.zzx.orderpay_detect;

import com.zzx.orderpay_detect.beans.OrderEvent;
import com.zzx.orderpay_detect.beans.OrderResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;
import java.time.Duration;

/**
 * Created with IntelliJ IDEA.
 *
 * @ProjectName: UserBehaviorAnalysis
 * @ClassName: OrderTimeout
 * @author: wb-zcx696752
 * @description:
 * @data: 2021/4/13 16:05 PM
 */
public class OrderTimeout {
    // 定义超时事件的侧输出流标签
    private final static OutputTag<OrderResult> orderTimeoutTag =
            new OutputTag<>("order-timeout", TypeInformation.of(OrderResult.class));

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取文件数据，并转换POJO类型
        URL resource = OrderTimeout.class.getResource("/OrderLog.csv");
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
                                .<OrderEvent>forBoundedOutOfOrderness(Duration.ofMinutes(15))
                                .withTimestampAssigner((event, l) -> event.getTimestamp())
                );

        // 自定义处理函数，主流输出正常匹配订单事件，侧输出流输出超时报警事件
        SingleOutputStreamOperator<OrderResult> resultStream = orderEventStream
                .keyBy(OrderEvent::getOrderId)
                .process(new OrderPayMatchDetect());

        resultStream.print("payed normally");
        resultStream.getSideOutput(orderTimeoutTag).print("payed timeout");

        env.execute("Order Timeout Detect Job");
    }

    // 实现自定义 KeyedProcessFunction
    public static class OrderPayMatchDetect extends KeyedProcessFunction<Long, OrderEvent, OrderResult> {
        // 定义状态 : 保存之前订单是否已经来过 create pay 的事件
        private ValueState<Boolean> isPayedState;
        private ValueState<Boolean> isCreatedState;

        // 定义状态 : 保存定时器时间戳
        private ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Boolean> isPayed = new ValueStateDescriptor<>("is-payed",
                    TypeInformation.of(Boolean.TYPE));
            ValueStateDescriptor<Boolean> isCreated = new ValueStateDescriptor<>("is-created",
                    TypeInformation.of(Boolean.TYPE));
            ValueStateDescriptor<Long> timerTs = new ValueStateDescriptor<>("timer-ts",
                    TypeInformation.of(Long.TYPE));

            isPayedState = getRuntimeContext().getState(isPayed);
            isCreatedState = getRuntimeContext().getState(isCreated);
            timerTsState = getRuntimeContext().getState(timerTs);

        }

        @Override
        public void close() throws Exception {
            isPayedState.clear();
            isCreatedState.clear();
            timerTsState.clear();
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<OrderResult> out) throws Exception {
            // 定时器触发，说明一定有一个事件没来
            if (isPayedState.value()) {
                // 如果 pay 来了，说明 create 没来
                ctx.output(orderTimeoutTag,
                        new OrderResult(
                                ctx.getCurrentKey(),
                                "payed but not found created log")
                );
            } else {
                // 如果 pay 没来，支付超时
                ctx.output(orderTimeoutTag,
                        new OrderResult(
                                ctx.getCurrentKey(),
                                "payed timeout")
                );
            }

            // 清空状态
            isPayedState.clear();
            isCreatedState.clear();
            timerTsState.clear();
        }

        @Override
        public void processElement(OrderEvent value,
                                   Context ctx,
                                   Collector<OrderResult> out) throws Exception {

            // 判断当前事件类型
            if ("create".equals(value.getEventType())) {
                // 1. 如果来的是 create ，要判断是否支付过
                if (isPayedState.value() != null) {
                    if (isPayedState.value()) {
                        // 1.1 如果已经正常支付，输出正常匹配结果
                        out
                                .collect(new OrderResult(
                                        value.getOrderId(),
                                        "payed successfully")
                                );

                        // 清空状态
                        ctx.timerService().deleteEventTimeTimer(timerTsState.value());
                        isPayedState.clear();
                        isCreatedState.clear();
                        timerTsState.clear();
                    }
                } else {
                    // 1.2 如果没有支付过，注册15分钟后的定时器，开始等待支付事件
                    Long ts = value.getTimestamp() + 15 * 60 * 1000;
                    ctx.timerService().registerEventTimeTimer(ts);

                    // 初始化支付状态
                    isPayedState.update(false);

                    // 更新状态
                    timerTsState.update(ts);
                    isCreatedState.update(true);
                }
            } else if ("pay".equals(value.getEventType())) {
                // 2. 如果来的是 pay ，要判断是否有下单事件来过
                if (isCreatedState.value() != null) {
                    if (isCreatedState.value()) {
                        // 2.1 已经有过下单事件，要继续判断当前支付的时间戳是否超过15分钟
                        if (value.getTimestamp() < timerTsState.value()) {
                            // 2.1.1 在15分钟内，没有超时，正常匹配输出
                            out
                                    .collect(new OrderResult(
                                            value.getOrderId(),
                                            "payed successfully")
                                    );
                        } else {
                            // 2.1.2 已经超时，输出到侧输出流报警
                            ctx
                                    .output(orderTimeoutTag,
                                            new OrderResult(
                                                    value.getOrderId(),
                                                    "payed but already timeout")
                                    );
                        }
                        // 统一清空状态
                        ctx.timerService().deleteEventTimeTimer(timerTsState.value());
                        isPayedState.clear();
                        isCreatedState.clear();
                        timerTsState.clear();
                    }
                } else {
                    // 2.2 没有下单事件，要注册定时器，等待下单事件到来
                    ctx.timerService().registerEventTimeTimer(value.getTimestamp());

                    // 初始化创建状态
                    isCreatedState.update(false);

                    // 更新状态
                    timerTsState.update(value.getTimestamp());
                    isPayedState.update(true);
                }
            }
        }
    }
}
