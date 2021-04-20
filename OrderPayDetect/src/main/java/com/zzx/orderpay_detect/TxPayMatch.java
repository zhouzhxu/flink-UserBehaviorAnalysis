package com.zzx.orderpay_detect;

import com.zzx.orderpay_detect.beans.OrderEvent;
import com.zzx.orderpay_detect.beans.ReceiptEvent;
import com.zzx.orderpay_detect.udf_source.OrderEventSource;
import com.zzx.orderpay_detect.udf_source.ReceiptEventSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;

/**
 * Created with IntelliJ IDEA.
 *
 * @ProjectName: UserBehaviorAnalysis
 * @ClassName: TxPayMatch
 * @author: wb-zcx696752
 * @description:
 * @data: 2021/4/13 18:31 PM
 */
public class TxPayMatch {

    // 定义侧输出流标签
    private final static OutputTag<OrderEvent> unmatchedPays = new OutputTag<>("unmatched-pays", TypeInformation.of(OrderEvent.class));
    private final static OutputTag<ReceiptEvent> unmatchedReceipts = new OutputTag<>("unmatched-receipts", TypeInformation.of(ReceiptEvent.class));

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 使用自定义数据源获取订单支付数据
        SingleOutputStreamOperator<OrderEvent> orderEventStream0 = env
                .addSource(new OrderEventSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<OrderEvent>forMonotonousTimestamps()
                                .withTimestampAssigner((event, l) -> event.getTimestamp())
                )
                .filter(data -> "pay".equals(data.getEventType()))
                .filter(data -> !"".equals(data.getTxId()) && data.getTxId() != null); // 交易id不为空，必须是pay事件

        URL order = TxPayMatch.class.getResource("/OrderLog.csv");
        SingleOutputStreamOperator<OrderEvent> orderEventStream = env
                .readTextFile(order.getPath())
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
                                .<OrderEvent>forMonotonousTimestamps()
                                .withTimestampAssigner((event, l) -> event.getTimestamp())
                )
                .filter(data -> "pay".equals(data.getEventType()))
                .filter(data -> !"".equals(data.getTxId()) && data.getTxId() != null);

        // 使用自定义数据源获取支付数据
        SingleOutputStreamOperator<ReceiptEvent> receiptEventStream0 = env
                .addSource(new ReceiptEventSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<ReceiptEvent>forMonotonousTimestamps()
                                .withTimestampAssigner((event, l) -> event.getTimestamp())
                );

        URL receipt = TxPayMatch.class.getResource("/ReceiptLog.csv");
        SingleOutputStreamOperator<ReceiptEvent> receiptEventStream = env
                .readTextFile(receipt.getPath())
                .map(line->{
                    String[] split = line.split(",");
                    return new ReceiptEvent(
                            split[0],
                            split[1],
                            Long.valueOf(split[2])
                    );
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<ReceiptEvent>forMonotonousTimestamps()
                                .withTimestampAssigner((event, l) -> event.getTimestamp())
                );

        // 将两条流进行连接合并，进行匹配处理,不匹配的时间输出到侧输出流
        SingleOutputStreamOperator<Tuple2<OrderEvent, ReceiptEvent>> resultStream = orderEventStream
                .connect(receiptEventStream)
                .keyBy(OrderEvent::getTxId, ReceiptEvent::getTxId)
                .process(new TxPayMatchDetect());

        resultStream.print("matched-pays");
        resultStream.getSideOutput(unmatchedPays).print("unmatched-pays");
        resultStream.getSideOutput(unmatchedReceipts).print("unmatched-receipts");

        env.execute("tx pay match detect job");

    }

    // 实现自定义 CoProcessFunction
    public static class TxPayMatchDetect
            extends CoProcessFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>> {

        // 定义状态，保存当前已经来到的订单支付事件和到账事件
        private ValueState<OrderEvent> payState;
        private ValueState<ReceiptEvent> receiptState;
        private ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<OrderEvent> payDesc = new ValueStateDescriptor<>("pay", TypeInformation.of(OrderEvent.class));
            ValueStateDescriptor<ReceiptEvent> receiptDesc = new ValueStateDescriptor<>("receipt", TypeInformation.of(ReceiptEvent.class));
            ValueStateDescriptor<Long> timerTs = new ValueStateDescriptor<>("timer-ts", TypeInformation.of(Long.TYPE));
            payState = getRuntimeContext().getState(payDesc);
            receiptState = getRuntimeContext().getState(receiptDesc);
            timerTsState = getRuntimeContext().getState(timerTs);
        }

        @Override
        public void close() throws Exception {
            payState.clear();
            receiptState.clear();
            timerTsState.clear();
        }

        @Override
        public void processElement1(OrderEvent pay, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            // 订单支付事件来了，判断是否已经有对应的到账事件
            if (receiptState.value() != null) {
                // 如果 receiptState 不为空，说明到账事件已经来过，输出匹配事件，清空状态
                out.collect(new Tuple2<>(pay, receiptState.value()));

                // 清空状态
                ctx.timerService().deleteEventTimeTimer(timerTsState.value());
                payState.clear();
                receiptState.clear();
                timerTsState.clear();
            } else {
                // 如果 receiptState 为空，注册一个定时器，开始等待  等待5s触发定时器
                ctx.timerService().registerEventTimeTimer(pay.getTimestamp() + 5000);

                // 更新状态
                timerTsState.update(pay.getTimestamp() + 5000);
                payState.update(pay);
            }
        }

        @Override
        public void processElement2(ReceiptEvent receipt, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            // 到账事件来了，判断是否已经有对应的支付事件
            if (payState.value() != null) {
                // 如果 payState 不为空，说明支付事件已经来过，输出匹配事件，清空状态
                out.collect(new Tuple2<>(payState.value(), receipt));

                // 清空状态
                ctx.timerService().deleteEventTimeTimer(timerTsState.value());
                payState.clear();
                receiptState.clear();
                timerTsState.clear();
            } else {
                // 如果 payState 为空，注册一个定时器，开始等待  等待3s触发定时器
                ctx.timerService().registerEventTimeTimer(receipt.getTimestamp() + 3000);

                // 更新状态
                timerTsState.update(receipt.getTimestamp() + 3000);
                receiptState.update(receipt);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            // 定时器触发的时候，有可能是有一个事件没来，不匹配
            if (payState.value() != null) {
                ctx.output(unmatchedPays, payState.value());
            }
            if (receiptState.value() != null) {
                ctx.output(unmatchedReceipts, receiptState.value());
            }

            // 清空状态
            payState.clear();
            receiptState.clear();
            timerTsState.clear();
        }
    }
}
