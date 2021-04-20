package com.zzx.orderpay_detect;

import com.zzx.orderpay_detect.beans.OrderEvent;
import com.zzx.orderpay_detect.beans.ReceiptEvent;
import com.zzx.orderpay_detect.udf_source.OrderEventSource;
import com.zzx.orderpay_detect.udf_source.ReceiptEventSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.net.URL;

/**
 * Created with IntelliJ IDEA.
 *
 * @ProjectName: UserBehaviorAnalysis
 * @ClassName: TxPayMatchByJoin
 * @author: wb-zcx696752
 * @description:
 * @data: 2021/4/14 10:53 PM
 */
public class TxPayMatchByJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取文件获取订单支付数据
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

        // 读取文件获取支付数据
        URL receipt = TxPayMatch.class.getResource("/ReceiptLog.csv");
        SingleOutputStreamOperator<ReceiptEvent> receiptEventStream = env
                .readTextFile(receipt.getPath())
                .map(line -> {
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

        // 区间连接两条流，得到匹配的数据
        SingleOutputStreamOperator<Tuple2<OrderEvent, ReceiptEvent>> resultStream = orderEventStream
                .keyBy(OrderEvent::getTxId)
                .intervalJoin(receiptEventStream.keyBy(ReceiptEvent::getTxId))
                .between(Time.seconds(-3), Time.seconds(5))
                .process(new TxPayMatchDetectByJoin());

        resultStream.print();

        env.execute("tx pay match detect by join job");

    }

    // 实现自定义
    public static class TxPayMatchDetectByJoin
            extends ProcessJoinFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>> {

        @Override
        public void processElement(OrderEvent pay,
                                   ReceiptEvent receipt,
                                   Context ctx,
                                   Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            out.collect(new Tuple2<>(pay, receipt));
        }
    }
}
