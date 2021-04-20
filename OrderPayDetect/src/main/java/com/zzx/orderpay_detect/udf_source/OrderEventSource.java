package com.zzx.orderpay_detect.udf_source;

import com.zzx.orderpay_detect.beans.OrderEvent;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 *
 * @ProjectName: UserBehaviorAnalysis
 * @ClassName: OrderEventSource
 * @author: wb-zcx696752
 * @description:
 * @data: 2021/4/13 15:09 PM
 */
public class OrderEventSource implements SourceFunction<OrderEvent> {

    private Boolean isRuning = true;

    @Override
    public void run(SourceContext<OrderEvent> ctx) throws Exception {
        // 初始化订单类型
        List<String> orderTypes = Arrays.asList("create", "pay");

        while (isRuning) {

            OrderEvent orderEvent = new OrderEvent();
            Long orderId = Long.valueOf(new Random().nextInt(10));
            String orderType = orderTypes.get(new Random().nextInt(orderTypes.size()));
            Long timestamp = System.currentTimeMillis();

            orderEvent.setOrderId(orderId);
            orderEvent.setEventType(orderType);
            orderEvent.setTimestamp(timestamp);
            if ("pay".equals(orderType)) {
                orderEvent.setTxId(getStringRandom(10));
            }
            ctx.collect(orderEvent);
        }
    }

    @Override
    public void cancel() {
        this.isRuning = false;
    }

    public static String getStringRandom(int length) {

        String val = "";
        Random random = new Random();

        //参数length，表示生成几位随机数
        for (int i = 0; i < length; i++) {

            String charOrNum = random.nextInt(2) % 2 == 0 ? "char" : "num";
            //输出字母还是数字
            if ("char".equalsIgnoreCase(charOrNum)) {
                //输出是大写字母还是小写字母
                int temp = random.nextInt(2) % 2 == 0 ? 65 : 97;
                val += (char) (random.nextInt(26) + temp);
            } else if ("num".equalsIgnoreCase(charOrNum)) {
                val += String.valueOf(random.nextInt(10));
            }
        }
        return val;
    }
}
