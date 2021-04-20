package com.zzx.orderpay_detect.udf_source;

import com.zzx.orderpay_detect.beans.ReceiptEvent;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 *
 * @ProjectName: UserBehaviorAnalysis
 * @ClassName: ReceiptEventSource
 * @author: wb-zcx696752
 * @description: 支付事件自定义数据源
 * @data: 2021/4/14 9:26 PM
 */
public class ReceiptEventSource implements SourceFunction<ReceiptEvent> {

    private Boolean isRuning = true;

    @Override
    public void run(SourceContext<ReceiptEvent> ctx) throws Exception {

        List<String> payChannels = Arrays.asList("wechat", "alipay");

        while (isRuning) {
            ctx.collect(new ReceiptEvent(
                    getStringRandom(10),
                    payChannels.get(new Random().nextInt(payChannels.size())),
                    System.currentTimeMillis()
            ));
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
