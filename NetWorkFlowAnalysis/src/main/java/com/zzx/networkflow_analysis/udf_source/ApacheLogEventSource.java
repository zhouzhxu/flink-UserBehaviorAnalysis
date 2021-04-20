package com.zzx.networkflow_analysis.udf_source;


import com.zzx.networkflow_analysis.beans.ApacheLogEvent;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 *
 * @ProjectName: UserBehaviorAnalysis
 * @ClassName: ApacheLogEventSource
 * @author: wb-zcx696752
 * @description:
 * @data: 2021/4/7 15:02 PM
 */
public class ApacheLogEventSource implements SourceFunction<ApacheLogEvent> {

    private Boolean isRunning = true;

    /*
     * 随机生成国内IP地址
     */
    public static String getRandomIp() {

        // ip范围
        int[][] range = {{607649792, 608174079},// 36.56.0.0-36.63.255.255
                {1038614528, 1039007743},// 61.232.0.0-61.237.255.255
                {1783627776, 1784676351},// 106.80.0.0-106.95.255.255
                {2035023872, 2035154943},// 121.76.0.0-121.77.255.255
                {2078801920, 2079064063},// 123.232.0.0-123.235.255.255
                {-1950089216, -1948778497},// 139.196.0.0-139.215.255.255
                {-1425539072, -1425014785},// 171.8.0.0-171.15.255.255
                {-1236271104, -1235419137},// 182.80.0.0-182.92.255.255
                {-770113536, -768606209},// 210.25.0.0-210.47.255.255
                {-569376768, -564133889}, // 222.16.0.0-222.95.255.255
        };

        Random rdint = new Random();
        int index = rdint.nextInt(10);
        String ip = num2ip(range[index][0] + new Random().nextInt(range[index][1] - range[index][0]));
        return ip;
    }

    /*
     * 将十进制转换成ip地址
     */
    public static String num2ip(int ip) {
        int[] b = new int[4];
        String x = "";

        b[0] = (int) ((ip >> 24) & 0xff);
        b[1] = (int) ((ip >> 16) & 0xff);
        b[2] = (int) ((ip >> 8) & 0xff);
        b[3] = (int) (ip & 0xff);
        x = Integer.toString(b[0]) + "." + Integer.toString(b[1]) + "." + Integer.toString(b[2]) + "." + Integer.toString(b[3]);

        return x;
    }

    //生成随机用户名，数字和字母组成,
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

    @Override
    public void run(SourceContext<ApacheLogEvent> ctx) throws Exception {
        // 初始化方法名
        String[] methods = {"GET", "POST", "HEAD", "DELETE", "PUT", "CONNECT", "OPTIONS", "TRACE"};

        String[] urls = {
                "/api/v0.1/get",
                "/api/v0.1/post",
                "/api/v0.1/head",
                "/api/v0.1/delete",
                "/api/v0.1/put",
                "/api/v0.1/connect",
                "/api/v0.1/options",
                "/api/v0.1/trace"
        };

        // 创造访问数据信息
        while (isRunning) {
            ctx
                    .collect(new ApacheLogEvent(getRandomIp(),
                            getStringRandom(6),
                            Calendar.getInstance().getTimeInMillis(),
                            methods[new Random().nextInt(methods.length)],
                            urls[new Random().nextInt(urls.length)]));

            // 控制数据生成输出频率
            Thread.sleep(100);
        }
    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }
}
