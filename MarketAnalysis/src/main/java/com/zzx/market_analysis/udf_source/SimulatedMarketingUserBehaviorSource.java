package com.zzx.market_analysis.udf_source;

import com.zzx.market_analysis.beans.MarketingUserBehavior;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 *
 * @ProjectName: UserBehaviorAnalysis
 * @ClassName: SimulatedMarketingUserBehaviorSource
 * @author: wb-zcx696752
 * @description: 自定义模拟用户市场行为数据源
 * @data: 2021/4/12 10:32 PM
 */
public class SimulatedMarketingUserBehaviorSource implements SourceFunction<MarketingUserBehavior> {

    private Boolean isRunning = true;

    @Override
    public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
        // 定义用户行为
        List<String> behaviorList = Arrays.asList("CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL");

        // 定义渠道
        List<String> channelList = Arrays.asList("app store", "wechat", "weibo");

        // 随机生成数据
        while (isRunning) {
            ctx.collect(new MarketingUserBehavior(
                    new Random().nextLong(),
                    behaviorList.get(new Random().nextInt(behaviorList.size())),
                    channelList.get(new Random().nextInt(channelList.size())),
                    System.currentTimeMillis()
            ));
            // 控制数据生成输出频率
            Thread.sleep(100);
        }
    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }
}
