package com.zzx.market_analysis.udf_source;

import com.zzx.market_analysis.beans.AdClickEvent;
import com.zzx.market_analysis.beans.Province;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 *
 * @ProjectName: UserBehaviorAnalysis
 * @ClassName: AdClickEventSource
 * @author: wb-zcx696752
 * @description:
 * @data: 2021/4/12 15:13 PM
 */
public class AdClickEventSource implements SourceFunction<AdClickEvent> {

    private Boolean isRunning = true;

    @Override
    public void run(SourceContext<AdClickEvent> ctx) throws Exception {
        // 初始化省份和城市
        List<Province> provinceList = Arrays.asList(
                new Province("北京市", "北京市"),
                new Province("广东省", "广州市"),
                new Province("广东省", "深圳市"),
                new Province("上海市", "上海市"),
                new Province("广东省", "珠海市"),
                new Province("广东省", "东莞市"),
                new Province("浙江省", "杭州市"),
                new Province("浙江省", "绍兴市"),
                new Province("浙江省", "温州市"),
                new Province("河南省", "郑州市"),
                new Province("河南省", "周口市"),
                new Province("河南省", "商丘市")
        );
        while (isRunning) {
            Province province = provinceList.get(new Random().nextInt(provinceList.size()));
            ctx.collect(new AdClickEvent(
                    Long.valueOf(new Random().nextInt(10)),
                    Long.valueOf(new Random().nextInt(10)),
                    province.getProvince(),
                    province.getCity(),
                    System.currentTimeMillis()
            ));
            // 控制测试数据生成输出频率
            Thread.sleep(100);
        }
    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }
}
