package com.zzx.hotitems_analysis.udf_source;

import com.zzx.hotitems_analysis.beans.UserBehavior;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: wb-zcx696752
 * @description: 自定义用户行为数据源
 * @data: 2021/4/2 15:34 PM
 */
public class UserBehaviorSource implements SourceFunction<UserBehavior> {

    private Boolean isRunning = true;

    @Override
    public void run(SourceContext<UserBehavior> sourceContext) throws Exception {
        // 创造用户行为数据
        while (isRunning) {
            sourceContext
                    .collect(new UserBehavior(
                            Long.valueOf(new Random().nextInt(800000)),
                            Long.valueOf(new Random().nextInt(5000)),
                            new Random().nextInt(500),
                            "pv",
                            Calendar.getInstance().getTimeInMillis()
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
