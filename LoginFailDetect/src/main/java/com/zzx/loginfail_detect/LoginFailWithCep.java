package com.zzx.loginfail_detect;

import com.zzx.loginfail_detect.beans.LoginEvent;
import com.zzx.loginfail_detect.udf_source.LoginEventSource;
import com.zzx.loginfail_detect.beans.LoginFailWarning;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 *
 * @ProjectName: UserBehaviorAnalysis
 * @ClassName: LoginFailWithCep
 * @author: wb-zcx696752
 * @description:
 * @data: 2021/4/13 10:27 PM
 */
public class LoginFailWithCep {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 从自定义数据源中读取数据
        SingleOutputStreamOperator<LoginEvent> loginEventStream = env
                .addSource(new LoginEventSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<LoginEvent>forMonotonousTimestamps()
                                .withTimestampAssigner((event, l) -> event.getTimestamp())
                );

        // 1. 定义一个匹配模式
        // firstFail -> lastFail  with in 1min
        Pattern<LoginEvent, LoginEvent> loginFailPattern0 = Pattern
                .<LoginEvent>begin("firstFail")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return "fail".equals(loginEvent.getLoginState());
                    }
                })
                .next("secondFail")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return "fail".equals(loginEvent.getLoginState());
                    }
                })
                .next("thirdFail")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return "fail".equals(loginEvent.getLoginState());
                    }
                })
                .within(Time.seconds(10));

        // 循环模式
        Pattern<LoginEvent, LoginEvent> loginFailPattern = Pattern
                .<LoginEvent>begin("failEvents")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return "fail".equals(loginEvent.getLoginState());
                    }
                })
                .times(5)
                .consecutive() // 连续，设定的事件必须是连续的
                .within(Time.seconds(10));

        // 2. 将匹配模式应用到数据流上，得到一个 pattern stream
        PatternStream<LoginEvent> patternStream = CEP
                .pattern(loginEventStream
                                .keyBy(LoginEvent::getUserId),
                        loginFailPattern);

        // 3. 检测出符合匹配条件的复杂事件，进行转换处理，得到报警信息
        SingleOutputStreamOperator<LoginFailWarning> warningStream = patternStream
                .select(new LoginFailMatchDetectWarning());

        warningStream.print();

        env.execute("Login fail Detect With CEP Job");
    }

    // 实现自定义的 PatternSelectFunction
    public static class LoginFailMatchDetectWarning
            implements PatternSelectFunction<LoginEvent, LoginFailWarning> {

        @Override
        public LoginFailWarning select(Map<String, List<LoginEvent>> pattern) throws Exception {
//            LoginEvent firstFailEvent = pattern.get("firstFail").get(0);
//            LoginEvent secondFailEvent = pattern.get("secondFail").get(0);
//            LoginEvent lastFailEvent = pattern.get("thirdFail").get(0);

            LoginEvent firstFailEvent = pattern.get("failEvents").get(0);
            LoginEvent lastFailEvent = pattern.get("failEvents").get(pattern.get("failEvents").size() - 1);

            return new LoginFailWarning(
                    firstFailEvent.getUserId(),
                    firstFailEvent.getTimestamp(),
                    lastFailEvent.getTimestamp(),
                    "login fail in 10s for " + pattern.get("failEvents").size() + " times."
            );
        }
    }

}
