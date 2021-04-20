package com.zzx.loginfail_detect;

import com.zzx.loginfail_detect.beans.LoginEvent;
import com.zzx.loginfail_detect.udf_source.LoginEventSource;
import com.zzx.loginfail_detect.beans.LoginFailWarning;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 *
 * @ProjectName: UserBehaviorAnalysis
 * @ClassName: LoginFail
 * @author: wb-zcx696752
 * @description: 登录失败检测
 * @data: 2021/4/12 17:02 PM
 */
public class LoginFail {
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

//        loginEventStream.print("data");

        // 自定义处理函数，检测连续登录失败事件
        SingleOutputStreamOperator<LoginFailWarning> warningStream = loginEventStream
                .keyBy(LoginEvent::getUserId)
                .process(new LoginFailDetectWarning(5));

        warningStream.print("warning");

        env.execute("Login fail Detect Job");
    }

    // 实现自定义 KeyedProcessFunction
    public static class LoginFailDetectWarning0 extends KeyedProcessFunction<Long, LoginEvent, LoginFailWarning> {

        // 最大连续登录失败次数 默认3次
        private Integer maxFailTimes = 3;

        public LoginFailDetectWarning0() {
        }

        public LoginFailDetectWarning0(Integer maxFailTimes) {
            this.maxFailTimes = maxFailTimes;
        }

        // 定义状态: 保存设定时间内登录失败事件
        ListState<LoginEvent> loginFailEventListState;

        // 定义状态: 保存注册的定时器时间戳
        ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ListStateDescriptor<LoginEvent> loginEventListStateDescriptor = new ListStateDescriptor<>("login-fail-list", TypeInformation.of(LoginEvent.class));
            ValueStateDescriptor<Long> timerTsStateDescriptor = new ValueStateDescriptor<>("timer-ts", TypeInformation.of(Long.TYPE));

            loginFailEventListState = getRuntimeContext().getListState(loginEventListStateDescriptor);
            timerTsState = getRuntimeContext().getState(timerTsStateDescriptor);
        }

        @Override
        public void processElement(LoginEvent value,
                                   Context ctx,
                                   Collector<LoginFailWarning> out) throws Exception {
            // 判断当前登录事件类型
            if ("fail".equals(value.getLoginState())) {
                // 1. 如果是失败事件，添加到列表状态中
                loginFailEventListState.add(value);
                // 判断是否有定时器，如果没有，注册一个一分钟之后的定时器
                if (timerTsState.value() == null) {
                    Long ts = value.getTimestamp() + 60 * 1000;
                    ctx.timerService().registerEventTimeTimer(ts);
                    timerTsState.update(ts);
                }
            } else {
                // 2. 如果是成功，就删除定时器，清空状态，重新开始
                if (timerTsState.value() != null) ctx.timerService().deleteEventTimeTimer(timerTsState.value());
                loginFailEventListState.clear();
                timerTsState.clear();
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<LoginFailWarning> out) throws Exception {
            // 如果定时器触发，证明2秒内没有登录成功，判断 listState 中失败的次数
            List<LoginEvent> loginFailEvents = Lists.newArrayList(loginFailEventListState.get().iterator());
            int failTimes = loginFailEvents.size();

            // 如果超出设定的最大的失败次数，则输出报警
            if (failTimes >= maxFailTimes) {
                out.collect(new LoginFailWarning(
                        ctx.getCurrentKey(),
                        loginFailEvents.get(0).getTimestamp(),
                        loginFailEvents.get(failTimes - 1).getTimestamp(),
                        "login fail in 2s for " + failTimes + "times.")
                );
            }

            // 清空状态
            loginFailEventListState.clear();
            timerTsState.clear();
        }
    }

    // 实现自定义 KeyedProcessFunction
    public static class LoginFailDetectWarning extends KeyedProcessFunction<Long, LoginEvent, LoginFailWarning> {

        // 最大连续登录失败次数 默认3次
        private Integer maxFailTimes = 3;

        public LoginFailDetectWarning() {
        }

        public LoginFailDetectWarning(Integer maxFailTimes) {
            this.maxFailTimes = maxFailTimes;
        }

        // 定义状态: 保存设定时间内登录失败事件
        ListState<LoginEvent> loginFailEventListState;


        @Override
        public void open(Configuration parameters) throws Exception {
            ListStateDescriptor<LoginEvent> loginEventListStateDescriptor = new ListStateDescriptor<>("login-fail-list", TypeInformation.of(LoginEvent.class));
            loginFailEventListState = getRuntimeContext().getListState(loginEventListStateDescriptor);
        }

        // 以登录事件作为判断报警的触发条件
        @Override
        public void processElement(LoginEvent value,
                                   Context ctx,
                                   Collector<LoginFailWarning> out) throws Exception {

            // 判断当前事件的登录状态
            if ("fail".equals(value.getLoginState())) {
                // 1. 如果登录失败，直接将当前事件存入状态
                loginFailEventListState.add(value);

                // 1.2 如果是失败事件，获取状态中之前的登录失败事件，继续判断是否已有失败事件
                Iterator<LoginEvent> iterator = loginFailEventListState.get().iterator();
                List<LoginEvent> loginFailEvents = Lists.newArrayList(iterator);

                // 判断登录失败次数是否大于等于 maxFailTimes，如果大于等于则进行下一步操作
                if (loginFailEvents.size() >= maxFailTimes) {
                    // 1.3 如果状态中已经有登录失败事件，继续判断时间戳是否在1分钟之内
                    // 获取第一次登录失败事件
                    LoginEvent firstFailEvent = loginFailEvents.get(0);
                    if (value.getTimestamp() - firstFailEvent.getTimestamp() <= 60 * 1000) {
                        // 如果在一分钟之内，则输出报警
                        out.collect(
                                new LoginFailWarning(
                                        value.getUserId(),
                                        firstFailEvent.getTimestamp(),
                                        value.getTimestamp(),
                                        "login fail " + maxFailTimes + " times in 1min"
                                )
                        );
                    }else{
                        // 如果当前事件时间超出和第一次事件时间，规定的相隔时间，则移除第一次事件，更新当前状态
                        loginFailEventListState.update(loginFailEvents.subList(1,loginFailEvents.size()-1));
                    }
                }
            } else {
                // 2. 如果登录成功，清空状态
                loginFailEventListState.clear();
            }
        }
    }
}
