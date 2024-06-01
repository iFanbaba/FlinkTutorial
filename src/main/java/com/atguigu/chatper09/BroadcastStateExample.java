package com.atguigu.chatper09;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial
 * <p>
 * Created by  wushengran
 */

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class BroadcastStateExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取用户行为事件流
        DataStreamSource<Action> actionStream = env.fromElements(
                new Action("Alice", "login"),
                new Action("Alice", "pay"),
                new Action("Bob", "login"),
                new Action("Bob", "order"),
                new Action("Jack", "login"),
                new Action("Jack", "buy")
        );

        // 定义行为模式流，代表了要检测的标准，给予他构建广播流
        DataStreamSource<Pattern> patternStream = env
                .fromElements(
                        new Pattern("login", "pay"),
                        new Pattern("login", "buy")
                );

        // 定义广播状态的描述器，创建广播流
        // 广播变量的key，如果只是void，那么后续在processBroadcastElement和processElment和方法中，更新或者读取的时候，都需要指定某个key才能取出对应状态值。
        MapStateDescriptor<String, Pattern> bcStateDescriptor = new MapStateDescriptor<>("patterns", Types.STRING, Types.POJO(Pattern.class));
//        MapStateDescriptor<Void, Pattern> bcStateDescriptor = new MapStateDescriptor<>("patterns", Types.VOID, Types.POJO(Pattern.class));
        BroadcastStream<Pattern> bcPatterns = patternStream.broadcast(bcStateDescriptor);

        // 将事件流和广播流连接起来，进行处理
        DataStream<Tuple2<String, Pattern>> matches = actionStream
                .keyBy(data -> data.userId)
                .connect(bcPatterns)
                .process(new PatternEvaluator());

        matches.print();

        env.execute();
    }

    //KeyedBroadcastProcessFunction<KS, IN1, IN2, OUT>
    //KS: 主流的key类型
    //IN1: 主流类型
    //IN2: 广播流类型
    //OUT: 输出流的类型
    public static class PatternEvaluator extends KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String, Pattern>> {

        // 定义一个值状态，保存上一次用户行为
        ValueState<String> prevActionState;

        @Override
        public void open(Configuration conf) {
            //此为初始化只状态，用于保存用户上一次的行为。
            prevActionState = getRuntimeContext().getState(new ValueStateDescriptor<>("lastAction", Types.STRING));
        }

        @Override
        public void processBroadcastElement(Pattern pattern, Context ctx, Collector<Tuple2<String, Pattern>> out) throws Exception {

            //从上下文中获取广播状态，并用当前数据更新广播状态
            BroadcastState<String, Pattern> bcState = ctx.getBroadcastState(new MapStateDescriptor<>("patterns", Types.STRING, Types.POJO(Pattern.class)));

            // 将广播状态更新为当前的pattern
            bcState.put("xxxx", pattern);
        }

        @Override
        public void processElement(Action action, ReadOnlyContext ctx, Collector<Tuple2<String, Pattern>> out) throws Exception {
            //在 processElement 方法中只能读取状态，不能修改
            ReadOnlyBroadcastState<String, Pattern> patterns = ctx.getBroadcastState(new MapStateDescriptor<>("patterns", Types.STRING, Types.POJO(Pattern.class)));
            //因为该广播状态的的key类型为null，所以直接get(null)即可将状态值取出
            Pattern pattern = patterns.get("xxxx");
            //用户上一次的行为，并不在广播状态中，而是在我们在本类中定义的ValueState中。
            String prevAction = prevActionState.value();

            if (pattern != null && prevAction != null) {
                // 如果前后两次行为都符合模式定义，输出一组匹配
                if (pattern.action1.equals(prevAction) && pattern.action2.equals(action.action)) {

                    //ctx.getCurrentKey()获取主流的key，为之前在keyBy
                    out.collect(new Tuple2<>(ctx.getCurrentKey(), pattern));
                }
            }
            // 更新状态
            prevActionState.update(action.action);
        }
    }

    // 定义用户行为事件POJO类
    public static class Action {
        public String userId;
        public String action;

        public Action() {
        }

        public Action(String userId, String action) {
            this.userId = userId;
            this.action = action;
        }

        @Override
        public String toString() {
            return "Action{" +
                    "userId=" + userId +
                    ", action='" + action + '\'' +
                    '}';
        }
    }

    // 定义行为模式POJO类，包含先后发生的两个行为
    public static class Pattern {
        public String action1;
        public String action2;

        public Pattern() {
        }

        public Pattern(String action1, String action2) {
            this.action1 = action1;
            this.action2 = action2;
        }

        @Override
        public String toString() {
            return "Pattern{" +
                    "action1='" + action1 + '\'' +
                    ", action2='" + action2 + '\'' +
                    '}';
        }
    }
}

