package com.atguigu.chatper09;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial
 * <p>
 * Created by  wushengran
 */

import com.atguigu.chapter05.ClickSource;
import com.atguigu.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;

import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Calendar;


/**
 * 9.2.3
 * 1. 值状态（ValueState）
 * 我们这里会使用用户 id 来进行分流，然后分别统计每个用户的 pv 数据，由于我们并不想
 * 每次 pv 加一，就将统计结果发送到下游去，所以这里我们注册了一个定时器，用来隔一段时
 * 间发送 pv 的统计结果，这样对下游算子的压力不至于太大。具体实现方式是定义一个用来保
 * 存定时器时间戳的值状态变量。当定时器触发并向下游发送数据以后，便清空储存定时器时间
 * 戳的状态变量，这样当新的数据到来时，发现并没有定时器存在，就可以注册新的定时器了，
 * 注册完定时器之后将定时器的时间戳继续保存在状态变量中。
 */
public class PeriodicPvExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );

        stream.print("input");

        // 统计每个用户的pv，隔一段时间（10s）输出一次结果
        stream.keyBy(data -> data.user)
                .process(new PeriodicPvResult())
                .print();

        env.execute();
    }

    // 注册定时器，周期性输出pv
    public static class PeriodicPvResult extends KeyedProcessFunction<String, Event, String> {
        // 定义两个状态，保存当前pv值，以及定时器时间戳
        ValueState<Long> countState;
        ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count", Long.class));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTs", Long.class));
        }

        @Override
        public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
            // 更新count值
            Long count = countState.value();
            if (count == null) {
                countState.update(1L);
            } else {
                countState.update(count + 1);
            }
            // 注册定时器，注册到10秒之后执行 onTimer 方法
            if (timerTsState.value() == null) {
                System.out.println("判断定时器...");
                ctx.timerService().registerEventTimeTimer(value.timestamp + 10 * 1000L);
                timerTsState.update(value.timestamp + 10 * 1000L);
            }
        }

        //timestamp: 注册的时间
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            long now = Calendar.getInstance().getTimeInMillis();
            out.collect(ctx.getCurrentKey() + " pv: " + countState.value()
                    + " timestamp: " + new Timestamp(timestamp)
                    + " now: " + new Timestamp(now)
                    + " diff: " + (now - timestamp)
            );
            // 清空状态
//            timerTsState.clear();

            // 状态清空后，立即注册一个新的定时器，那么在processElement中就只需要判断一次timerTsState是否为null
            ctx.timerService().registerEventTimeTimer(timestamp + 10 * 1000L);
//            timerTsState.update(timestamp + 10 * 1000L);
        }
    }
}


