package com.atguigu.chatper09;

import com.atguigu.chapter05.ClickSource;
import com.atguigu.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * 9.2.2 支持的结构类型
 */

public class StateTest {
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

        stream.keyBy(data -> data.user)
                .flatMap(new MyFlatMap())
                .print();


        env.execute();
    }

    // 实现自定义的FlatMapFunction，用于Keyed State测试
    public static class MyFlatMap extends RichFlatMapFunction<Event, String> {
        // 定义状态
        ValueState<Event> myValueState;
        ListState<Event> myListState;
        MapState<String, Long> myMapState;
        ReducingState<Event> myReducingState;
        AggregatingState<Event, String> myAggregatingState;

        // 增加一个本地变量进行对比
        Long count = 0L;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Event> valueStateDescriptor = new ValueStateDescriptor<>("my-state", Event.class);
            myValueState = getRuntimeContext().getState(valueStateDescriptor);

            myListState = getRuntimeContext().getListState(new ListStateDescriptor<Event>("my-list", Event.class));
            myMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("my-map", String.class, Long.class));

            myReducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Event>("my-reduce",
                    new ReduceFunction<Event>() {
                        @Override
                        public Event reduce(Event value1, Event value2) throws Exception {
                            return new Event(value1.user, value1.url, value2.timestamp);
                        }
                    }
                    , Event.class));

            myAggregatingState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Event, Long, String>("my-agg",
                    new AggregateFunction<Event, Long, String>() {
                        @Override
                        public Long createAccumulator() {
                            return 0L;
                        }

                        @Override
                        public Long add(Event value, Long accmulator) {
                            return accmulator + 1;
                        }

                        @Override
                        public String getResult(Long accumulator) {
                            return "count: " + accumulator;
                        }

                        @Override
                        public Long merge(Long a, Long b) {
//                            return a + b;
                            return null;
                        }
                    }
                    , Long.class));

            // 配置状态的TTL
            // https://www.bilibili.com/video/BV133411s7Sa?p=108&spm_id_from=pageDriver&vd_source=334d133c406f06be3c611aa4910822b9
            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(1))//到了可以清除的时间
                    .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                    .build();

            valueStateDescriptor.enableTimeToLive(ttlConfig);
        }

        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {

            String key = value.user;
            // 访问和更新状态
            myValueState.update(value);
            System.out.println(key + ": value: " + myValueState.value());

            myListState.add(value);
            int listValueCount = 0;
            for (Event event : myListState.get()) {
                listValueCount++;
            }
            System.out.println(key + ": list value: " + listValueCount);

            myMapState.put(value.user, myMapState.get(value.user) == null ? 1 : myMapState.get(value.user) + 1);
            System.out.println(key + ": map value: " + myMapState.get(value.user));

            myReducingState.add(value);
            System.out.println(key + ": reducing value: " + myReducingState.get());

            myAggregatingState.add(value);
            System.out.println(key + ": agg value: " + myAggregatingState.get());

            count++;
            System.out.println("count: " + count);
        }
    }
}
