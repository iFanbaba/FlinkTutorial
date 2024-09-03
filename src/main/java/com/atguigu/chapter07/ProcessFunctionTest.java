package com.atguigu.chapter07;

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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class ProcessFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event event, long l) {
                                        return event.timestamp;
                                    }
                                })
                )
                .process(new ProcessFunction<Event, String>() {
                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
//                        if (value.user.equals("Mary")) {
//                            out.collect(value.user + " clicks " + value.url) ;

//                        } else if (value.user.equals("Bob")) {
//                            out.collect(value.user);
//                            out.collect(value.user);
//                        }
//                        System.out.println(ctx.timestamp());
                        //ctx.timestamp()是当前数据的时间戳，
                        //ctx.timerService().currentWatermark()是当前的水印，他是 上一个 timestamp - 乱序时间 - 1 得到的
                        //源码: output.emitWatermark(new Watermark(this.maxTimestamp - this.outOfOrdernessMillis - 1L))
                        System.out.println(value);
                        System.out.println(ctx.timestamp() + " -> " + ctx.timerService().currentWatermark() + " -> " + (ctx.timestamp()-ctx.timerService().currentWatermark()));

                        //会直接报错，因为只能在keystream中设置定时器
//                        ctx.timerService().registerEventTimeTimer(1725373270240L);
                    }
                })
                .print();

        env.execute();
    }
}

