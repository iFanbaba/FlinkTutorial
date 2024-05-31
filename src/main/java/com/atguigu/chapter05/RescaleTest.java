package com.atguigu.chapter05;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @create 2024/5/31
 * @create 22:05
 */
public class RescaleTest {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 构建数据流
        DataStream<Integer> input = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        // 对数据流进行重新分区
        DataStream<Integer> rescaledStream = input.rescale();

        DataStream<Integer> rebalance = input.rebalance();

        rebalance.print("rebalance");

        // 对重新分区后的数据流进行map操作
        DataStream<Integer> mappedStream = rescaledStream.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                return value * 2;
            }
        });

//        mappedStream.print("mappedStream");

        // 对map后的数据流进行filter操作
        DataStream<Integer> filteredStream = mappedStream.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer value) throws Exception {
                return value % 2 == 0;
            }
        });

        // 打印分区后的数据流
//        filteredStream.print("filteredStream");

        // 执行任务
        env.execute("Rescale Example");
    }
}
