package com.atguigu.chapter05;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @create 2024/5/31
 * @create 22:50
 */
public class CustomPartitionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        DataStreamSource<Event> eventDataStreamSource = env.addSource(new ClickSource());
//        eventDataStreamSource.partitionCustom(new Partitioner<Event>() {
//            @Override
//            public int partition(Event integer, int i) {
//                System.out.println("integer=" + integer + ", i=" + i);
//                return  (integer.age % 3);
//            }
//        }, new KeySelector<Event, Integer>() {
//            @Override
//            public Integer getKey(Event event) throws Exception {
//                return null;
//            }
//        }).print().setParallelism(3);

        // 将自然数按照奇偶分区
        env.fromElements(123, 12, 32, 424, 12, 1, 4445, 18)
                .partitionCustom(new Partitioner<Integer>() {
                    @Override
                    public int partition(Integer key, int numPartitions) {
                        return key % 2;
                    }
                }, new KeySelector<Integer, Integer>() {
                    @Override
                    public Integer getKey(Integer value) throws Exception {
                        return value;
                    }
                })
                .print().setParallelism(2);
        env.execute();
    }
}

