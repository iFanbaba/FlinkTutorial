package com.atguigu.cardata;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @create 2024/6/20
 * @create 21:07
 */
public class JSONFileToKafka {
    public static void main(String[] args) throws Exception {
        //flinks读取data.txt（json格式文件）文件并进行解析，写入到mysql中（每个{}为一条数据） 这个可以解决吗
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        DataStreamSource<String> originData = env.readTextFile("D:\\1.学习资料\\★尚硅谷\\尚硅谷大数据学科--项目实战\\尚硅谷大数据技术之新能源汽车数仓\\data\\data-2024-06-20.log");
        originData.print("json");
        env.execute();
    }
}
