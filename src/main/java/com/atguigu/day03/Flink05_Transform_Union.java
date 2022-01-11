package com.atguigu.day03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class Flink05_Transform_Union {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从元素中获取数据
        DataStreamSource<String> intValue = env.fromElements("1", "2", "3", "4", "5", "6");

        DataStreamSource<String> strValue = env.fromElements("a", "b", "c", "d", "e");


        //TODO 3.使用union连接两条流
        DataStream<String> union = intValue.union(strValue);

        union.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value + "aaa";
            }
        }).print();

        env.execute();
    }
}
