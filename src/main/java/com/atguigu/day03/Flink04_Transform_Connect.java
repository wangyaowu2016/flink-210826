package com.atguigu.day03;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class Flink04_Transform_Connect {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从元素中获取数据
        DataStreamSource<Integer> intValue = env.fromElements(1, 2, 3, 4, 5, 6);

        DataStreamSource<String> strValue = env.fromElements("a", "b", "c", "d", "e");


        //TODO 3.使用connect连接两条流
        ConnectedStreams<Integer, String> connect = intValue.connect(strValue);

        connect.map(new CoMapFunction<Integer, String, String>() {
            @Override
            public String map1(Integer value) throws Exception {
                return (value + 1) + "";
            }

            @Override
            public String map2(String value) throws Exception {
                return (value + 1) + "";
            }
        }).print();


        env.execute();
    }
}
