package com.atguigu.day02;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

public class Flink02_Source_Collection {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //TODO 2.从集合中获取数据(不能设置多并行度)
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
//        DataStreamSource<Integer> streamSource = env.fromCollection(list);

        //TODO 从文件读取数据
//        DataStreamSource<String> streamSource = env.readTextFile("input/word.txt").setParallelism(2);

        //TODO 从端口读取数据
//        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        //TODO 从元素中读取数据
        DataStreamSource<String> streamSource = env.fromElements("a", "b", "c", "d");


        streamSource.print();

        env.execute();
    }
}
