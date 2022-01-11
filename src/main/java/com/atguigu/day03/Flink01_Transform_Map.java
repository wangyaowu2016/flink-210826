package com.atguigu.day03;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink01_Transform_Map {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //2.从端口读取数据
//        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);
        DataStreamSource<String> streamSource = env.readTextFile("input/sensor.txt");

        //TODO 3.使用map将读进来的数据转为JavaBean
        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MyMap());

        map.print();

        env.execute();
    }

    public static class MyMap extends RichMapFunction<String,WaterSensor>{

        /**
         * 生命周期方法，程序最开始时调用（每个并行度调用一次）
         * 通常在这个方法中写初始化或者创建连接相关的代码
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open......");
        }

        /**
         * 生命周期方法,程序最后结束时调用（每个并行度调用一次，只有在读文件时每个并行度调用两次）
         * 通常在这个方法中写关闭连接的代码
         * @throws Exception
         */
        @Override
        public void close() throws Exception {
            System.out.println("close.......");
        }

        @Override
        public WaterSensor map(String value) throws Exception {
            System.out.println(getRuntimeContext().getTaskName());
            System.out.println(getRuntimeContext().getTaskNameWithSubtasks());
            System.out.println(getRuntimeContext().getJobId());
            String[] split = value.split(" ");
            return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
        }
    }
}
