package com.atguigu.day06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Flink01_Timer_Processing {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        //3.将数据转为WaterSensor
        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        //4.对相同id的数据聚和到一块
        KeyedStream<WaterSensor, Tuple> keyedStream = map.keyBy("id");

        //TODO 5.使用基于处理时间的定时器
        keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, String>() {
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                //注册一个基于处理时间的定时器
                System.out.println("创建WaterMark" + ((System.currentTimeMillis())));
                ctx.timerService().registerProcessingTimeTimer(System.currentTimeMillis() + 200);
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                out.collect("创建WaterMark" + System.currentTimeMillis());
                ctx.timerService().registerProcessingTimeTimer(System.currentTimeMillis()+200);
            }
        }).print();


        env.execute();
    }
}
