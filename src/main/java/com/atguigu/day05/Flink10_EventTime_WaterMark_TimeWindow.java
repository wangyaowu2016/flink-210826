package com.atguigu.day05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Flink10_EventTime_WaterMark_TimeWindow {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        //2.从端口读取数据
//        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);
        DataStreamSource<String> streamSource = env.readTextFile("input/sensor.txt");

        //3.将数据转为WaterSensor
        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        //TODO 4.指定单调增长的WaterMark，固定延迟为0S（乱序程度为0S）
        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = map.assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs() * 1000;
                    }
                })
        ).setParallelism(1);

        //5.将id的数据聚和到一块
        KeyedStream<WaterSensor, String> keyedStream = waterSensorSingleOutputStreamOperator.keyBy(r->r.getId());

        //6.开启一个基于事件时间的滚动窗口
        WindowedStream<WaterSensor, String, TimeWindow> window = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)));

        window.process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                           @Override
                           public void process(String key, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                               String msg = "当前key: " + key
                                       + "窗口: [" + context.window().getStart() / 1000 + "," + context.window().getEnd() / 1000 + ") 一共有 "
                                       + elements.spliterator().estimateSize() + "条数据 ";
                               out.collect(msg);
                           }
                       }
        ).print();

        env.execute();
    }
}
