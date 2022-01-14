package com.atguigu.day05;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Flink04_CountWindow_Tumbling {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //并行度设置为1
        env.setParallelism(1);

        //2.读取无界数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        //3.将一行数据按照空格切分
        SingleOutputStreamOperator<String> wordDStream = streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });

        //4.将数据封装为Tuple2元组
        SingleOutputStreamOperator<String> wordToOneDStream = wordDStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value;
            }
        });

        //5.将相同单词的数据聚和到一块
        KeyedStream<String, String> keyedStream = wordToOneDStream.keyBy(r->r);

        //TODO 6.开启一个基于元素个数的滚动窗口，窗口大小为5
        WindowedStream<String, String, GlobalWindow> window = keyedStream.countWindow(5);

        window.process(new ProcessWindowFunction<String, String, String, GlobalWindow>() {
                           @Override
                           public void process(String tuple, Context context, Iterable<String> elements, Collector<String> out) throws Exception {
                               String msg =
                                     "一共有 "
                                               + elements.spliterator().estimateSize() + "条数据 ";
                               out.collect(msg);
                           }
                       }
        ).print();

        env.execute();
    }
}
