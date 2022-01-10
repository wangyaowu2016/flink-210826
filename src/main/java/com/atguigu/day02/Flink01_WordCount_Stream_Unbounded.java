package com.atguigu.day02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink01_WordCount_Stream_Unbounded {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //通过这种获取执行环境的方式可以在本地通过访问 localhost:8081来访问UI界面
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //并行度设置为1
        env.setParallelism(1);

        //全局都不串
//        env.disableOperatorChaining();

        //2.读取无界数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //3.将一行数据按照空格切分
        SingleOutputStreamOperator<String> wordDStream = streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        })
                //设置共享组
                .slotSharingGroup("group1")
//                .setParallelism(3)
                //与前面都断开
//                .startNewChain()
                //与前后都断开
//                .disableChaining()
                ;

        //4.将数据封装为Tuple2元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDStream = wordDStream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        });

        //5.将相同单词的数据聚和到一块
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordToOneDStream.keyBy(0);

        //6.累加单词个数
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);

        result.print();

        env.execute();
    }
}
