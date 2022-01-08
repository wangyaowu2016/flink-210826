package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class Flink01_WordCount_Batch {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //2.读文件中的数据
        DataSource<String> dataSource = env.readTextFile("input/word.txt");

        //flatMap（将一行数据按照空格切分，切出一个个单词）->map（转化数据结构，转为Tuple2元组（word，1））->reduceByKey（按照相同的单词进行聚合，并将聚合后的value相加）->打印到控制台

        //3.将一行数据按照空格切分，切出一个个单词
        FlatMapOperator<String, String> word = dataSource.flatMap(new MyFlatMap());

        //4.转化数据结构，转为Tuple2元组（word，1）
        MapOperator<String, Tuple2<String, Integer>> wordToOne = word.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
//                return new Tuple2<>(value, 1);
                return Tuple2.of(value, 1);
            }
        });

        //5.按照相同的单词进行聚合
        UnsortedGrouping<Tuple2<String, Integer>> groupBy = wordToOne.groupBy(0);

        //6.并将聚合后的value相加
        AggregateOperator<Tuple2<String, Integer>> result = groupBy.sum(1);

        //7.打印到控制台
        result.print();


    }

    //自定义一个类实现FlatMapFunction接口
    public static class MyFlatMap implements FlatMapFunction<String,String>{

        /**
         *
         * @param value 输入的数据
         * @param out 采集器，将数据采集起来发送至下游
         * @throws Exception
         */
        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(word);
            }
        }
    }
}
