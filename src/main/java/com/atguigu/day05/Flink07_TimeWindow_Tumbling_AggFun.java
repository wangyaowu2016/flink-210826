package com.atguigu.day05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class Flink07_TimeWindow_Tumbling_AggFun {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //并行度设置为1
        env.setParallelism(1);

        //2.读取无界数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        //3.将读过来的数据转为WaterSensor
        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        KeyedStream<WaterSensor, Tuple> keyedStream = map.keyBy("id");


        //开启一个基于时间的滚动窗口，窗口大小为5
        WindowedStream<WaterSensor, Tuple, TimeWindow> window = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5)));

        //TODO 使用增量聚合函数，求VC累加值
        SingleOutputStreamOperator<Integer> result = window.aggregate(new AggregateFunction<WaterSensor, Integer, Integer>() {
            /**
             * 初始化累加器
             * @return
             */
            @Override
            public Integer createAccumulator() {
                System.out.println("初始化累加器。。。");
                return 0;
            }

            /**
             * 累加操作
             * @param value
             * @param accumulator
             * @return
             */
            @Override
            public Integer add(WaterSensor value, Integer accumulator) {
                System.out.println("累加操作。。。");
                return value.getVc() + accumulator;
            }

            /**
             * 获取结果
             * @param accumulator
             * @return
             */
            @Override
            public Integer getResult(Integer accumulator) {
                System.out.println("返回结果。。。");
                return accumulator;
            }

            /**
             * 合并累加器，只在会话窗口中用
             * @param a
             * @param b
             * @return
             */
            @Override
            public Integer merge(Integer a, Integer b) {
                System.out.println("合并累加器。。。");
                return a + b;
            }
        });

        result.print();

        env.execute();
    }
}
