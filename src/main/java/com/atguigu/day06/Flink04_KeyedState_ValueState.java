package com.atguigu.day06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.OffsetDateTime;

public class Flink04_KeyedState_ValueState {
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

        //TODO 5.检测传感器的水位线值，如果连续的两个水位线差值超过10，就输出报警。
        SingleOutputStreamOperator<String> result = keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, String>() {
            //TODO 1.定义状态
            private ValueState<Integer> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //TODO 2. 初始化状态
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("value-state", Integer.class));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                //1.先取出状态中保存的上一次水位
                Integer lastVc = valueState.value()==null?0:valueState.value();

//                if (valueState.value()==null){
//                    valueState.update(0);
//                }

                //2.根据取出的上一次水位和当前水位做查取绝对值，判断是否大于10
                if (Math.abs(value.getVc()-lastVc)>10){
                    out.collect("水位相差超过10");
                }

                //3.将当前水位保存到状态中
                valueState.update(value.getVc());

            }

        });

        result.print();


        env.execute();
    }
}
