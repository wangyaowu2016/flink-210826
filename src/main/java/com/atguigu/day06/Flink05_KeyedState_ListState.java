package com.atguigu.day06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;

public class Flink05_KeyedState_ListState {
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

        //TODO 5.针对每个传感器输出最高的3个水位值。
        SingleOutputStreamOperator<String> result = keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, String>() {
            //TODO 1.定义状态
            private ListState<Integer> listState;


            @Override
            public void open(Configuration parameters) throws Exception {
                //TODO 2. 初始化状态
                listState = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("list-state", Integer.class));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                //1.先获取状态中的数据
                Iterable<Integer> topVcIter = listState.get();

                //2.创建一个集合用来保存迭代器中的数据和当前的数据
                ArrayList<Integer> listVc = new ArrayList<>();

                //3.遍历迭代器中的数据并放入list集合
                for (Integer integer : topVcIter) {
                    listVc.add(integer);
                }

                //4.把当前的数据放入list集合中
                listVc.add(value.getVc());

                //5.对list集合中的数据进行由大到小排序
//                listVc.sort((o1, o2) -> o2-o1);
                listVc.sort(new Comparator<Integer>() {
                    @Override
                    public int compare(Integer o1, Integer o2) {
                        return o2-o1;
                    }
                });

                //6.判断集合的元素个数，如果大于三个元素，则删除第四个元素
                if (listVc.size()>3){
                    listVc.remove(3);
                }

                //7.将listVc集合中的数据更新到状态中
                listState.update(listVc);

                out.collect(listVc.toString());

            }

        });

        result.print();


        env.execute();
    }
}
