package com.atguigu.day06;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class Flink10_State_Operator_Broadcast {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.分别从不同的来源获取数据
        DataStreamSource<String> hadoopStream = env.socketTextStream("hadoop102", 9999);

        DataStreamSource<String> localStream = env.socketTextStream("localhost", 9999);

        //3.定义状态并广播
        MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<>("map", String.class, String.class);

        //4.广播状态
        BroadcastStream<String> broadcastStream = localStream.broadcast(mapStateDescriptor);

        //5.连接两条流
        BroadcastConnectedStream<String, String> connect = hadoopStream.connect(broadcastStream);

        //6.根据一条流的数据来控制另一条流的逻辑
        connect.process(new BroadcastProcessFunction<String, String, String>() {
            @Override
            public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                //提取广播过来的状态
                ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                String aSwitch = broadcastState.get("switch");

                if ("1".equals(aSwitch)) {
                    System.out.println("执行逻辑1.。。。。。");
                } else if ("2".equals(aSwitch)) {
                    System.out.println("执行逻辑2.。。。。。");
                } else {
                    System.out.println("执行其他逻辑。。。。。");
                }


            }

            @Override
            public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
                //提取状态
                BroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapStateDescriptor);


                broadcastState.put("switch", value);
            }
        }).print();

        env.execute();
    }
}
