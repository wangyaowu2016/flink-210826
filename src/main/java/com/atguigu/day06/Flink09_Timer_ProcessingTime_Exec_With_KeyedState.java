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

public class Flink09_Timer_ProcessingTime_Exec_With_KeyedState {
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

        //TODO 5.监控水位传感器的水位值，如果水位值在五秒钟之内连续上升，则报警，并将报警信息输出到侧输出流。
        SingleOutputStreamOperator<String> result = keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, String>() {
            //定义一个状态，用来保存上一次的水位
//            private Integer lastVc = Integer.MIN_VALUE;
            private ValueState<Integer> lastVc;


            //定一个状态，用来保存定时器的定时时间
//            private Long timer = Long.MIN_VALUE;
            private ValueState<Long> timer;

            @Override
            public void open(Configuration parameters) throws Exception {
                //初始化状态
                lastVc = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("lastVc-State", Integer.class,Integer.MIN_VALUE));

                timer = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-State", Long.class,Long.MIN_VALUE));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                //判断当前水位是否高于上一次水位值
                if (value.getVc() > lastVc.value()) {
                    //判断是否为5s内的第一条数据，如果是的话要注册定时器
                    //如果定时器没被注册，为 Long.MIN_VALUE 则证明为5S内的第一条数据
                    if (timer.value() == Long.MIN_VALUE) {
                        //注册一个基于处理时间的定时器
                        timer.update(ctx.timerService().currentProcessingTime() + 5000);
                        System.out.println("注册定时器：" + ctx.getCurrentKey() + "定时时间：" + timer.value());
                        ctx.timerService().registerProcessingTimeTimer(timer.value());
                    }
                } else {
                    //如果水位没有上升
                    //删除定时器
                    System.out.println("删除定时器：" + ctx.getCurrentKey() + "定时时间：" + timer.value());
                    ctx.timerService().deleteProcessingTimeTimer(timer.value());
                    //重置定时器时间
//                    timer.update(Long.MIN_VALUE);
                    timer.clear();
                }

                //无论如何都要将当前水位保存到上一次水位中
                lastVc.update(value.getVc());

                out.collect("一切正常");
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                //获取到侧输出
                ctx.output(new OutputTag<String>("output") {
                }, "警报！！！！连续5s水位上升！！！");
                //报警之后为了方便下一个5s的数据注册定时器，需要重置定时器时间
                timer.clear();
            }
        });

        result.print("主流");

        result.getSideOutput(new OutputTag<String>("output") {
        }).print("侧输出");


        env.execute();
    }
}
