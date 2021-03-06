package com.atguigu.day02;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class Flink04_Source_Custom {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //TODO 自定义source
        DataStreamSource<WaterSensor> streamSource = env.addSource(new MySource()).setParallelism(2);

        streamSource.print();

        env.execute();
    }
    //自定义一个类实现SourceFunction
//    public static class MySource implements SourceFunction<WaterSensor>{
    //如果要设置多并行度，则需要实现ParallelSourceFunction接口
    public static class MySource implements ParallelSourceFunction<WaterSensor> {

        private Boolean isRunning = true;
        private volatile Random random = new Random();
        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {
            while (isRunning){
                ctx.collect(new WaterSensor("sensor_"+random.nextInt(1000), System.currentTimeMillis(), random.nextInt(2000)));
                Thread.sleep(200);
            }

        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
