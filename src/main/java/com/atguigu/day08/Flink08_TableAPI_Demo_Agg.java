package com.atguigu.day08;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class Flink08_TableAPI_Demo_Agg {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> waterSensorStream =
                env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 4000L, 40),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_2", 6000L, 60));

        //TODO 获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 将流转为表
        Table table = tableEnv.fromDataStream(waterSensorStream);

        //TODO 通过连续查询查询表中的数据
        Table resultTable = table
                .groupBy($("id"))
                .select($("id"),$("vc").sum())
                ;

        //TODO 将表转为流
//        DataStream<Row> resultDataStream = tableEnv.toAppendStream(resultTable, Row.class);
        //如果查询出来的结果动态表数据有更新的话，需要用撤回流
        DataStream<Tuple2<Boolean, Row>> resultDataStream = tableEnv.toRetractStream(resultTable, Row.class);

        resultDataStream.print();

        env.execute();

    }
}
