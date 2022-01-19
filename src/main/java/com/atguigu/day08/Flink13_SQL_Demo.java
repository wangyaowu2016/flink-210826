package com.atguigu.day08;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import static org.apache.flink.table.api.Expressions.$;

public class Flink13_SQL_Demo {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        DataStreamSource<WaterSensor> waterSensorStream =
                env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 4000L, 40),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_2", 6000L, 60));

        //2.获取流的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //3.将流转为动态表(未注册的表)
        Table table = tableEnv.fromDataStream(waterSensorStream);

        //TODO 注册这个动态表(可以直接在流转为表的时候注册，也可以在流转为表时再进行注册)
        tableEnv.createTemporaryView("sensor", table);

        //4.查询数据
//        TableResult tableResult = tableEnv.executeSql("select * from " + table + " where id='sensor_1'");
        TableResult tableResult = tableEnv.executeSql("select * from sensor where id='sensor_1'");
//        Table resultTable = tableEnv.sqlQuery("select * from " + table + " where id='sensor_1'");

        tableResult.print();

//        resultTable.execute().print();

    }
}
