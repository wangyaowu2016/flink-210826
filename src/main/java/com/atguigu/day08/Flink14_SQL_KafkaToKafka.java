package com.atguigu.day08;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Flink14_SQL_KafkaToKafka {
    public static void main(String[] args) {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //创建从kafka读取数据的表
        tableEnv.executeSql("create table source_sensor (id string, ts bigint, vc int) with (" +
                "'connector' = 'kafka'," +
                "'topic' = 'topic_source_sensor'," +
                "'properties.bootstrap.servers' = 'hadoop102:9092'," +
                "'properties.group.id' = '0826'," +
                "'scan.startup.mode' = 'latest-offset'," +
                "'format' = 'csv'" +
                ")"
        );

        //创建从kafka写数据的表
        tableEnv.executeSql("create table sink_sensor(id string, ts bigint, vc int) with("
                + "'connector' = 'kafka',"
                + "'topic' = 'topic_sink_sensor',"
                + "'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',"
                + "'format' = 'csv'"
                + ")"
        );

        tableEnv.executeSql("insert into sink_sensor select * from source_sensor where id='sensor_1'");
    }
}
