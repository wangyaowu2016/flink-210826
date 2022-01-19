package com.atguigu.day08;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.e;

public class Flink11_TableAPI_Connect_File_Sink {
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

        //3.将流转为表
        Table table = tableEnv.fromDataStream(waterSensorStream, $("id"), $("ts"), $("vc"));

        Table tableResult = table
                .where($("id").isEqual("sensor_1"))
                .select($("id"), $("ts"), $("vc"));


        //TODO 连接外部文件系统获取数据源
        Schema schema = new Schema();
        schema.field("id", "STRING");
        schema.field("ts", DataTypes.BIGINT());
        schema.field("vc", DataTypes.INT());

        tableEnv.connect(new FileSystem().path("output/sensor-sql.txt"))
                .withFormat(new Csv().fieldDelimiter('|'))
                .withSchema(schema)
                .createTemporaryTable("sensor");

        //将查询出来的结果表的数据插入到临时表中，以此将数据写入文件系统
        tableResult.executeInsert("sensor");


    }
}
