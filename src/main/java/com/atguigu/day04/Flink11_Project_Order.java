package com.atguigu.day04;

import com.atguigu.bean.OrderEvent;
import com.atguigu.bean.TxEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

public class Flink11_Project_Order {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从不同的文件中获取两条流
        DataStreamSource<String> orderDStream = env.readTextFile("input/OrderLog.csv");

        DataStreamSource<String> receiptDStream = env.readTextFile("input/ReceiptLog.csv");

        //3.分别将两条流转为JavaBean
        SingleOutputStreamOperator<OrderEvent> orderEventDStream = orderDStream.map(new MapFunction<String, OrderEvent>() {
            @Override
            public OrderEvent map(String value) throws Exception {
                String[] split = value.split(",");
                return new OrderEvent(Long.parseLong(split[0]),
                        split[1],
                        split[2],
                        Long.parseLong(split[3]));
            }
        });

        SingleOutputStreamOperator<TxEvent> txEventDStream = receiptDStream.map(new MapFunction<String, TxEvent>() {
            @Override
            public TxEvent map(String value) throws Exception {
                String[] split = value.split(",");
                return new TxEvent(split[0], split[1], Long.parseLong(split[2]));
            }
        });

        //4.使用connect连接两条流
        ConnectedStreams<OrderEvent, TxEvent> connect = orderEventDStream.connect(txEventDStream);

        //5.将相同的交易码的数据聚和到一块
        ConnectedStreams<OrderEvent, TxEvent> connectedStreams = connect.keyBy("txId", "txId");

        //6.实时对账两条流的数据
        connectedStreams.process(new KeyedCoProcessFunction<String, OrderEvent, TxEvent, String>() {
            HashMap<String, OrderEvent> orderEventHashMap = new HashMap<>();
            HashMap<String, TxEvent> txEventHashMap = new HashMap<>();

            @Override
            public void processElement1(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
                //先判断交易表的Map集合中是否有能关联上的数据
                if (txEventHashMap.containsKey(value.getTxId())) {
                    //有能关联上的数据
                    out.collect("订单：" + value.getOrderId() + "对账成功！！！！");
                    //关联上的话则删除已经关联上的数据，因为后续不会再有数据能关联上了
                    txEventHashMap.remove(value.getTxId());
                } else {
                    //没有关联上，则将自己存入缓存
                    orderEventHashMap.put(value.getTxId(), value);
                }

            }

            @Override
            public void processElement2(TxEvent value, Context ctx, Collector<String> out) throws Exception {
                //先判断订单表的Map集合中是否有能关联上的数据
                if (orderEventHashMap.containsKey(value.getTxId())) {
                    //有能关联上的数据
                    out.collect("订单：" + orderEventHashMap.get(value.getTxId()).getOrderId() + "对账成功！！！！");
                    //关联上的话则删除已经关联上的数据，因为后续不会再有数据能关联上了
                    orderEventHashMap.remove(value.getTxId());
                } else {
                    //没有关联上，则将自己存入缓存
                    txEventHashMap.put(value.getTxId(), value);
                }

            }
        }).print();

        env.execute();
    }
}
