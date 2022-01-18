package com.atguigu.day07;

import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

public class Flink01_StateBackend {
    public static void main(String[] args) throws IOException {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //1.12版本
        env.setStateBackend(new MemoryStateBackend());
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/ck/...."));

        env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop102:8020/ck/...."));

        //1.13版本
        //内存级别
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());

        //文件级别
        env.setStateBackend(new HashMapStateBackend());

        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("hdfs://hadoop102:8020/ck/...."));

        //RocksDB
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("hdfs://hadoop102:8020/ck/rocksDb/...."));

        //barrier不对齐
        env.getCheckpointConfig().enableUnalignedCheckpoints();
    }
}
