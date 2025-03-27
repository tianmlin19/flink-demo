package com.tml;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 从socket流读取数据
 */
public class WordCountFromSocket {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setString(RestOptions.BIND_PORT, "8081");  // 设置端口
        configuration.setString(RestOptions.BIND_ADDRESS, "localhost");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);
        DataStream<String> socketTextStream = env.socketTextStream("43.139.114.233", 9999);
        //数据流处理
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = socketTextStream.flatMap(new Tokenizer()).keyBy(t -> t.f0).sum(1);
        sum.print();
        env.execute("Socket Stream WordCount~");
    }
}
