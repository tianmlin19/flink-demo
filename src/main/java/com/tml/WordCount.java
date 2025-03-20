package com.tml;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WordCount {

    public static void main(String[] args) throws Exception {

        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //从本地读取文件
        DataStreamSource<String> stringDataStreamSource = env.readTextFile("input/word.txt");
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = stringDataStreamSource.
                flatMap(new Tokenizer()).
                keyBy(t -> t.f0).
                sum(1);


        sum.print();
        env.execute();
    }
}
