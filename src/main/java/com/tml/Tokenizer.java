package com.tml;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


/**
 * 自定义 FlatMapFunction 实现单词拆分
 */
public class Tokenizer implements FlatMapFunction<String, Tuple2<String,Integer>> {
    @Override
    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
        //按照空格或者制表符分割单词
        String[] words = s.split("\\s+");
        for (String word : words) {
            collector.collect(new Tuple2<>(word, 1));
        }
    }
}
