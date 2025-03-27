package com.tml.common;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public interface StreamService<T> {

    StreamExecutionEnvironment getEnv();

    DataStream<T>  getSource(StreamExecutionEnvironment env);
}
