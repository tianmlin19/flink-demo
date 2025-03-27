package com.tml.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataGenerateDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataGeneratorSource dataGeneratorSource = new DataGeneratorSource(o -> "data:"+o,Long.MAX_VALUE, RateLimiterStrategy.perSecond(10), Types.STRING);
        env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "dataGeneratorSource").print();
        env.execute();
    }
}
