package com.tml.common;

import com.tml.msg.CommonMsg;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public abstract class AbsStreamCommonService<T> implements StreamService<T> {


    public void processStream(Integer parallelism) throws Exception {
        StreamExecutionEnvironment env = getEnv();
        env.setParallelism(parallelism);
        DataStream<T> stream = getSource(env);
        handle(stream);
        env.execute();
    }

    public abstract void handle(DataStream<T> source);

    @Override
    public StreamExecutionEnvironment getEnv() {

        return StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
    }

    public DataStream<String> getSourceFromSocket(StreamExecutionEnvironment environment) {
        return environment.socketTextStream("43.139.114.233", 9999);
    }

    public DataStream<CommonMsg> getSourceFromCollection(StreamExecutionEnvironment environment) {
        DataStreamSource<CommonMsg> source = environment.fromElements(
                new CommonMsg("11", "hello world", 11L),
                new CommonMsg("11", "hello flink", 3L),
                new CommonMsg("12", "hello kitty", 13L),
                new CommonMsg("13", "hello world", 12L),
                new CommonMsg("11", "hello java", 23L));

        return source;
    }

    public DataStream<Long> getSourceFromDataGenerator(StreamExecutionEnvironment environment) {
        DataGeneratorSource<Long> dataGeneratorSource =
                new DataGeneratorSource<>((GeneratorFunction<Long, Long>) o -> o, 100000L,RateLimiterStrategy.perSecond(2), Types.LONG);
        return environment.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "dataGeneratorSource", Types.LONG);
    }


}
