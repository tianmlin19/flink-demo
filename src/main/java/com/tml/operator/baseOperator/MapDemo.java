package com.tml.operator.baseOperator;

import com.tml.common.AbsStreamCommonService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MapDemo extends AbsStreamCommonService<Long> {

    public static void main(String[] args) throws Exception {
        new MapDemo().processStream(1);
    }

    @Override
    public void handle(DataStream<Long> source) {
        source.map(t -> "hello " + t).print();
    }

    @Override
    public DataStream<Long> getSource(StreamExecutionEnvironment env) {
        return super.getSourceFromDataGenerator(env);
    }
}
