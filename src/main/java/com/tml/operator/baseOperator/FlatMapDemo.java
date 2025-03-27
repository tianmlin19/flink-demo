package com.tml.operator.baseOperator;

import com.tml.common.AbsStreamCommonService;
import com.tml.msg.CommonMsg;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * flatMap和Map的区别在于map是一对一的，而flatMap是一进多出
 */
public class FlatMapDemo extends AbsStreamCommonService<CommonMsg> {
    public static void main(String[] args) throws Exception {
        new FlatMapDemo().processStream(1);
    }

    @Override
    public void handle(DataStream<CommonMsg> source) {
        source.flatMap((FlatMapFunction<CommonMsg, String>) (value, out) -> {
            out.collect(value.getMsg());
            out.collect("time:" + value.getTime());
        }, TypeInformation.of(String.class)).print();
    }

    @Override
    public DataStream<CommonMsg> getSource(StreamExecutionEnvironment env) {
        return super.getSourceFromCollection(env);
    }
}
