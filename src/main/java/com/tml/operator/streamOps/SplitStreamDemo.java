package com.tml.operator.streamOps;

import com.tml.common.AbsStreamCommonService;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 分流操作
 */
public class SplitStreamDemo extends AbsStreamCommonService<String> {

    public static void main(String[] args) throws Exception {
        new SplitStreamDemo().processStream(2);

    }

    @Override
    public void handle(DataStream<String> source) {
        SingleOutputStreamOperator<String> process = source.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
                if (value.startsWith("hello")) {
                    ctx.output(new OutputTag<>("hello tag", Types.STRING), value);
                } else {
                    out.collect(value);
                }
            }
        }, Types.STRING);

        process.print("主流：");
        process.getSideOutput(new OutputTag<>("hello tag", Types.STRING)).print("测流：");
    }

    @Override
    public DataStream<String> getSource(StreamExecutionEnvironment env) {
        return super.getSourceFromSocket(env);
    }
}
