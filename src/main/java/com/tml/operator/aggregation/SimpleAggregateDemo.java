package com.tml.operator.aggregation;

import com.tml.common.AbsStreamCommonService;
import com.tml.msg.CommonMsg;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class SimpleAggregateDemo extends AbsStreamCommonService<CommonMsg> {

    public static void main(String[] args) throws Exception {
        new SimpleAggregateDemo().processStream(1);
    }


    @Override
    public void handle(DataStream<CommonMsg> stream) {
        KeyedStream<CommonMsg, String> keyStream = stream.keyBy((KeySelector<CommonMsg, String>) CommonMsg::getId, TypeInformation.of(String.class));
        //使用sum聚合
        //SingleOutputStreamOperator<CommonMsg> time = stream.sum("time");
        //SingleOutputStreamOperator<CommonMsg> min = stream.min("time");
        /**
         * max、maxyBy的区别在于
         * max不会对非比较字段重新赋值，而maxBy会更新非比较字段的值
         */
        SingleOutputStreamOperator<CommonMsg> minBy = keyStream.minBy("time");
        //min.print();
        minBy.print();
    }

    @Override
    public DataStream<CommonMsg> getSource(StreamExecutionEnvironment env) {
        return super.getSourceFromCollection(env);
    }
}
