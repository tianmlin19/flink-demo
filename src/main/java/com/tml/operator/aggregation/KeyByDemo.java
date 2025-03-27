package com.tml.operator.aggregation;

import com.tml.common.AbsStreamCommonService;
import com.tml.msg.CommonMsg;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class KeyByDemo extends AbsStreamCommonService<CommonMsg> {

    public static void main(String[] args) throws Exception {
        new KeyByDemo().processStream(4);
    }

    @Override
    public void handle(DataStream<CommonMsg> stream) {
        /**
         * keyby算子返回的是一个keyedStream
         * 1.keyby不是一个转换算子，只对数据进行了重分区，另外还不能设置并行度
         * 2.keyby分组和分区的概念
         *  keyby是对数据进行分组，保证同一个分组的数据会落到同一个数据分区内
         *  分区：一个子任务可以理解为一个分区，一个分区可以包含有多个分组的数据
         */
        KeyedStream<CommonMsg, String> keyBy = stream.keyBy((KeySelector<CommonMsg, String>) CommonMsg::getId, TypeInformation.of(String.class));
        keyBy.print();
    }


    @Override
    public DataStream<CommonMsg> getSource(StreamExecutionEnvironment env) {
        return super.getSourceFromCollection(env);
    }
}
