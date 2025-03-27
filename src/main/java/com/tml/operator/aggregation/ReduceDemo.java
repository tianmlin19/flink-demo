package com.tml.operator.aggregation;

import com.tml.common.AbsStreamCommonService;
import com.tml.msg.CommonMsg;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReduceDemo extends AbsStreamCommonService<CommonMsg> {

    public static void main(String[] args) throws Exception {
        new ReduceDemo().processStream(1);

    }

    @Override
    public void handle(DataStream<CommonMsg> source) {
        KeyedStream<CommonMsg, String> stream = source.keyBy((KeySelector<CommonMsg, String>) CommonMsg::getId, TypeInformation.of(String.class));

        /**
         * reduce函数是非常灵活的，可以根据业务需求，非常灵活的进行聚合计算
         * 当每个分组中只有一条数据的时候，是不会进行reduce的，因为只有一条数据，没有比较的数据，进行reduce没有必要
         */
        SingleOutputStreamOperator<CommonMsg> reduce = stream.reduce((t1, t2) -> {
            System.out.println("t1==>" + t1);
            System.out.println("t2==>" + t2);
            CommonMsg commonMsg = new CommonMsg(t1.getId(), t2.getMsg(), t1.getTime() + t2.getTime());

            return commonMsg;
        });

        reduce.print();
    }

    @Override
    public DataStream<CommonMsg> getSource(StreamExecutionEnvironment env) {
        return super.getSourceFromCollection(env);
    }
}
