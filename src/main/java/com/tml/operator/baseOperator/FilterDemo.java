package com.tml.operator.baseOperator;

import com.tml.common.AbsStreamCommonService;
import com.tml.msg.CommonMsg;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FilterDemo extends AbsStreamCommonService<CommonMsg> {


    public static void main(String[] args) throws Exception {
        new FilterDemo().processStream(2);
    }

    @Override
    public void handle(DataStream<CommonMsg> source) {
        source.filter(t -> StringUtils.equals("11", t.getId())).map(CommonMsg::getMsg).print();
    }

    @Override
    public DataStream<CommonMsg> getSource(StreamExecutionEnvironment env) {
        return super.getSourceFromCollection(env);
    }
}
