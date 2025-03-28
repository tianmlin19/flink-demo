package com.tml.window;

import com.tml.TokenMap;
import com.tml.common.AbsStreamCommonService;
import com.tml.msg.CommonMsg;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 增量窗口聚合函数
 * 来一条数据处理一条数据，优点是只用少量的数据存储中间结果
 * 不足是不能获取上下文信息
 */
public class WindowAggregateDemo extends AbsStreamCommonService<String> {

    public static void main(String[] args) throws Exception {
        new WindowAggregateDemo().processStream(2);

    }
    @Override
    public void handle(DataStream<String> source) {


        SingleOutputStreamOperator<CommonMsg> map = source.map(new TokenMap());
        KeyedStream<CommonMsg, String> keyedStream = map.keyBy(CommonMsg::getId);
        //定义一个30秒的固定窗口
        WindowedStream<CommonMsg, String, TimeWindow> window = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(30)));
        SingleOutputStreamOperator<String> aggregate = window.aggregate(new MyAggFunction());
        aggregate.print("窗口结果=>");

    }

    @Override
    public DataStream<String> getSource(StreamExecutionEnvironment env) {
        return super.getSourceFromSocket(env);
    }
}
