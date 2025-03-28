package com.tml.window;

import com.tml.TokenMap;
import com.tml.common.AbsStreamCommonService;
import com.tml.msg.CommonMsg;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;


/**
 * 可以使用增量聚合窗口函数+全窗口process函数
 * 增量计算完成后，会将结果传递给全窗口计算函数
 */
public class WindowAggAndProcessDemo extends AbsStreamCommonService<String> {
    public static void main(String[] args) throws Exception {
        new WindowAggAndProcessDemo().processStream(2);

    }

    @Override
    public void handle(DataStream<String> source) {


        SingleOutputStreamOperator<CommonMsg> map = source.map(new TokenMap());
        KeyedStream<CommonMsg, String> keyedStream = map.keyBy(CommonMsg::getId);
        WindowedStream<CommonMsg, String, TimeWindow> window = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(20)));
        SingleOutputStreamOperator<String> aggregate = window.aggregate(new MyAggFunction()
                , new MyProcessFunction());

        aggregate.print("窗口结果=>");

    }

    @Override
    public DataStream<String> getSource(StreamExecutionEnvironment env) {
        return super.getSourceFromSocket(env);
    }


}
