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

public class WindowReduceDemo extends AbsStreamCommonService<String> {

    public static void main(String[] args) throws Exception {
        new WindowReduceDemo().processStream(2);

    }
    @Override
    public void handle(DataStream<String> source) {


        SingleOutputStreamOperator<CommonMsg> map = source.map(new TokenMap());
        KeyedStream<CommonMsg, String> keyedStream = map.keyBy(CommonMsg::getId);
        //定义一个10秒的固定窗口
        WindowedStream<CommonMsg, String, TimeWindow> window = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        SingleOutputStreamOperator<CommonMsg> reduce = window.reduce((t1, t2) -> {
            System.out.println("t1==>" + t1 + "  t2==>" + t2);
            return new CommonMsg(t1.getId(), t2.getMsg(), t1.getTime() + t2.getTime());
        });
        reduce.print("窗口结果=>");

    }

    @Override
    public DataStream<String> getSource(StreamExecutionEnvironment env) {
        return super.getSourceFromSocket(env);
    }
}
