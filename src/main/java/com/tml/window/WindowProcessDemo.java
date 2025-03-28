package com.tml.window;

import com.tml.common.AbsStreamCommonService;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 全窗口处理函数
 * 来一条数据先用迭代器存储起来，到了窗口的结束时间后，在统一计算输出
 * 优点是可以获取到上下文信息，缺点是需要存储的中间数据会比较多，占用过多的存储空间
 */
public class WindowProcessDemo extends AbsStreamCommonService<String> {

    public static void main(String[] args) throws Exception {
        new WindowProcessDemo().processStream(2);

    }
    @Override
    public void handle(DataStream<String> source) {
        KeyedStream<String, String> keyBy = source.keyBy(String::toUpperCase);
        //使用的是滚动窗口
        //WindowedStream<String, String, TimeWindow> window = keyBy.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        //使用的滑动窗口，滑动窗口需要使用两个参数来构建，第一个参数是窗口的步长，第二个参数是两个窗口之间的间隔，比如求最近一个小时内出现的次数【每五分钟输出一次】这种情况下就需要使用滑动窗口了
        //WindowedStream<String, String, TimeWindow> window = keyBy.window(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(5)));


        //会话窗口，支持静态窗口和动态窗口，静态窗口的意思是，等了固定时间后，没有数据过来，就结束上一个窗口，开启下一个窗口
        //动态窗口指的是，等待的时间间隔是可以根据传递来的数据进行动态的调整的
        WindowedStream<String, String, TimeWindow> window = keyBy.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)));
        SingleOutputStreamOperator<String> process = window.process(new MyProcessFunction());
        process.print();
    }

    @Override
    public DataStream<String> getSource(StreamExecutionEnvironment env) {
        return super.getSourceFromSocket(env);
    }
}
