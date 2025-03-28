package com.tml.window;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MyProcessFunction extends ProcessWindowFunction<String, String, String, TimeWindow> {
    /**
     *
     * @param s The key for which this window is evaluated. 分组的key
     * @param context The context in which the window is being evaluated. 上下文信息
     * @param elements The elements in the window being evaluated. 存放的数据
     * @param out A collector for emitting elements.输出
     * @throws Exception
     */
    @Override
    public void process(String s, ProcessWindowFunction<String, String, String, TimeWindow>.Context context, Iterable<String> elements, Collector<String> out) throws Exception {
        String startTime = DateFormatUtils.format(context.window().getStart(), "yyyy-MM-dd HH:mm:ss.SSS");
        String endTime = DateFormatUtils.format(context.window().getEnd(), "yyyy-MM-dd HH:mm:ss.SSS");
        long l = elements.spliterator().estimateSize();
        System.out.println("分组key：" + s + " 窗口【" + startTime + "----" + endTime + "】 的数据条数有：" + l + elements);
        out.collect("窗口" + s + "数量" + l);
    }
}
