package com.tml.window;

import com.tml.msg.CommonMsg;
import org.apache.flink.api.common.functions.AggregateFunction;

public class MyAggFunction implements AggregateFunction<CommonMsg, Long, String> {
    /**
     * 创建初始化器
     */
    @Override
    public Long createAccumulator() {
        System.out.println("创建窗口的计算初始化器createAccumulator");
        return 0L;
    }

    /**
     * 计算逻辑
     *
     * @param value       The value to add
     * @param accumulator The accumulator to add the value to
     * @return
     */
    @Override
    public Long add(CommonMsg value, Long accumulator) {
        System.out.println("窗口期执行add累加" + value);
        return accumulator + value.getTime();
    }

    /**
     * 窗口结束后返回结果
     *
     * @param accumulator The accumulator of the aggregation
     * @return
     */
    @Override
    public String getResult(Long accumulator) {
        return "result:" + accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return 0L;
    }
}
