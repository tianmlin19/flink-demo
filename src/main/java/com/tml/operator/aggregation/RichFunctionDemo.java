package com.tml.operator.aggregation;

import com.tml.common.AbsStreamCommonService;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * richfunction添加了一些额外的功能
 * 提供了一些生命周期的管理方法，比如open()\close()
 * open() 在每个子任务启动的时候调用一次
 * close() 在每个任务结束的时候调用一次，如果是flink程序挂掉，不会调用这个close方法，在控制台上点击cancel任务，这个close方法也是可以额正常调用的
 *
 * 另外多了一些运行时上下文，可以通过getRuntimeContext() 来获取上下文中的一些关键信息
 * 在close方法中可以做一些释放资源的操作，回调通知操作等一些hook函数
 */
public class RichFunctionDemo extends AbsStreamCommonService<String> {
    public static void main(String[] args) throws Exception {
        new RichFunctionDemo().processStream(1);
    }

    @Override
    public void handle(DataStream<String> stream) {
        SingleOutputStreamOperator<String> map = stream.map(new RichMapFunction<String, String>() {

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                RuntimeContext context = getRuntimeContext();
                String taskName = context.getTaskName();
                int subtasks = context.getNumberOfParallelSubtasks();
                System.out.println("taskName: " + taskName + ", subtasks: " + subtasks + " call open()");
            }

            @Override
            public void close() throws Exception {
                super.close();
                RuntimeContext context = getRuntimeContext();
                String taskName = context.getTaskName();
                int subtasks = context.getNumberOfParallelSubtasks();
                System.out.println("taskName: " + taskName + ", subtasks: " + subtasks + " call close()");
            }

            @Override
            public String map(String value) throws Exception {
                return "(" + value + ")";
            }
        }, TypeInformation.of(String.class));

        map.print();
    }

    @Override
    public DataStream<String> getSource(StreamExecutionEnvironment env) {
        return super.getSourceFromSocket(env);
    }
}
