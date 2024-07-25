package org.apache.flink.cep.functions;

import org.apache.flink.cep.context.RuleFunctionContext;
import org.apache.flink.cep.context.RuleFunctionOnTimerContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.util.Collector;

import java.io.Serializable;

/**
 * 规则处理函数
 *
 * @author shirukai
 */
public abstract class AbstractRuleProcessFunction<KEY, IN, OUT> implements Serializable {

    public  void open(Configuration configuration) throws Exception{}

    /**
     * 处理事件
     *
     * @param value 输入数据
     * @param ctx   函数上下文
     *              1. 通过ctx.getCurrentKey()获取当前key
     *              2. 通过ctx.timerService()获取定时器服务，并可以通过timerService.registerProcessingTimeTimer(timestamp)注册定时器
     *              以及其它定时器操作，定时器的操作可以参考{@link TimerService} API
     *              3. 通过ctx.output(new OutputTag<Row>("side-output"), value)发送侧输出
     *              4. 通过ctx.getState()获取用户自定义状态
     *              5. 通过ctx.updateState(S s)更新用户状态
     * @param out   输出数据收集器
     * @throws Exception e
     */
    public abstract void process(IN value, RuleFunctionContext<KEY> ctx, Collector<OUT> out) throws Exception;

    /**
     * 定时器处理逻辑
     *
     * @param timestamp 触发时间戳
     * @param ctx       定时器上下文
     * @param out       输出数据收集器
     * @throws Exception e
     */
    public void onTimer(long timestamp, RuleFunctionOnTimerContext<KEY> ctx, Collector<OUT> out) throws Exception {
    }

    public void close()throws Exception{}
}
