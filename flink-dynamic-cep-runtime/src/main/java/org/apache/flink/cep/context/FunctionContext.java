package org.apache.flink.cep.context;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.util.OutputTag;

/**
 * 处理函数上下文
 *
 * @author shirukai
 */
public interface FunctionContext {
    /**
     * RuntimeContext 包含有关在其中执行函数的上下文的信息。函数的每个并行实例都有一个上下文，通过该上下文，
     * 它可以访问静态上下文信息（例如当前并行性）和其他结构（如累加器和广播变量）
     */
    RuntimeContext getRuntimeContext();

    /**
     * 当前正在处理的元素的时间戳或触发计时器的时间戳
     *
     * <p>这可能是 {@code null}，例如，如果程序的时间特征设置为处理时间
     */
    Long timestamp();

    /**
     * 用于查询时间和注册计时器的服务{@link TimerService}。
     */
    TimerService timerService();

    /**
     * 向 {@link OutputTag} 标识的旁路输出发出一条记录
     *
     * @param outputTag {@code OutputTag}，要发出的旁路输出标识
     * @param value     要发出的记录
     */
    <X> void output(OutputTag<X> outputTag, X value);


}
