package org.apache.flink.cep.context;

import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.util.Optional;

public interface RuleFunctionContext<KEY> extends FunctionContext {
    /**
     * 获取正在处理的元素的Key
     */
    KEY getCurrentKey();

    /**
     * 获取正在处理的规则KEY
     */
    String getCurrentRuleKey();

    /**
     * 获取当前Key的自定义状态
     */
    <S> Optional<S> getState() throws IOException;

    /**
     * 更新当前Key的自定义状态，注意用户自定义状态需要是可序列化的
     */
    void updateState(Object userState) throws IOException;

    abstract class Context<KEY> {
        /**
         * Timestamp of the element currently being processed or timestamp of a firing timer.
         *
         * <p>This might be {@code null}, for example if the time characteristic of your program is
         * set to {@link org.apache.flink.streaming.api.TimeCharacteristic#ProcessingTime}.
         */
        public abstract Long timestamp();

        /**
         * A {@link TimerService} for querying time and registering timers.
         */
        public abstract TimerService timerService();

        /**
         * Emits a record to the side output identified by the {@link OutputTag}.
         *
         * @param outputTag the {@code OutputTag} that identifies the side output to emit to.
         * @param value     The record to emit.
         */
        public abstract <X> void output(OutputTag<X> outputTag, X value);

        /**
         * Get key of the element being processed.
         */
        public abstract KEY getCurrentKey();

        public abstract String getCurrentRuleKey();
    }

    abstract class OnTimerContext<KEY> extends Context<KEY> {
        /**
         * The {@link TimeDomain} of the firing timer.
         */
        public abstract TimeDomain timeDomain();

        /**
         * Get key of the firing timer.
         */
        @Override
        public abstract KEY getCurrentKey();
    }
}
