package org.apache.flink.cep.context.impl;


import lombok.Getter;
import lombok.Setter;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.cep.context.RuleFunctionContext;
import org.apache.flink.cep.state.ManagedState;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.util.Optional;

/**
 * 规则函数上下文
 *
 * @author shirukai
 */
public class RuleFunctionContextImpl<KEY> implements RuleFunctionContext<KEY> {

    private final transient RuntimeContext runtimeContext;
    @Setter
    private transient Context<KEY> context;
    @Getter
    private final ValueState<ManagedState> managedState;

    private ManagedState state;

    public RuleFunctionContextImpl(RuntimeContext runtimeContext, ValueState<ManagedState> managedState) {
        this.runtimeContext = runtimeContext;
        this.managedState = managedState;
    }

    @Override
    public RuntimeContext getRuntimeContext() {
        return runtimeContext;
    }

    @Override
    public Long timestamp() {
        return context.timestamp();
    }

    @Override
    public TimerService timerService() {
        return context.timerService();
    }

    @Override
    public <X> void output(OutputTag<X> outputTag, X value) {
        context.output(outputTag, value);
    }

    @Override
    public KEY getCurrentKey() {
        return context.getCurrentKey();
    }

    @Override
    public String getCurrentRuleKey() {
        return context.getCurrentRuleKey();
    }

    @Override
    public <S> Optional<S> getState() throws IOException {
        state = Optional.ofNullable(managedState.value()).orElse(new ManagedState());
        return Optional.ofNullable(state.getUserState());
    }

    @Override
    public void updateState(Object userState) throws IOException {
        Optional.ofNullable(state).orElse(new ManagedState()).setUserState(userState);
        this.managedState.update(state);
    }
}
