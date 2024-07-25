package org.apache.flink.cep.operator;


import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.cep.configuration.ObjectConfiguration;
import org.apache.flink.cep.context.RuleFunctionContext;
import org.apache.flink.cep.context.impl.RuleFunctionContextImpl;
import org.apache.flink.cep.context.impl.RuleFunctionOnTimerContextImpl;
import org.apache.flink.cep.event.EventRecord;
import org.apache.flink.cep.event.RuleUpdated;
import org.apache.flink.cep.event.RuleUpdatedEvent;
import org.apache.flink.cep.functions.AbstractRuleProcessFunction;
import org.apache.flink.cep.state.ManagedState;
import org.apache.flink.cep.types.RuleRowKey;
import org.apache.flink.cep.utils.JacksonUtils;
import org.apache.flink.cep.utils.UserClassLoaderUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.streaming.api.SimpleTimerService;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.OutputTag;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * 自定义函数规则处理算子，参考 {@link KeyedProcessOperator}
 *
 * @author shirukai
 */
public class UdfRuleProcessorOperator<KEY, IN, OUT> extends AbstractStreamOperator<OUT>
        implements OneInputStreamOperator<EventRecord<IN>, OUT>,
        Triggerable<RuleRowKey<KEY>, VoidNamespace>,
        OperatorEventHandler {
    private static final long serialVersionUID = 1L;
    private transient TimestampedCollector<OUT> collector;
    private transient ContextImpl context;
    private transient OnTimerContextImpl onTimerContext;
    private transient RuleFunctionContextImpl<KEY> ruleFunctionContext;
    private transient RuleFunctionOnTimerContextImpl<KEY> ruleFunctionOnTimerContext;
    private final Map<String, UdfRuleProcessor<KEY, IN, OUT>> processors;
    private final String userLibDir;
    private ListState<UdfRuleProcessor<KEY, IN, OUT>> processorListState;


    public UdfRuleProcessorOperator(ProcessingTimeService processingTimeService, String userLibDir) {
        this.processingTimeService = processingTimeService;
        this.userLibDir = userLibDir;
        chainingStrategy = ChainingStrategy.ALWAYS;
        processors = new HashMap<>();

    }


    @Override
    public void open() throws Exception {
        super.open();
        collector = new TimestampedCollector<>(output);
        InternalTimerService<VoidNamespace> internalTimerService =
                getInternalTimerService("user-timers", VoidNamespaceSerializer.INSTANCE, this);

        TimerService timerService = new SimpleTimerService(internalTimerService);

        context = new ContextImpl(timerService);
        onTimerContext = new OnTimerContextImpl(timerService);

        ValueState<ManagedState> managedState = new ValueStateWrapper<>();
        ruleFunctionContext = new RuleFunctionContextImpl<>(getRuntimeContext(), managedState);
        ruleFunctionOnTimerContext = new RuleFunctionOnTimerContextImpl<>(getRuntimeContext(), managedState);
    }

    @Override
    public void close() throws Exception {
        super.close();
        for (UdfRuleProcessor<KEY, IN, OUT> processor : processors.values()) {
            processor.close();
        }

    }

    @Override
    public void onEventTime(InternalTimer<RuleRowKey<KEY>, VoidNamespace> timer) throws Exception {
        collector.setAbsoluteTimestamp(timer.getTimestamp());
        invokeUserFunction(TimeDomain.EVENT_TIME, timer);
    }

    @Override
    public void onProcessingTime(InternalTimer<RuleRowKey<KEY>, VoidNamespace> timer) throws Exception {
        collector.eraseTimestamp();
        invokeUserFunction(TimeDomain.PROCESSING_TIME, timer);
    }


    @Override
    public void processElement(StreamRecord<EventRecord<IN>> element) throws Exception {
        collector.setTimestamp(element);
        context.element = element;
        EventRecord<IN> eventRecord = element.getValue();
        final String ruleId = eventRecord.getRuleId();
        if (processors.containsKey(ruleId)) {
            UdfRuleProcessor<KEY, IN, OUT> processor = processors.get(ruleId);
            ruleFunctionContext.setContext(context);
            processor.process(element.getValue().getEvent(), ruleFunctionContext, collector);
        }
        context.element = null;
    }

    private void invokeUserFunction(TimeDomain timeDomain, InternalTimer<RuleRowKey<KEY>, VoidNamespace> timer)
            throws Exception {
        onTimerContext.timeDomain = timeDomain;
        onTimerContext.timer = timer;
        RuleRowKey<KEY> rowKey = timer.getKey();
        String ruleId = rowKey.getRuleKey();
        if (processors.containsKey(ruleId)) {
            ruleFunctionOnTimerContext.setContext(onTimerContext);
            processors.get(ruleId).onTimer(timer.getTimestamp(), ruleFunctionOnTimerContext, collector);
        }
        onTimerContext.timeDomain = null;
        onTimerContext.timer = null;
    }

    @Override
    public void handleOperatorEvent(OperatorEvent evt) {
        RuleUpdatedEvent updatedEvent = (RuleUpdatedEvent) evt;
        List<RuleUpdated> updates = updatedEvent.getUpdates();

        List<String> newIds = new ArrayList<>(updates.size());
        for (RuleUpdated ruleUpdated : updates) {
            newIds.add(ruleUpdated.getId());
            if (processors.containsKey(ruleUpdated.getId())) {
                processors.get(ruleUpdated.getId()).update(ruleUpdated);
            } else {
                UdfRuleProcessor<KEY, IN, OUT> processor = new UdfRuleProcessor<>();
                processor.rule = ruleUpdated;
                processor.setup(getExecutionConfig(), userLibDir, getUserCodeClassloader());
                processors.put(ruleUpdated.getId(), processor);
            }
        }
        // 清理无用的规则状态
        Iterator<Map.Entry<String, UdfRuleProcessor<KEY, IN, OUT>>> iterator = processors.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, UdfRuleProcessor<KEY, IN, OUT>> entry = iterator.next();
            if (!newIds.contains(entry.getKey())) {
                entry.getValue().close();
                iterator.remove();
            }
        }

    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        processorListState.clear();
        for (UdfRuleProcessor<KEY, IN, OUT> processor : processors.values()) {
            processor.snapshotState();
        }
        processorListState.addAll(new ArrayList<>(processors.values()));

    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        ListStateDescriptor<UdfRuleProcessor<KEY, IN, OUT>> processorListStateDescriptor = new ListStateDescriptor<>("processors",
                new TypeHint<UdfRuleProcessor<KEY, IN, OUT>>() {
                }.getTypeInfo());
        processorListState = context.getOperatorStateStore()
                .getListState(processorListStateDescriptor);

        Iterable<UdfRuleProcessor<KEY, IN, OUT>> iterable = processorListState.get();

        if (iterable != null) {
            for (UdfRuleProcessor<KEY, IN, OUT> processor : iterable) {
                processor.restoreState(getExecutionConfig(), userLibDir, getUserCodeClassloader());
                processors.put(processor.rule.getId(), processor);
            }
        }
    }

    public static class ValueStateWrapper<T> implements ValueState<T> {

        private T v;

        @Override
        public T value() throws IOException {
            return v;
        }

        @Override
        public void update(T value) throws IOException {
            this.v = value;
        }

        @Override
        public void clear() {
            this.v = null;
        }
    }

    @Data
    public static class UdfRuleProcessor<KEY, IN, OUT> implements Serializable {
        @Getter
        @Setter
        private RuleUpdated rule;
        private transient KryoSerializer<Map<KEY, ManagedState>> kryoSerializer;

        private transient AbstractRuleProcessFunction<KEY, IN, OUT> function;

        private transient ClassLoader classLoader;

        private transient Map<KEY, ManagedState> managedStateMap;

        @Getter
        @Setter
        private byte[] serialized;

        public UdfRuleProcessor() {
        }


        @SuppressWarnings("unchecked")
        public void setup(ExecutionConfig config, String userLibDir, ClassLoader parent) {
            if (managedStateMap == null) {
                managedStateMap = new HashMap<>();
            }
            if (classLoader == null) {
                classLoader = UserClassLoaderUtils.getClassLoader(userLibDir, rule.getLibs(), rule.getVersion(), parent);
            }
            if (kryoSerializer == null) {
                kryoSerializer = new KryoSerializer<>(new TypeHint<Map<KEY, ManagedState>>() {
                }.getTypeInfo().getTypeClass(), config);
                kryoSerializer.getKryo().setClassLoader(classLoader);
            }

            if (function == null) {
                try {
                    Map<String, Object> parameters = JacksonUtils.getObjectMapper().readValue(rule.getParameters(), new TypeReference<Map<String, Object>>() {
                    });
                    Configuration configuration = ObjectConfiguration.of(parameters);
                    function = (AbstractRuleProcessFunction<KEY, IN, OUT>) classLoader.loadClass(rule.getFunction()).getConstructor()
                            .newInstance();
                    function.open(configuration);
                } catch (Exception e) {
                    throw new FlinkRuntimeException("Failed to create process function.", e);
                }
            }
        }

        public void process(IN value, RuleFunctionContextImpl<KEY> ctx, Collector<OUT> out) throws Exception {
            ManagedState managedState = managedStateMap.getOrDefault(ctx.getCurrentKey(), new ManagedState());
            ctx.getManagedState().update(managedState);
            this.function.process(value, ctx, out);
            managedState = ctx.getManagedState().value();
            ctx.getManagedState().update(null);
            managedStateMap.put(ctx.getCurrentKey(), managedState);
        }

        public void onTimer(long timestamp, RuleFunctionOnTimerContextImpl<KEY> ctx, Collector<OUT> out) throws Exception {
            ManagedState managedState = managedStateMap.getOrDefault(ctx.getCurrentKey(), new ManagedState());
            ctx.getManagedState().update(managedState);
            this.function.onTimer(timestamp, ctx, out);
            managedState = ctx.getManagedState().value();
            ctx.getManagedState().update(null);
            managedStateMap.put(ctx.getCurrentKey(), managedState);
        }

        public void update(RuleUpdated newRule) {
            // 判断是否过期
            if (newRule.getVersion() > rule.getVersion()) {
                close();
                this.rule = newRule;
            }
        }

        public void restoreState(ExecutionConfig config, String userLibDir, ClassLoader parent) throws IOException {
            this.setup(config, userLibDir, parent);
            if (serialized != null) {
                ByteArrayInputStream bio = new ByteArrayInputStream(serialized);
                DataInputView in = new DataInputViewStreamWrapper(bio);
                managedStateMap = kryoSerializer.deserialize(in);
            }
        }

        public void snapshotState() throws IOException {
            if (managedStateMap != null) {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                DataOutputView out = new DataOutputViewStreamWrapper(bos);
                kryoSerializer.serialize(managedStateMap, out);
                serialized = bos.toByteArray();
            }
        }


        public void close() {
            classLoader = null;
            managedStateMap = null;
            serialized = null;
            if (function != null) {
                try {
                    function.close();
                } catch (Exception e) {
                    LOG.error("Failed to close function: ", e);
                } finally {
                    function = null;
                }

            }
        }

    }


    public class ContextImpl extends RuleFunctionContext.Context<KEY> {
        private final TimerService timerService;

        private StreamRecord<EventRecord<IN>> element;

        public ContextImpl(TimerService timerService) {
            this.timerService = checkNotNull(timerService);
        }


        @Override
        public Long timestamp() {
            checkState(element != null);

            if (element.hasTimestamp()) {
                return element.getTimestamp();
            } else {
                return null;
            }
        }

        @Override
        public TimerService timerService() {
            return timerService;
        }

        @Override
        public <X> void output(OutputTag<X> outputTag, X value) {
            if (outputTag == null) {
                throw new IllegalArgumentException("OutputTag must not be null.");
            }

            output.collect(outputTag, new StreamRecord<>(value, element.getTimestamp()));
        }

        @Override
        @SuppressWarnings("unchecked")
        public KEY getCurrentKey() {
            return ((RuleRowKey<KEY>) UdfRuleProcessorOperator.this.getCurrentKey()).getUserKey();
        }

        @Override
        @SuppressWarnings("unchecked")
        public String getCurrentRuleKey() {
            return ((RuleRowKey<KEY>) UdfRuleProcessorOperator.this.getCurrentKey()).getRuleKey();
        }

    }

    public class OnTimerContextImpl extends RuleFunctionContext.OnTimerContext<KEY> {
        private final TimerService timerService;
        private TimeDomain timeDomain;
        private InternalTimer<RuleRowKey<KEY>, VoidNamespace> timer;

        OnTimerContextImpl(TimerService timerService) {
            this.timerService = checkNotNull(timerService);
        }

        @Override
        public Long timestamp() {
            checkState(timer != null);
            return timer.getTimestamp();
        }

        @Override
        public TimerService timerService() {
            return timerService;
        }

        @Override
        public <X> void output(OutputTag<X> outputTag, X value) {
            if (outputTag == null) {
                throw new IllegalArgumentException("OutputTag must not be null.");
            }

            output.collect(outputTag, new StreamRecord<>(value, timer.getTimestamp()));
        }

        @Override
        public TimeDomain timeDomain() {
            checkState(timeDomain != null);
            return timeDomain;
        }

        @Override
        public KEY getCurrentKey() {
            return timer.getKey().getUserKey();
        }

        @Override
        public String getCurrentRuleKey() {
            return timer.getKey().getRuleKey();
        }
    }

}
