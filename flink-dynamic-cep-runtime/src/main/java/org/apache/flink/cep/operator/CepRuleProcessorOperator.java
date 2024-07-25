package org.apache.flink.cep.operator;

import lombok.Getter;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.EventComparator;
import org.apache.flink.cep.configuration.ObjectConfiguration;
import org.apache.flink.cep.configuration.SharedBufferCacheConfig;
import org.apache.flink.cep.dynamic.impl.json.util.CepJsonUtils;
import org.apache.flink.cep.event.EventRecord;
import org.apache.flink.cep.event.RuleUpdated;
import org.apache.flink.cep.event.RuleUpdatedEvent;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.NFAState;
import org.apache.flink.cep.nfa.NFAStateSerializer;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBuffer;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBufferAccessor;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.time.TimerService;
import org.apache.flink.cep.types.RuleRowKey;
import org.apache.flink.cep.utils.JacksonUtils;
import org.apache.flink.cep.utils.UserClassLoaderUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Stream;

/**
 * Cep规则处理算子，参考{@link CepOperator}
 *
 * @author shirukai
 */
public class CepRuleProcessorOperator<IN, OUT> extends AbstractStreamOperator<OUT>
        implements OneInputStreamOperator<EventRecord<IN>, OUT>,
        Triggerable<RuleRowKey<?>, VoidNamespace>,
        OperatorEventHandler {


    private static final long serialVersionUID = -4166778210774160757L;

    private static final String LATE_ELEMENTS_DROPPED_METRIC_NAME = "numLateRecordsDropped";

    private final boolean isProcessingTime;

    private final TypeSerializer<IN> inputSerializer;

    ///////////////			State			//////////////

    private static final String NFA_STATE_NAME = "nfaStateName";
    private static final String EVENT_QUEUE_STATE_NAME = "eventQueuesStateName";


    private transient ValueState<NFAState> computationStates;
    private transient MapState<Long, List<IN>> elementQueueState;
    private transient InternalTimerService<VoidNamespace> timerService;


    /**
     * Comparator for secondary sorting. Primary sorting is always done on time.
     */
    private final EventComparator<IN> comparator;

    /**
     * {@link OutputTag} to use for late arriving events. Elements with timestamp smaller than the
     * current watermark will be emitted to this.
     */
    private final OutputTag<EventRecord<IN>> lateDataOutputTag;


    /**
     * Context passed to user function.
     */
    private transient ContextFunctionImpl context;

    /**
     * Main output collector, that sets a proper timestamp to the StreamRecord.
     */
    private transient TimestampedCollector<OUT> collector;

    /**
     * Wrapped RuntimeContext that limits the underlying context features.
     */
    private transient CepRuntimeContext cepRuntimeContext;

    /**
     * Thin context passed to NFA that gives access to time related characteristics.
     */
    private transient TimerService cepTimerService;

    // ------------------------------------------------------------------------
    // Metrics
    // ------------------------------------------------------------------------

    private transient Counter numLateRecordsDropped;

    private final String userLibDir;
    private StateInitializationContext stateInitializationContext;

    private transient Map<String, CepRuleProcessor> processors;


    public CepRuleProcessorOperator(
            ProcessingTimeService processingTimeService,
            final TypeSerializer<IN> inputSerializer,
            final boolean isProcessingTime,
            @Nullable final EventComparator<IN> comparator,
            @Nullable final OutputTag<EventRecord<IN>> lateDataOutputTag,
            String userLibDir
    ) {
        this.processingTimeService = processingTimeService;
        this.inputSerializer = Preconditions.checkNotNull(inputSerializer);

        this.isProcessingTime = isProcessingTime;
        this.comparator = comparator;
        this.lateDataOutputTag = lateDataOutputTag;
        this.userLibDir = userLibDir;
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<OUT>> output) {
        super.setup(containingTask, config, output);
        this.cepRuntimeContext = new CepRuntimeContext(getRuntimeContext());
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);


        // initializeState through the provided context
        computationStates =
                context.getKeyedStateStore()
                        .getState(
                                new ValueStateDescriptor<>(
                                        NFA_STATE_NAME, new NFAStateSerializer()));
        elementQueueState =
                context.getKeyedStateStore()
                        .getMapState(
                                new MapStateDescriptor<>(
                                        EVENT_QUEUE_STATE_NAME,
                                        LongSerializer.INSTANCE,
                                        new ListSerializer<>(inputSerializer)));

        stateInitializationContext = context;
    }

    @Override
    public void open() throws Exception {
        super.open();
        timerService =
                getInternalTimerService(
                        "watermark-callbacks", VoidNamespaceSerializer.INSTANCE, this);
        context = new ContextFunctionImpl();
        collector = new TimestampedCollector<>(output);
        cepTimerService = new TimerServiceImpl();

        // metrics
        this.numLateRecordsDropped = metrics.counter(LATE_ELEMENTS_DROPPED_METRIC_NAME);

        this.processors = new HashMap<>();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (processors != null) {
            for (CepRuleProcessor value : processors.values()) {
                value.close();
            }
        }

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
                processors.put(ruleUpdated.getId(), new CepRuleProcessor(ruleUpdated));
            }
        }
        // 清理无用的规则状态
        Iterator<Map.Entry<String, CepRuleProcessor>> iterator = processors.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, CepRuleProcessor> entry = iterator.next();
            if (!newIds.contains(entry.getKey())) {
                try {
                    entry.getValue().close();
                } catch (Exception e) {
                    throw new FlinkRuntimeException("Failed to close old processor.");
                }
                iterator.remove();
            }
        }
    }

    @Override
    public void processElement(StreamRecord<EventRecord<IN>> element) throws Exception {
        final String ruleId = element.getValue().getRuleId();
        CepRuleProcessor processor;
        if (processors.containsKey(ruleId)) {
            processor = processors.get(ruleId);
        } else {
            return;
        }
        processor.open();

        if (isProcessingTime) {
            if (comparator == null) {
                // there can be no out of order elements in processing time
                NFAState nfaState = getNFAState(processor.nfa);
                long timestamp = getProcessingTimeService().getCurrentProcessingTime();
                advanceTime(processor, nfaState, timestamp);
                processEvent(processor, nfaState, element.getValue().getEvent(), timestamp);
                updateNFA(nfaState);
            } else {
                long currentTime = timerService.currentProcessingTime();
                bufferEvent(element.getValue().getEvent(), currentTime);
            }

        } else {

            long timestamp = element.getTimestamp();
            IN value = element.getValue().getEvent();

            // In event-time processing we assume correctness of the watermark.
            // Events with timestamp smaller than or equal with the last seen watermark are
            // considered late.
            // Late events are put in a dedicated side output, if the user has specified one.

            if (timestamp > timerService.currentWatermark()) {

                // we have an event with a valid timestamp, so
                // we buffer it until we receive the proper watermark.

                bufferEvent(value, timestamp);

            } else if (lateDataOutputTag != null) {
                output.collect(lateDataOutputTag, element);
            } else {
                numLateRecordsDropped.inc();
            }
        }
    }

    private void registerTimer(long timestamp) {
        if (isProcessingTime) {
            timerService.registerProcessingTimeTimer(VoidNamespace.INSTANCE, timestamp + 1);
        } else {
            timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, timestamp);
        }
    }

    private void bufferEvent(IN event, long currentTime) throws Exception {
        List<IN> elementsForTimestamp = elementQueueState.get(currentTime);
        if (elementsForTimestamp == null) {
            elementsForTimestamp = new ArrayList<>();
            registerTimer(currentTime);
        }

        elementsForTimestamp.add(event);
        elementQueueState.put(currentTime, elementsForTimestamp);
    }

    @Override
    public void onEventTime(InternalTimer<RuleRowKey<?>, VoidNamespace> timer) throws Exception {

        // 1) get the queue of pending elements for the key and the corresponding NFA,
        // 2) process the pending elements in event time order and custom comparator if exists
        //		by feeding them in the NFA
        // 3) advance the time to the current watermark, so that expired patterns are discarded.
        // 4) update the stored state for the key, by only storing the new NFA and MapState iff they
        //		have state to be used later.
        // 5) update the last seen watermark.

        RuleRowKey<?> rowKey = timer.getKey();
        final String ruleId = rowKey.getRuleKey();
        if (!processors.containsKey(ruleId)) {
            return;
        }
        CepRuleProcessor processor = processors.get(ruleId);


        // STEP 1
        PriorityQueue<Long> sortedTimestamps = getSortedTimestamps();
        NFAState nfaState = getNFAState(processor.nfa);

        // STEP 2
        while (!sortedTimestamps.isEmpty()
                && sortedTimestamps.peek() <= timerService.currentWatermark()) {
            long timestamp = sortedTimestamps.poll();
            advanceTime(processor, nfaState, timestamp);
            try (Stream<IN> elements = sort(elementQueueState.get(timestamp))) {
                elements.forEachOrdered(
                        event -> {
                            try {
                                processEvent(processor, nfaState, event, timestamp);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
            }
            elementQueueState.remove(timestamp);
        }

        // STEP 3
        advanceTime(processor, nfaState, timerService.currentWatermark());

        // STEP 4
        updateNFA(nfaState);
    }

    @Override
    public void onProcessingTime(InternalTimer<RuleRowKey<?>, VoidNamespace> timer) throws Exception {
        // 1) get the queue of pending elements for the key and the corresponding NFA,
        // 2) process the pending elements in process time order and custom comparator if exists
        //		by feeding them in the NFA
        // 3) update the stored state for the key, by only storing the new NFA and MapState iff they
        //		have state to be used later.

        RuleRowKey<?> rowKey = timer.getKey();
        final String ruleId = rowKey.getRuleKey();
        if (!processors.containsKey(ruleId)) {
            return;
        }
        CepRuleProcessor processor = processors.get(ruleId);


        // STEP 1
        PriorityQueue<Long> sortedTimestamps = getSortedTimestamps();
        NFAState nfa = getNFAState(processor.nfa);

        // STEP 2
        while (!sortedTimestamps.isEmpty()) {
            long timestamp = sortedTimestamps.poll();
            advanceTime(processor, nfa, timestamp);
            try (Stream<IN> elements = sort(elementQueueState.get(timestamp))) {
                elements.forEachOrdered(
                        event -> {
                            try {
                                processEvent(processor, nfa, event, timestamp);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
            }
            elementQueueState.remove(timestamp);
        }

        // STEP 3
        advanceTime(processor, nfa, timerService.currentProcessingTime());

        // STEP 4
        updateNFA(nfa);
    }

    private Stream<IN> sort(Collection<IN> elements) {
        Stream<IN> stream = elements.stream();
        return (comparator == null) ? stream : stream.sorted(comparator);
    }

    private NFAState getNFAState(NFA<IN> nfa) throws IOException {
        NFAState nfaState = computationStates.value();
        return nfaState != null ? nfaState : nfa.createInitialNFAState();
    }

    private void updateNFA(NFAState nfaState) throws IOException {
        if (nfaState.isStateChanged()) {
            nfaState.resetStateChanged();
            nfaState.resetNewStartPartialMatch();
            computationStates.update(nfaState);
        }
    }

    private PriorityQueue<Long> getSortedTimestamps() throws Exception {
        PriorityQueue<Long> sortedTimestamps = new PriorityQueue<>();
        for (Long timestamp : elementQueueState.keys()) {
            sortedTimestamps.offer(timestamp);
        }
        return sortedTimestamps;
    }

    /**
     * Process the given event by giving it to the NFA and outputting the produced set of matched
     * event sequences.
     *
     * @param nfaState  Our NFAState object
     * @param event     The current event to be processed
     * @param timestamp The timestamp of the event
     */
    private void processEvent(CepRuleProcessor processor, NFAState nfaState, IN event, long timestamp) throws Exception {
        try (SharedBufferAccessor<IN> sharedBufferAccessor = processor.partialMatches.getAccessor()) {
            Collection<Map<String, List<IN>>> patterns =
                    processor.nfa.process(
                            sharedBufferAccessor,
                            nfaState,
                            event,
                            timestamp,
                            processor.afterMatchSkipStrategy,
                            cepTimerService);
            if (processor.nfa.getWindowTime() > 0 && nfaState.isNewStartPartialMatch()) {
                registerTimer(timestamp + processor.nfa.getWindowTime());
            }
            processMatchedSequences(processor, patterns, timestamp);
        }
    }

    /**
     * Advances the time for the given NFA to the given timestamp. This means that no more events
     * with timestamp <b>lower</b> than the given timestamp should be passed to the nfa, This can
     * lead to pruning and timeouts.
     */
    private void advanceTime(CepRuleProcessor processor, NFAState nfaState, long timestamp) throws Exception {
        try (SharedBufferAccessor<IN> sharedBufferAccessor = processor.partialMatches.getAccessor()) {
            Tuple2<
                    Collection<Map<String, List<IN>>>,
                    Collection<Tuple2<Map<String, List<IN>>, Long>>>
                    pendingMatchesAndTimeout =
                    processor.nfa.advanceTime(
                            sharedBufferAccessor,
                            nfaState,
                            timestamp,
                            processor.afterMatchSkipStrategy);

            Collection<Map<String, List<IN>>> pendingMatches = pendingMatchesAndTimeout.f0;
            Collection<Tuple2<Map<String, List<IN>>, Long>> timedOut = pendingMatchesAndTimeout.f1;

            if (!pendingMatches.isEmpty()) {
                processMatchedSequences(processor, pendingMatches, timestamp);
            }
            if (!timedOut.isEmpty()) {
                processTimedOutSequences(processor, timedOut);
            }
        }
    }

    private void processMatchedSequences(CepRuleProcessor processor, Iterable<Map<String, List<IN>>> matchingSequences, long timestamp) throws Exception {

        setTimestamp(timestamp);
        for (Map<String, List<IN>> matchingSequence : matchingSequences) {
            processor.function.processMatch(matchingSequence, context, collector);
        }
    }

    private void processTimedOutSequences(CepRuleProcessor processor,
                                          Collection<Tuple2<Map<String, List<IN>>, Long>> timedOutSequences) throws Exception {
        PatternProcessFunction<IN, OUT> function = processor.function;
        if (function instanceof TimedOutPartialMatchHandler) {

            @SuppressWarnings("unchecked")
            TimedOutPartialMatchHandler<IN> timeoutHandler =
                    (TimedOutPartialMatchHandler<IN>) function;

            for (Tuple2<Map<String, List<IN>>, Long> matchingSequence : timedOutSequences) {
                setTimestamp(matchingSequence.f1);
                timeoutHandler.processTimedOutMatch(matchingSequence.f0, context);
            }
        }
    }

    private void setTimestamp(long timestamp) {
        if (!isProcessingTime) {
            collector.setAbsoluteTimestamp(timestamp);
        }

        context.setTimestamp(timestamp);
    }

    /**
     * Gives {@link NFA} access to {@link InternalTimerService} and tells if {@link CepOperator}
     * works in processing time. Should be instantiated once per operator.
     */
    private class TimerServiceImpl implements TimerService {

        @Override
        public long currentProcessingTime() {
            return timerService.currentProcessingTime();
        }
    }

    /**
     * Implementation of {@link PatternProcessFunction.Context}. Design to be instantiated once per
     * operator. It serves three methods:
     *
     * <ul>
     *   <li>gives access to currentProcessingTime through {@link InternalTimerService}
     *   <li>gives access to timestamp of current record (or null if Processing time)
     *   <li>enables side outputs with proper timestamp of StreamRecord handling based on either
     *       Processing or Event time
     * </ul>
     */
    private class ContextFunctionImpl implements PatternProcessFunction.Context {

        private Long timestamp;

        @Override
        public <X> void output(final OutputTag<X> outputTag, final X value) {
            final StreamRecord<X> record;
            if (isProcessingTime) {
                record = new StreamRecord<>(value);
            } else {
                record = new StreamRecord<>(value, timestamp());
            }
            output.collect(outputTag, record);
        }

        void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        @Override
        public long timestamp() {
            return timestamp;
        }

        @Override
        public long currentProcessingTime() {
            return timerService.currentProcessingTime();
        }
    }


    public class CepRuleProcessor implements Serializable {
        private RuleUpdated rule;
        @Getter
        private transient SharedBuffer<IN> partialMatches;
        @Getter
        private transient Pattern<IN, ?> pattern;
        @Getter
        private transient NFA<IN> nfa;
        @Getter
        private transient AfterMatchSkipStrategy afterMatchSkipStrategy;
        @Getter
        private transient PatternProcessFunction<IN, OUT> function;
        private final Configuration configuration;


        public CepRuleProcessor(RuleUpdated rule) {
            this.rule = rule;
            try {
                Map<String, Object> parameters = JacksonUtils.getObjectMapper().readValue(rule.getParameters(),
                        new TypeReference<Map<String, Object>>() {
                        });
                configuration = ObjectConfiguration.of(parameters);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }

        @SuppressWarnings("unchecked")

        public void open() {
            if (partialMatches == null) {
                try {
                    // 1. 初始化SharedBuffer
                    partialMatches = new SharedBuffer<>(
                            stateInitializationContext.getKeyedStateStore(),
                            inputSerializer,
                            SharedBufferCacheConfig.of(getOperatorConfig().getConfiguration()));

                    if (stateInitializationContext.isRestored()) {
                        partialMatches.migrateOldState(getKeyedStateBackend(), computationStates);
                    }
                    // 2. 创建ClassLoader
                    ClassLoader classLoader = UserClassLoaderUtils.getClassLoader(userLibDir, rule.getLibs(), rule.getVersion(), getUserCodeClassloader());

                    // 3. 创建Pattern
                    pattern = (Pattern<IN, ?>) CepJsonUtils.convertJSONStringToPattern(rule.getPattern(),
                            classLoader, configuration);
                    afterMatchSkipStrategy = Optional.ofNullable(pattern.getAfterMatchSkipStrategy()).orElse(AfterMatchSkipStrategy.noSkip());

                    // 4. 创建处理函数
                    function = (PatternProcessFunction<IN, OUT>) classLoader
                            .loadClass(rule.getFunction())
                            .getConstructor().newInstance();
                    function.open(configuration);

                    // 5. 创建NFA
                    final NFACompiler.NFAFactory<IN> nfaFactory =
                            NFACompiler.compileFactory(pattern,
                                    function instanceof TimedOutPartialMatchHandler);
                    nfa = nfaFactory.createNFA();
                    nfa.open(cepRuntimeContext, new Configuration());
                } catch (Exception e) {
                    throw new FlinkRuntimeException(e);
                }
            }
        }

        public void update(RuleUpdated updated) {
            if (updated.getVersion() > rule.getVersion()) {
                close();
                this.rule = updated;
            }
        }

        public void close() {
            try {
                if (nfa != null) {
                    nfa.close();
                }
                if (partialMatches != null) {
                    partialMatches.releaseCacheStatisticsTimer();
                    // 清理状态，清理所当前规则的所有状态
                    KeyedStateBackend<RuleRowKey<?>> keyedStateBackend = getKeyedStateBackend();
                    keyedStateBackend.getKeys(NFA_STATE_NAME, VoidNamespace.INSTANCE).forEach(key -> {

                        String ruleId = key.getRuleKey();
                        if (rule.getId().equals(ruleId)) {
                            keyedStateBackend.setCurrentKey(key);
                            partialMatches.clear();
                            computationStates.clear();
                            elementQueueState.clear();
                        }

                    });


                }
                if (function != null) {
                    FunctionUtils.closeFunction(function);
                }
            } catch (Exception e) {
                throw new FlinkRuntimeException("Failed to close old cep rule processor.", e);
            }

            nfa = null;
            partialMatches = null;
            pattern = null;
            function = null;
        }

    }

}
