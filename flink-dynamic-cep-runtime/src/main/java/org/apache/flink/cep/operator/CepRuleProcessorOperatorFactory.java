package org.apache.flink.cep.operator;


import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.cep.EventComparator;
import org.apache.flink.cep.TimeBehaviour;
import org.apache.flink.cep.coordinator.RuleProcessorCoordinatorProvider;
import org.apache.flink.cep.event.EventRecord;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeServiceAware;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;

/**
 * The Factory class for {@link CepRuleProcessorOperator}.
 *
 * @author shirukai
 */
public class CepRuleProcessorOperatorFactory<IN, OUT> extends AbstractStreamOperatorFactory<OUT>
        implements OneInputStreamOperatorFactory<EventRecord<IN>, OUT>,
        CoordinatedOperatorFactory<OUT>,
        ProcessingTimeServiceAware {

    private static final long serialVersionUID = 6491248798964426467L;


    private final String ruleQueueId;

    private final String userLibDir;
    private final EventComparator<IN> comparator;

    private final OutputTag<EventRecord<IN>> lateDataOutputTag;

    private final TimeBehaviour timeBehaviour;

    private final TypeSerializer<IN> inputSerializer;

    public CepRuleProcessorOperatorFactory(
            final TypeSerializer<IN> inputSerializer,
            String ruleQueueId,
            String userLibDir,
            final TimeBehaviour timeBehaviour,
            @Nullable final EventComparator<IN> comparator,
            @Nullable final OutputTag<EventRecord<IN>> lateDataOutputTag) {

        this.ruleQueueId = ruleQueueId;
        this.userLibDir = userLibDir;

        this.timeBehaviour = timeBehaviour;
        this.comparator = comparator;
        this.lateDataOutputTag = lateDataOutputTag;
        this.inputSerializer = inputSerializer;
    }

    @Override
    public OperatorCoordinator.Provider getCoordinatorProvider(String operatorName, OperatorID operatorID) {
        return new RuleProcessorCoordinatorProvider(operatorName, operatorID, ruleQueueId);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<OUT>> T createStreamOperator(StreamOperatorParameters<OUT> parameters) {
        final OperatorID operatorId = parameters.getStreamConfig().getOperatorID();
        try {
            final CepRuleProcessorOperator<IN, OUT> processorOperator = new CepRuleProcessorOperator<>(
                    processingTimeService,
                    inputSerializer,
                    timeBehaviour == TimeBehaviour.ProcessingTime,

                    comparator,
                    lateDataOutputTag,
                    userLibDir);
            processorOperator.setup(
                    parameters.getContainingTask(),
                    parameters.getStreamConfig(),
                    parameters.getOutput());
            parameters
                    .getOperatorEventDispatcher()
                    .registerEventHandler(operatorId, processorOperator);

            return (T) processorOperator;
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Cannot create udf rule processor operator for "
                            + parameters.getStreamConfig().getOperatorName(),
                    e);
        }
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return UdfRuleProcessorOperator.class;
    }
}
