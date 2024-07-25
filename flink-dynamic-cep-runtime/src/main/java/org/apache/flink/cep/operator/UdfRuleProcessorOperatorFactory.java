package org.apache.flink.cep.operator;

import org.apache.flink.cep.coordinator.RuleProcessorCoordinatorProvider;
import org.apache.flink.cep.event.EventRecord;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeServiceAware;

/**
 * The Factory class for {@link UdfRuleProcessorOperator}.
 *
 * @author shirukai
 */
public class UdfRuleProcessorOperatorFactory<IN, OUT> extends AbstractStreamOperatorFactory<OUT>
        implements OneInputStreamOperatorFactory<EventRecord<IN>, OUT>,
        CoordinatedOperatorFactory<OUT>,
        ProcessingTimeServiceAware {

    private static final long serialVersionUID = 6491248798964426467L;


    private final String ruleQueueId;

    private final String userLibDir;

    public UdfRuleProcessorOperatorFactory(String ruleQueueId, String userLibDir) {
        this.ruleQueueId = ruleQueueId;
        this.userLibDir = userLibDir;
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
            final UdfRuleProcessorOperator<?, IN, OUT> processorOperator = new UdfRuleProcessorOperator<>(processingTimeService, userLibDir);
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
