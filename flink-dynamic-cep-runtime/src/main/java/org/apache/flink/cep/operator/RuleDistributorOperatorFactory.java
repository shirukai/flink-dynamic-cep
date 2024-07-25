package org.apache.flink.cep.operator;


import org.apache.flink.cep.coordinator.RuleDistributorCoordinatorProvider;
import org.apache.flink.cep.discover.RuleDiscovererFactory;
import org.apache.flink.cep.event.EventRecord;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeServiceAware;

/**
 * The Factory class for {@link RuleDistributorOperator}.
 *
 * @author shirukai
 */
public class RuleDistributorOperatorFactory<IN> extends AbstractStreamOperatorFactory<EventRecord<IN>>
        implements OneInputStreamOperatorFactory<IN, EventRecord<IN>>,
        CoordinatedOperatorFactory<EventRecord<IN>>,
        ProcessingTimeServiceAware {

    private final RuleDiscovererFactory discoverFactory;

    private final String ruleQueueId;
    private final boolean keyBindingEnabled;

    public RuleDistributorOperatorFactory(String ruleQueueId, RuleDiscovererFactory discoverFactory, boolean keyBindingEnabled) {
        this.discoverFactory = discoverFactory;
        this.ruleQueueId = ruleQueueId;
        this.keyBindingEnabled = keyBindingEnabled;
    }

    @Override
    public OperatorCoordinator.Provider getCoordinatorProvider(String operatorName, OperatorID operatorID) {
        return new RuleDistributorCoordinatorProvider(operatorName, operatorID, ruleQueueId, discoverFactory);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<EventRecord<IN>>> T createStreamOperator(StreamOperatorParameters<EventRecord<IN>> parameters) {
        final OperatorID operatorId = parameters.getStreamConfig().getOperatorID();
        try {
            final RuleDistributorOperator<IN> distributorOperator = new RuleDistributorOperator<>(processingTimeService,keyBindingEnabled);
            distributorOperator.setup(
                    parameters.getContainingTask(),
                    parameters.getStreamConfig(),
                    parameters.getOutput());
            parameters
                    .getOperatorEventDispatcher()
                    .registerEventHandler(operatorId, distributorOperator);

            return (T) distributorOperator;
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Cannot create rule distributor operator for "
                            + parameters.getStreamConfig().getOperatorName(),
                    e);
        }

    }

    @Override
    @SuppressWarnings("rawtypes")
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return RuleDistributorOperator.class;
    }
}
