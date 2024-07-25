package org.apache.flink.cep.coordinator;


import org.apache.flink.cep.discover.RuleDiscoverer;
import org.apache.flink.cep.discover.RuleDiscovererFactory;
import org.apache.flink.cep.discover.RuleManager;
import org.apache.flink.cep.event.*;
import org.apache.flink.runtime.operators.coordination.CoordinatorStore;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.function.ThrowingRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.flink.util.IOUtils.closeAll;

/**
 * 规则发现协调器
 *
 * @author shirukai
 */
public class RuleDistributorCoordinator
        implements OperatorCoordinator, RuleManager {
    private static final Logger LOG = LoggerFactory.getLogger(RuleDistributorCoordinator.class);

    /**
     * The name of the operator this RuleDistributorCoordinator is associated with.
     */
    private final String operatorName;
    private final RuleDiscovererFactory discovererFactory;
    private final CoordinatorContext context;
    private final String ruleUpdatedQueueId;
    private boolean started;
    private RuleDiscoverer discoverer;
    private RuleBindingEvent currentRuleBindingEvent;
    private final LinkedBlockingQueue<RuleUpdatedEvent> updatedEventQueue;

    public RuleDistributorCoordinator(String operatorName,
                                      String ruleUpdatedQueueId,
                                      RuleDiscovererFactory discovererFactory,
                                      CoordinatorContext coordinatorContext
    ) {
        this.operatorName = operatorName;
        this.discovererFactory = discovererFactory;
        this.context = coordinatorContext;
        this.ruleUpdatedQueueId = ruleUpdatedQueueId;
        updatedEventQueue = getRuleUpdatedEventQueue();
    }


    /**
     * 启动RuleDiscover，查询规则
     *
     * @throws Exception e
     */
    @Override
    public void start() throws Exception {
        LOG.info(
                "Starting RuleDiscoverer for {}: {}.",
                this.getClass().getSimpleName(),
                operatorName);

        // we mark this as started first, so that we can later distinguish the cases where 'start()'
        // wasn't called and where 'start()' failed.
        started = true;
        if (discoverer == null) {
            try {
                discoverer =
                        discovererFactory.createRuleDiscoverer(
                                context.getUserCodeClassloader());
            } catch (Throwable t) {
                ExceptionUtils.rethrowIfFatalError(t);
                LOG.error(
                        "Failed to create RuleDiscoverer for {}: {}.",
                        this.getClass().getSimpleName(),
                        operatorName,
                        t);
                context.failJob(t);
                return;
            }
        }

        // The rule discovery is the first task in the coordinator executor.
        // We rely on the single-threaded coordinator executor to guarantee
        // the other methods are invoked after the discoverer has discovered.
        runInEventLoop(
                () -> discoverer.discoverRuleUpdates(this),
                "discovering the Rule updates.");
    }


    @Override
    public void close() throws Exception {
        LOG.info("Closing RuleDistributorCoordinator for rule distributor {}.", operatorName);
        if (started) {
            closeAll(context, discoverer);
        }
        started = false;
        LOG.info("RuleDistributorCoordinator for rule distributor {} closed.", operatorName);
    }

    @Override
    public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event) throws Exception {
        // no op
    }

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture) throws Exception {
        runInEventLoop(
                () -> {
                    LOG.debug(
                            "Taking a state snapshot on operator {} for checkpoint {}",
                            operatorName,
                            checkpointId);
                    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                         ObjectOutputStream out = new ObjectOutputStream(baos)) {
                        out.writeObject(currentRuleBindingEvent);
                        out.flush();
                        resultFuture.complete(baos.toByteArray());

                    } catch (Throwable e) {
                        ExceptionUtils.rethrowIfFatalErrorOrOOM(e);
                        resultFuture.completeExceptionally(
                                new CompletionException(
                                        String.format(
                                                "Failed to checkpoint the RuleBindingEvent for rule distributor %s",
                                                operatorName),
                                        e));
                    }
                },
                "taking checkpoint %d",
                checkpointId);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {

    }

    @Override
    public void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData) throws Exception {
        // The checkpoint data is null if there was no completed checkpoint before in that case we
        // don't restore here, but let a fresh RuleProcessorDiscoverer be created when "start()"
        // is called.
        if (checkpointData == null) {
            return;
        }

        LOG.info(
                "Restoring RuleDiscoverer of rule distributor {} from checkpoint.",
                operatorName);
        try (ByteArrayInputStream bais = new ByteArrayInputStream(checkpointData);
             ObjectInputStream in = new ObjectInputStream(bais)) {
            currentRuleBindingEvent = (RuleBindingEvent) in.readObject();
        }
        discoverer = discovererFactory.createRuleDiscoverer(context.getUserCodeClassloader());

    }

    @Override
    public void subtaskReset(int subtask, long checkpointId) {
        LOG.info(
                "Recovering subtask {} to checkpoint {} for rule distributor {} to checkpoint.",
                subtask,
                checkpointId,
                operatorName);
        runInEventLoop(
                () -> {
                    if (currentRuleBindingEvent != null) {
                        context.sendEventToOperator(
                                subtask, currentRuleBindingEvent);
                    }
                },
                "making event gateway to subtask %d available",
                subtask);
    }

    @Override
    public void executionAttemptFailed(int subtask, int attemptNumber, @Nullable Throwable reason) {
        runInEventLoop(
                () -> {
                    LOG.info(
                            "Removing itself after failure for subtask {} of rule distributor {}.",
                            subtask,
                            operatorName);
                    context.subtaskNotReady(subtask);
                },
                "handling subtask %d failure",
                subtask);
    }

    @Override
    public void executionAttemptReady(int subtask, int attemptNumber, SubtaskGateway gateway) {
        assert subtask == gateway.getSubtask();
        LOG.debug("Subtask {} of rule distributor {} is ready.", subtask, operatorName);
        runInEventLoop(
                () -> {
                    context.subtaskReady(gateway);
                    if (currentRuleBindingEvent != null) {
                        context.sendEventToOperator(subtask, currentRuleBindingEvent);
                    }

                },
                "making event gateway to subtask %d available",
                subtask);

    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        LOG.info(
                "Marking checkpoint {} as aborted for rule distributor {}.",
                checkpointId,
                operatorName);
    }

    private void ensureStarted() {
        if (!started) {
            throw new IllegalStateException("The coordinator has not started yet.");
        }
    }

    private void runInEventLoop(
            final ThrowingRunnable<Throwable> action,
            final String actionName,
            final Object... actionNameFormatParameters) {
        ensureStarted();

        // We may end up here even for a non-started discoverer, in case the instantiation failed,
        // and we get the 'subtaskFailed()' notification during the failover.
        // We need to ignore those.
        if (discoverer == null) {
            return;
        }

        context.runInCoordinatorThread(
                () -> {
                    try {
                        action.run();
                    } catch (Throwable t) {
                        // If we have a JVM critical error, promote it immediately, there is a good
                        // chance the logging or job failing will not succeed any more
                        ExceptionUtils.rethrowIfFatalErrorOrOOM(t);
                        final String actionString =
                                String.format(actionName, actionNameFormatParameters);
                        LOG.error(
                                "Uncaught exception in the RuleDiscovererCoordinator for {} while {}. Triggering job failover.",
                                operatorName,
                                actionString,
                                t);
                        context.failJob(t);
                    }
                });
    }


    /**
     * RuleDiscover查询到规则之后，会调用当前方法
     *
     * @param rules 查询到的规则列表
     */
    @Override
    public void onRuleUpdated(List<Rule> rules) {

        List<RuleUpdated> updates = new ArrayList<>(rules.size());
        List<RuleBinding> bindings = new ArrayList<>(rules.size());


        for (Rule rule : rules) {
            updates.add(RuleUpdated.of(rule));
            bindings.add(RuleBinding.of(rule));
        }

        // 1. 规则更新事件，通过队列发送给RuleProcessorCoordinator
        try {
            updatedEventQueue.put(new RuleUpdatedEvent(updates));
        } catch (InterruptedException e) {
            LOG.error("Failed to send RuleUpdatedEvent to rule processor coordinator.", e);
            context.failJob(e);
            return;
        }
        currentRuleBindingEvent = new RuleBindingEvent(bindings);
        // 2. 规则绑定事件，发送给RuleDistributorOperator子任务
        for (int subtask : context.getSubtasks()) {
            try {
                context.sendEventToOperator(subtask, currentRuleBindingEvent);
            } catch (Exception e) {
                LOG.error(
                        "Failed to send RuleBindingEvent to rule distributor operator {}",
                        operatorName,
                        e);
                context.failJob(e);
                return;
            }
        }
    }

    @SuppressWarnings("unchecked")
    public LinkedBlockingQueue<RuleUpdatedEvent> getRuleUpdatedEventQueue() {
        CoordinatorStore coordinatorStore = context.getOperatorCoordinatorContext().getCoordinatorStore();
        return (LinkedBlockingQueue<RuleUpdatedEvent>) coordinatorStore.compute(ruleUpdatedQueueId, (key, value) -> {
            if (value == null) {
                return new LinkedBlockingQueue<>();
            } else {
                return value;
            }
        });
    }
}
