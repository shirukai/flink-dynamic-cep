package org.apache.flink.cep.coordinator;


import org.apache.flink.cep.event.RuleUpdatedEvent;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.flink.util.IOUtils.closeAll;

/**
 * 规则发现协调器
 *
 * @author shirukai
 */
public class RuleProcessorCoordinator
        implements OperatorCoordinator {
    private static final Logger LOG = LoggerFactory.getLogger(RuleProcessorCoordinator.class);

    /**
     * The name of the operator this RuleProcessorCoordinator is associated with.
     */
    private final String operatorName;
    private final CoordinatorContext context;
    private boolean started;
    private final String ruleUpdatedQueueId;

    private RuleUpdatedEvent currentRuleUpdatedEvent;
    private final LinkedBlockingQueue<RuleUpdatedEvent> updatedEventQueue;

    public RuleProcessorCoordinator(String operatorName,
                                    String ruleUpdatedQueueId,
                                    CoordinatorContext coordinatorContext) {
        this.operatorName = operatorName;
        this.context = coordinatorContext;
        this.ruleUpdatedQueueId = ruleUpdatedQueueId;
        this.updatedEventQueue = getRuleUpdatedEventQueue();
    }

    @Override
    public void start() throws Exception {
        LOG.info(
                "Starting RuleUpdatedConsumer for {}: {}.",
                this.getClass().getSimpleName(),
                operatorName);

        // we mark this as started first, so that we can later distinguish the cases where 'start()'
        // wasn't called and where 'start()' failed.
        started = true;

        // The rule discovery is the first task in the coordinator executor.
        // We rely on the single-threaded coordinator executor to guarantee
        // the other methods are invoked after the discoverer has discovered.
        runInEventLoop(
                this::consumeRuleUpdates,
                "consuming the Rule updates.");
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing RuleProcessorCoordinator for rule processor {}.", operatorName);
        if (started) {
            closeAll(context);
        }
        started = false;
        LOG.info("RuleProcessorCoordinator for rule processor {} closed.", operatorName);

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
                        out.writeObject(currentRuleUpdatedEvent);
                        out.flush();
                        resultFuture.complete(baos.toByteArray());

                    } catch (Throwable e) {
                        ExceptionUtils.rethrowIfFatalErrorOrOOM(e);
                        resultFuture.completeExceptionally(
                                new CompletionException(
                                        String.format(
                                                "Failed to checkpoint the RuleUpdatedEvent for rule distributor %s",
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
                "Restoring RuleUpdatedEvent of rule processor {} from checkpoint.",
                operatorName);
        try (ByteArrayInputStream bais = new ByteArrayInputStream(checkpointData);
             ObjectInputStream in = new ObjectInputStream(bais)) {
            currentRuleUpdatedEvent = (RuleUpdatedEvent) in.readObject();
        }
    }

    @Override
    public void subtaskReset(int subtask, long checkpointId) {
        LOG.info(
                "Recovering subtask {} to checkpoint {} for rule processor {} to checkpoint.",
                subtask,
                checkpointId,
                operatorName);
        runInEventLoop(
                () -> {
                    if (currentRuleUpdatedEvent != null) {
                        context.sendEventToOperator(
                                subtask, currentRuleUpdatedEvent);
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
                            "Removing itself after failure for subtask {} of rule processor {}.",
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
        LOG.debug("Subtask {} of rule processor {} is ready.", subtask, operatorName);
        runInEventLoop(
                () -> {
                    context.subtaskReady(gateway);
                    if (currentRuleUpdatedEvent != null) {
                        context.sendEventToOperator(
                                subtask, currentRuleUpdatedEvent);
                    }

                },
                "making event gateway to subtask %d available",
                subtask);

    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        LOG.info(
                "Marking checkpoint {} as aborted for rule processor {}.",
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
                                "Uncaught exception in the RuleProcessorCoordinator for {} while {}. Triggering job failover.",
                                operatorName,
                                actionString,
                                t);
                        context.failJob(t);
                    }
                });
    }

    public void consumeRuleUpdates() throws InterruptedException {
        // 1. 规则更新事件，发送给RuleProcessorOperator子任务
        Thread consumerThread = new Thread(() -> {
            while (started) {
                try {
                    currentRuleUpdatedEvent = updatedEventQueue.take();
                    // 1. 规则更新事件，发送给RuleProcessorOperator子任务
                    for (int subtask : context.getSubtasks()) {
                        context.sendEventToOperator(subtask, currentRuleUpdatedEvent);
                    }
                } catch (Exception e) {
                    LOG.error(
                            "Failed to send RuleUpdatedEvent to rule processor operator {}",
                            operatorName,
                            e);
                    context.failJob(e);
                    return;
                }
            }
        });
        consumerThread.setDaemon(true);
        consumerThread.start();
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
