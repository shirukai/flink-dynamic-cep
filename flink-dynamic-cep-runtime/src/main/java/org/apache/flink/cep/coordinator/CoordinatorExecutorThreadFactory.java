package org.apache.flink.cep.coordinator;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.FatalExitExceptionHandler;

import javax.annotation.Nullable;
import java.util.concurrent.ThreadFactory;

/**
 * A thread factory class that provides some helper methods.
 */
public class CoordinatorExecutorThreadFactory
        implements ThreadFactory, Thread.UncaughtExceptionHandler {

    private final String coordinatorThreadName;
    private final ClassLoader classLoader;
    private final Thread.UncaughtExceptionHandler errorHandler;

    @Nullable
    private Thread thread;

    // TODO discuss if we should fail the job(JM may restart the job later) or directly kill JM
    // process
    // Currently we choose to directly kill JM process
    CoordinatorExecutorThreadFactory(
            final String coordinatorThreadName, final ClassLoader contextClassLoader) {
        this(coordinatorThreadName, contextClassLoader, FatalExitExceptionHandler.INSTANCE);
    }

    @VisibleForTesting
    CoordinatorExecutorThreadFactory(
            final String coordinatorThreadName,
            final ClassLoader contextClassLoader,
            final Thread.UncaughtExceptionHandler errorHandler) {
        this.coordinatorThreadName = coordinatorThreadName;
        this.classLoader = contextClassLoader;
        this.errorHandler = errorHandler;
    }

    @Override
    public synchronized Thread newThread(Runnable r) {
        thread = new Thread(r, coordinatorThreadName);
        thread.setContextClassLoader(classLoader);
        thread.setUncaughtExceptionHandler(this);
        return thread;
    }

    @Override
    public synchronized void uncaughtException(Thread t, Throwable e) {
        errorHandler.uncaughtException(t, e);
    }

    public String getCoordinatorThreadName() {
        return coordinatorThreadName;
    }

    boolean isCurrentThreadCoordinatorThread() {
        return Thread.currentThread() == thread;
    }
}
