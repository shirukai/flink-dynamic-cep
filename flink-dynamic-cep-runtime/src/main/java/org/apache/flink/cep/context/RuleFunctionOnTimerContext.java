package org.apache.flink.cep.context;

import org.apache.flink.streaming.api.TimeDomain;

/**
 * @author shirukai
 */
public interface RuleFunctionOnTimerContext<KEY> extends RuleFunctionContext<KEY> {
    TimeDomain timeDomain();
}
