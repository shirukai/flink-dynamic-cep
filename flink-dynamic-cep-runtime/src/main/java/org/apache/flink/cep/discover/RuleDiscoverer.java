package org.apache.flink.cep.discover;


import java.io.Closeable;

/**
 * @author shirukai
 */
public interface RuleDiscoverer extends Closeable {
    void discoverRuleUpdates(RuleManager ruleManager);
}
