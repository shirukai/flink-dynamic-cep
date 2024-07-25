package org.apache.flink.cep.discover;

import java.io.Serializable;

/**
 * @author shirukai
 */
public interface RuleDiscovererFactory extends Serializable {
    RuleDiscoverer createRuleDiscoverer(ClassLoader userCodeClassloader)
            throws Exception;
}
