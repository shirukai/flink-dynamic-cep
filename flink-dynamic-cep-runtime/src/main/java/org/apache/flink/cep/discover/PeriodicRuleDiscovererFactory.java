package org.apache.flink.cep.discover;




import org.apache.flink.cep.event.Rule;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Implementation of the {@link RuleDiscovererFactory} that creates the {@link
 * PeriodicRuleDiscoverer} instance.
 *
 */
public abstract class PeriodicRuleDiscovererFactory
        implements RuleDiscovererFactory {
    @Nullable private final List<Rule> initialRules;

    private final Long intervalMillis;

    public PeriodicRuleDiscovererFactory(
            @Nullable final List<Rule> initialRules, Long intervalMillis) {
        this.initialRules = initialRules;
        this.intervalMillis = intervalMillis;
    }

    @Nullable
    public List<Rule> getInitialRules() {
        return initialRules;
    }

    @Override
    public abstract PeriodicRuleDiscoverer createRuleDiscoverer(
            ClassLoader userCodeClassLoader) throws Exception;

    public Long getIntervalMillis() {
        return intervalMillis;
    }
}