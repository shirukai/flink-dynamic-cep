package org.apache.flink.cep.discover;

import org.apache.flink.cep.event.Rule;
import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;

import javax.annotation.Nullable;
import java.util.List;

/**
 * 简单规则发现器工厂
 *
 * @author shirukai
 */
public class JdbcPeriodicRuleDiscovererFactory extends PeriodicRuleDiscovererFactory {
    private final JdbcConnectorOptions jdbcConnectorOptions;
    private final int maxRetryTimes;
    private final String ruleType;


    public JdbcPeriodicRuleDiscovererFactory(
            final JdbcConnectorOptions jdbcConnectorOptions,
            final int maxRetryTimes,
            final String ruleType,
            @Nullable List<Rule> initialRules, Long intervalMillis) {
        super(initialRules, intervalMillis);
        this.jdbcConnectorOptions = jdbcConnectorOptions;
        this.maxRetryTimes = maxRetryTimes;
        this.ruleType = ruleType;
    }

    @Override
    public PeriodicRuleDiscoverer createRuleDiscoverer(ClassLoader userCodeClassLoader) throws Exception {
        return new JdbcPeriodicRuleDiscoverer(jdbcConnectorOptions, maxRetryTimes, ruleType, getInitialRules(), getIntervalMillis(), userCodeClassLoader);
    }
}
