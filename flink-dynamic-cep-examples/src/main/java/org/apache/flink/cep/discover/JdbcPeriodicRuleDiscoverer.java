package org.apache.flink.cep.discover;

import org.apache.flink.cep.event.Rule;
import org.apache.flink.cep.utils.JacksonUtils;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.sql.*;
import java.util.*;

import static java.util.Objects.requireNonNull;

/**
 * 简单规则发现器
 *
 * @author shirukai
 */
public class JdbcPeriodicRuleDiscoverer extends PeriodicRuleDiscoverer {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcPeriodicRuleDiscoverer.class);
    private final int maxRetryTimes;
    private final List<Rule> initialRules;
    private final JdbcConnectionProvider connectionProvider;
    private Statement statement;
    private final String query;

    public JdbcPeriodicRuleDiscoverer(
            final JdbcConnectorOptions jdbcConnectorOptions,
            final int maxRetryTimes,
            final String ruleType,
            @Nullable List<Rule> initialRules,
            Long intervalMillis,
            ClassLoader userCodeClassLoader) throws Exception {
        super(intervalMillis);
        this.initialRules = initialRules;
        this.maxRetryTimes = maxRetryTimes;
        Driver driver = (Driver) Class.forName(jdbcConnectorOptions.getDriverName(), true, userCodeClassLoader).newInstance();
        DriverManager.registerDriver(driver);
        this.connectionProvider = new SimpleJdbcConnectionProvider(jdbcConnectorOptions);
        query = String.format("SELECT id,version,parameters,function,pattern,libs,binding_keys FROM %s WHERE rule_type='%s'", jdbcConnectorOptions.getTableName(), ruleType);
        establishConnectionAndStatement();
    }

    @Override
    public List<Rule> getLatestRules() throws Exception {
        List<Rule> rules = new ArrayList<>();
        for (int retry = 0; retry < maxRetryTimes; retry++) {
            rules.clear();
            rules.addAll(initialRules);
            try {
                Map<String, Rule> currentRules = new HashMap<>();
                try (ResultSet resultSet = statement.executeQuery(query)) {
                    while (resultSet.next()) {
                        Set<String> bindingKeySet;
                        Set<String> libSet;
                        String id = resultSet.getString("id");
                        String bindingKeys = resultSet.getString("binding_keys");
                        String libs = resultSet.getString("libs");
                        if (!StringUtils.isNullOrWhitespaceOnly(bindingKeys)) {
                            bindingKeySet = JacksonUtils.getObjectMapper().readValue(bindingKeys, new TypeReference<Set<String>>() {
                            });
                        } else {
                            bindingKeySet = Collections.emptySet();
                        }
                        if (!StringUtils.isNullOrWhitespaceOnly(libs)) {
                            libSet = JacksonUtils.getObjectMapper().readValue(libs, new TypeReference<Set<String>>() {
                            });
                        } else {
                            libSet = Collections.emptySet();
                        }
                        Rule rule = new Rule();
                        rule.setId(id);
                        rule.setVersion(resultSet.getInt("version"));
                        rule.setPattern(resultSet.getString("pattern"));
                        rule.setParameters(resultSet.getString("parameters"));
                        rule.setFunction(resultSet.getString("function"));
                        rule.setLibs(libSet);
                        rule.setBindingKeys(bindingKeySet);
                        currentRules.put(id, rule);
                    }
                }
                rules.addAll(currentRules.values());
                return rules;

            } catch (Exception e) {
                LOG.warn("Rule discoverer checks rule changes error,retry times = {} ", retry + 1, e);
                try {
                    Thread.sleep(1000L * retry + 1);
                    if (!connectionProvider.isConnectionValid()) {
                        statement.close();
                        connectionProvider.closeConnection();
                        establishConnectionAndStatement();
                    }
                } catch (InterruptedException | SQLException | ClassNotFoundException exception) {
                    LOG.warn("JDBC connection is not valid, and reestablish connection failed:{}", e.getMessage());
                }
            }
        }
        return rules;
    }

    private void establishConnectionAndStatement() throws SQLException, ClassNotFoundException {
        Connection connection = connectionProvider.getOrEstablishConnection();
        statement = connection.createStatement();
    }

    @Override
    public void close() throws IOException {
        super.close();
        try {
            if (statement != null) {
                statement.close();
            }
        } catch (SQLException e) {
            LOG.warn(
                    "Statement of the pattern processor discoverer couldn't be closed - "
                            + e.getMessage());
        } finally {
            statement = null;
        }
    }
}
