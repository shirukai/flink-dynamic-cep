/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KTD, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.cep.discover;

import org.apache.flink.cep.event.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Implementation of the {@link RuleDiscoverer} that periodically discovers the rule
 * processor updates.
 */
public abstract class PeriodicRuleDiscoverer
        implements RuleDiscoverer {

    private static final Logger LOG = LoggerFactory.getLogger(PeriodicRuleDiscoverer.class);
    private final Long intervalMillis;

    private final Timer timer;

    private List<Rule> rules;

    /**
     * Creates a new {@link RuleDiscoverer} using the given initial {@link
     * Rule} and the time interval how often to check the rule processor updates.
     *
     * @param intervalMillis Time interval in milliseconds how often to check updates.
     */
    public PeriodicRuleDiscoverer(final Long intervalMillis) {
        this.intervalMillis = intervalMillis;
        this.timer = new Timer();
    }


    /**
     * Returns the latest rule processors.
     *
     * @return The list of {@link Rule}.
     */
    public abstract List<Rule> getLatestRules() throws Exception;

    @Override
    public void discoverRuleUpdates(
            RuleManager ruleManager) {
        // Periodically discovers the rule updates.
        timer.schedule(
                new TimerTask() {
                    @Override
                    public void run() {
                        try {
                            List<Rule> latestRules = getLatestRules();
                            if (isUpdated(latestRules)) {
                                rules = latestRules;
                                LOG.info("Latest rules are updated.");
                                ruleManager.onRuleUpdated(rules);
                            }
                        } catch (Exception e) {
                            LOG.error("Get latest rule error", e);
                        }

                    }
                },
                0,
                intervalMillis);
    }

    @Override
    public void close() throws IOException {
        timer.cancel();
    }

    public boolean isUpdated(List<Rule> latestRules) {
        return rules == null || rules.size() != latestRules.size() || !new HashSet<>(rules).containsAll(latestRules);
    }
}