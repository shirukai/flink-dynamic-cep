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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cep.dynamic.impl.json.spec;

import org.apache.flink.cep.dynamic.PatternWrapper;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.Quantifier.ConsumingStrategy;
import org.apache.flink.cep.pattern.Quantifier.QuantifierProperty;
import org.apache.flink.cep.pattern.WithinType;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.RichOrCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

/**
 * The Node is used to describe a PatternWrapper and contains all necessary fields of a PatternWrapper. This class
 * is to (de)serialize Nodes in json format.
 */
public class NodeSpec {
    private final String name;
    private final QuantifierSpec quantifier;
    private final ConditionSpec condition;
    private final PatternNodeType type;
    private final AfterMatchSkipStrategySpec afterMatchSkipStrategy;
    protected final WindowSpec window;

    private final @Nullable TimesSpec times;
    private final @Nullable ConditionSpec untilCondition;


    public NodeSpec(String name,
                    QuantifierSpec quantifier,
                    ConditionSpec condition,
                    TimesSpec times,
                    ConditionSpec untilCondition,
                    WindowSpec window,
                    AfterMatchSkipStrategySpec afterMatchStrategy) {
        this(name, quantifier, condition, times, untilCondition, window, afterMatchStrategy, PatternNodeType.ATOMIC);
    }

    public NodeSpec(
            @JsonProperty("name") String name,
            @JsonProperty("quantifier") QuantifierSpec quantifier,
            @JsonProperty("condition") ConditionSpec condition,
            @Nullable @JsonProperty("times") TimesSpec times,
            @Nullable @JsonProperty("untilCondition") ConditionSpec untilCondition,
            @JsonProperty("window") WindowSpec window,
            @JsonProperty("afterMatchSkipStrategy") AfterMatchSkipStrategySpec afterMatchSkipStrategy,
            @JsonProperty("type") PatternNodeType type

    ) {
        this.name = name;
        this.quantifier = quantifier;
        this.condition = condition;
        this.times = times;
        this.untilCondition = untilCondition;
        this.window = window;
        this.afterMatchSkipStrategy = afterMatchSkipStrategy;
        this.type = type;
    }

    public static Builder newBuilder(Pattern<?, ?> pattern) {
        // Name
        String name = pattern.getName();

        // Quantifier
        QuantifierSpec quantifier = new QuantifierSpec(pattern.getQuantifier());

        // Condition
        ConditionSpec condition = pattern.getCondition() != null ? ConditionSpec.fromCondition(pattern.getCondition()) : null;

        // Window
        Map<WithinType, Time> windowTime = new HashMap<>();
        if (pattern.getWindowTime(WithinType.FIRST_AND_LAST) != null) {
            windowTime.put(WithinType.FIRST_AND_LAST, pattern.getWindowTime(WithinType.FIRST_AND_LAST));
        } else if (pattern.getWindowTime(WithinType.PREVIOUS_AND_CURRENT) != null) {
            windowTime.put(
                    WithinType.PREVIOUS_AND_CURRENT,
                    pattern.getWindowTime(WithinType.PREVIOUS_AND_CURRENT));
        }
        WindowSpec window = windowTime.isEmpty() ? null : WindowSpec.fromWindowTime(windowTime);

        // Times
        TimesSpec times = pattern.getTimes() != null ? TimesSpec.of(pattern.getTimes()) : null;

        // UntilCondition
        ConditionSpec untilCondition = pattern.getUntilCondition() != null ? ConditionSpec.fromCondition(pattern.getUntilCondition()) : null;

        // AfterMatchSkipStrategy
        AfterMatchSkipStrategySpec afterMatchSkipStrategy = AfterMatchSkipStrategySpec.fromAfterMatchSkipStrategy(pattern.getAfterMatchSkipStrategy());

        return new Builder()
                .name(name)
                .quantifier(quantifier)
                .times(times)
                .condition(condition)
                .untilCondition(untilCondition)
                .window(window)
                .afterMatchSkipStrategy(afterMatchSkipStrategy);
    }

    /**
     * Build NodeSpec from given PatternWrapper.
     */
    public static NodeSpec fromPattern(Pattern<?, ?> pattern) {
        return NodeSpec.newBuilder(pattern).buildNode();
    }

    /**
     * Converts the {@link NodeSpec} to the {@link Pattern}.
     *
     * @param classLoader The {@link ClassLoader} of the {@link Pattern}.
     * @return The converted {@link Pattern}.
     * @throws Exception Exceptions thrown while deserialization of the PatternWrapper.
     */
    public Pattern<?, ?> toPattern(
            final Pattern<?, ?> previous,
            final ConsumingStrategy consumingStrategy,
            final ClassLoader classLoader,
            Configuration globalConfiguration
    )
            throws Exception {
        Pattern<?, ?> pattern =
                new PatternWrapper(this.getName(), previous, consumingStrategy, afterMatchSkipStrategy.toAfterMatchSkipStrategy());
        final ConditionSpec conditionSpec = this.getCondition();
        if (conditionSpec != null) {
            IterativeCondition iterativeCondition = conditionSpec.toIterativeCondition(classLoader, globalConfiguration);
            if (iterativeCondition instanceof RichOrCondition) {
                pattern.or(iterativeCondition);
            } else {
                pattern.where(iterativeCondition);
            }
        }
        processQuantifier(pattern, classLoader, globalConfiguration);
        return pattern;
    }

    public void processQuantifier(Pattern<?, ?> pattern, ClassLoader classLoader, Configuration globalConfiguration) throws Exception {
        // Process quantifier's properties
        for (QuantifierProperty property : this.getQuantifier().getProperties()) {
            if (property.equals(QuantifierProperty.OPTIONAL)) {
                pattern.optional();
            } else if (property.equals(QuantifierProperty.GREEDY)) {
                pattern.greedy();
            } else if (property.equals(QuantifierProperty.LOOPING)) {
                final TimesSpec times = this.getTimes();
                if (times != null) {
                    TimesSpec.TimeSpec windowTime = times.getWindowTime();
                    pattern.timesOrMore(times.getFrom(), windowTime != null ? windowTime.toTime() : null);
                }
            } else if (property.equals(QuantifierProperty.TIMES)) {
                final TimesSpec times = this.getTimes();
                if (times != null) {
                    pattern.times(times.getFrom(), times.getTo());
                }
            }
        }

        // Process innerConsumingStrategy of the quantifier
        final ConsumingStrategy innerConsumingStrategy =
                this.getQuantifier().getInnerConsumingStrategy();
        if (innerConsumingStrategy.equals(ConsumingStrategy.SKIP_TILL_ANY)) {
            pattern.allowCombinations();
        } else if (innerConsumingStrategy.equals(ConsumingStrategy.STRICT)) {
            pattern.consecutive();
        }

        // Process until condition
        final ConditionSpec untilCondition = this.getUntilCondition();
        if (untilCondition != null) {
            final IterativeCondition iterativeCondition =
                    untilCondition.toIterativeCondition(classLoader, globalConfiguration);
            pattern.until(iterativeCondition);
        }
        // Add window semantics
        if (window != null) {
            pattern.within(this.window.getTime(), this.window.getType());
        }

    }

    public String getName() {
        return name;
    }

    public PatternNodeType getType() {
        return type;
    }

    public AfterMatchSkipStrategySpec getAfterMatchSkipStrategy() {
        return afterMatchSkipStrategy;
    }

    public WindowSpec getWindow() {
        return window;
    }

    public QuantifierSpec getQuantifier() {
        return quantifier;
    }

    public ConditionSpec getCondition() {
        return condition;
    }

    @Nullable
    public TimesSpec getTimes() {
        return times;
    }


    @Nullable
    public ConditionSpec getUntilCondition() {
        return untilCondition;
    }

    /**
     * Type of Node.
     */
    public enum PatternNodeType {
        // ATOMIC Node is the basic PatternWrapper
        ATOMIC,
        // COMPOSITE Node is a Graph
        COMPOSITE
    }

    /**
     * The Builder for ModeSpec.
     */
    public static final class Builder {
        private String name;
        private QuantifierSpec quantifier;
        private ConditionSpec condition;

        private GraphSpec graph;
        private WindowSpec window;
        private @Nullable TimesSpec times;
        private @Nullable ConditionSpec untilCondition;
        private AfterMatchSkipStrategySpec afterMatchSkipStrategy;

        private Builder() {
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder quantifier(QuantifierSpec quantifier) {
            this.quantifier = quantifier;
            return this;
        }


        public Builder graph(GraphSpec graph) {
            this.graph = graph;
            return this;
        }

        public Builder condition(ConditionSpec condition) {
            this.condition = condition;
            return this;
        }

        public Builder window(WindowSpec window) {
            this.window = window;
            return this;
        }

        public Builder times(TimesSpec times) {
            this.times = times;
            return this;
        }

        public Builder untilCondition(ConditionSpec untilCondition) {
            this.untilCondition = untilCondition;
            return this;
        }

        public Builder afterMatchSkipStrategy(AfterMatchSkipStrategySpec afterMatchSkipStrategy) {
            this.afterMatchSkipStrategy = afterMatchSkipStrategy;
            return this;
        }

        public NodeSpec buildNode() {
            return new NodeSpec(this.name, this.quantifier, this.condition, times, untilCondition, window, afterMatchSkipStrategy);
        }

        public GroupNodeSpec buildGroup() {
            return new GroupNodeSpec(name, quantifier, null, graph, times, untilCondition, window, afterMatchSkipStrategy);
        }
    }
}
