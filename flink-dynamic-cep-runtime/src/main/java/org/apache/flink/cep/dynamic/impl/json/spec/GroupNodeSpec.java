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

import org.apache.flink.cep.pattern.GroupPattern;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.Quantifier;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

/**
 * The Graph is used to describe a complex Pattern which contains Nodes(i.e {@link Pattern}) and
 * Edges. The Node of a Graph can be a embedded Graph as well. This class is to (de)serialize Graphs
 * in json format.
 */
public class GroupNodeSpec extends NodeSpec {

    private final GraphSpec graph;


    public GroupNodeSpec(
            @JsonProperty("name") String name,
            @JsonProperty("quantifier") QuantifierSpec quantifier,
            @JsonProperty("condition") ConditionSpec condition,
            @JsonProperty("graph") GraphSpec graph,
            @Nullable @JsonProperty("times") TimesSpec times,
            @Nullable @JsonProperty("untilCondition") ConditionSpec untilCondition,
            @JsonProperty("window") WindowSpec window,
            @JsonProperty("afterMatchSkipStrategy") AfterMatchSkipStrategySpec afterMatchSkipStrategy) {
        super(name, quantifier, condition, times, untilCondition, window, afterMatchSkipStrategy, PatternNodeType.COMPOSITE);
        this.graph = graph;
    }

    public static GroupNodeSpec fromPattern(Pattern<?, ?> pattern) {
        GraphSpec graph = GraphSpec.fromPattern(((GroupPattern<?, ?>) pattern).getRawPattern());
        return NodeSpec.newBuilder(pattern).graph(graph).buildGroup();
    }

    /**
     * Converts the {@link GroupNodeSpec} to the {@link Pattern}.
     *
     * @param classLoader The {@link ClassLoader} of the {@link Pattern}.
     * @return The converted {@link Pattern}.
     * @throws Exception Exceptions thrown while deserialization of the Pattern.
     */
    @Override
    public Pattern<?, ?> toPattern(final Pattern<?, ?> previous,
                                   final Quantifier.ConsumingStrategy consumingStrategy,
                                   final ClassLoader classLoader, final Configuration globalConfiguration) throws Exception {

        Pattern<?, ?> pattern = graph.toPattern(classLoader, globalConfiguration);

        pattern = buildGroupPattern(consumingStrategy, pattern, previous, previous == null);

        processQuantifier(pattern, classLoader, globalConfiguration);

        return pattern;
    }

    public static GroupPattern<?, ?> buildGroupPattern(
            Quantifier.ConsumingStrategy strategy,
            Pattern<?, ?> currentPattern,
            Pattern<?, ?> prevPattern,
            boolean isBeginPattern) {
        // construct GroupPattern
        if (strategy.equals(Quantifier.ConsumingStrategy.STRICT)) {
            if (isBeginPattern) {
                currentPattern = Pattern.begin(currentPattern);
            } else {
                currentPattern = prevPattern.next((Pattern) currentPattern);
            }
        } else if (strategy.equals(Quantifier.ConsumingStrategy.SKIP_TILL_NEXT)) {
            currentPattern = prevPattern.followedBy((Pattern) currentPattern);
        } else if (strategy.equals(Quantifier.ConsumingStrategy.SKIP_TILL_ANY)) {
            currentPattern = prevPattern.followedByAny((Pattern) currentPattern);
        }
        return (GroupPattern<?, ?>) currentPattern;
    }

    public GraphSpec getGraph() {
        return graph;
    }

}
