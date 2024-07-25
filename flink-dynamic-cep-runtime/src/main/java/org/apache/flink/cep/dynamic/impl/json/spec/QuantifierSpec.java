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

import org.apache.flink.cep.pattern.Quantifier;
import org.apache.flink.cep.pattern.Quantifier.ConsumingStrategy;
import org.apache.flink.cep.pattern.Quantifier.QuantifierProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.EnumSet;

/**
 * This class is to (de)serialize {@link Quantifier} in json format. It contains Times and
 * untilCondition as well, which are logically a part of a Quantifier.
 */
public class QuantifierSpec {

    private final ConsumingStrategy consumingStrategy;
    private ConsumingStrategy innerConsumingStrategy = ConsumingStrategy.SKIP_TILL_NEXT;
    private final EnumSet<QuantifierProperty> properties;

    public QuantifierSpec(
            @JsonProperty("consumingStrategy") ConsumingStrategy consumingStrategy,
            @JsonProperty("innerConsumingStrategy") ConsumingStrategy innerConsumingStrategy,
            @JsonProperty("properties") EnumSet<QuantifierProperty> properties) {
        this.consumingStrategy = consumingStrategy;
        this.properties = properties;
        this.innerConsumingStrategy = innerConsumingStrategy;
    }

    public QuantifierSpec(Quantifier quantifier) {
        this.innerConsumingStrategy = quantifier.getInnerConsumingStrategy();
        this.consumingStrategy = quantifier.getConsumingStrategy();
        this.properties = EnumSet.noneOf(QuantifierProperty.class);
        for (QuantifierProperty property : QuantifierProperty.values()) {
            if (quantifier.hasProperty(property)) {
                this.properties.add(property);
            }
        }

    }

    public ConsumingStrategy getInnerConsumingStrategy() {
        return innerConsumingStrategy;
    }

    public ConsumingStrategy getConsumingStrategy() {
        return consumingStrategy;
    }

    public EnumSet<QuantifierProperty> getProperties() {
        return properties;
    }


}
