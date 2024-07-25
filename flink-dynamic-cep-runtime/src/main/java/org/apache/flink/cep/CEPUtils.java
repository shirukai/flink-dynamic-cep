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

package org.apache.flink.cep;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;

import org.apache.flink.cep.discover.RuleDiscovererFactory;
import org.apache.flink.cep.event.EventRecord;
import org.apache.flink.cep.operator.CepRuleProcessorOperatorFactory;
import org.apache.flink.cep.operator.RuleDistributorOperatorFactory;
import org.apache.flink.cep.operator.UdfRuleProcessorOperatorFactory;
import org.apache.flink.cep.types.RuleRowKey;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * Utility class for complex event processing.
 *
 * <p>Methods which transform a {@link DataStream} into a {@link PatternStream} to do CEPUtils.
 */
public class CEPUtils {

    public static <T, R> SingleOutputStreamOperator<R> dynamicUdfRules(
            DataStream<T> input,
            RuleDiscovererFactory discovererFactory,
            TypeInformation<R> outTypeInfo,
            String ruleQueueId,
            String userLibDir,
            boolean keyBindingEnabled
    ) {
        final RuleDistributorOperatorFactory<T> distributorOperatorFactory =
                new RuleDistributorOperatorFactory<>(
                        ruleQueueId,
                        discovererFactory,
                        keyBindingEnabled
                );

        final UdfRuleProcessorOperatorFactory<T, R> processorDiscovererFactory =
                new UdfRuleProcessorOperatorFactory<>(ruleQueueId, userLibDir);

        TypeHint<EventRecord<T>> typeHint = new TypeHint<EventRecord<T>>() {
        };
        TypeInformation<EventRecord<T>> eventTypeInformation = TypeInformation.of(typeHint);
        if (input instanceof KeyedStream) {
            KeyedStream<T, ?> keyedStream = (KeyedStream<T, ?>) input;
            KeySelector<T, ?> keySelector = keyedStream.getKeySelector();
            return keyedStream.transform("RuleDistributorOperator", eventTypeInformation, distributorOperatorFactory)
                    .keyBy(ruleKeySelector(keySelector)).transform("UdfRuleProcessorOperator", outTypeInfo, processorDiscovererFactory);
        } else {
            return input.keyBy(RuleRowKey.nullRowKeySelector())
                    .transform("RuleDistributorOperator", eventTypeInformation, distributorOperatorFactory)
                    .forceNonParallel()
                    .keyBy((KeySelector<EventRecord<T>, RuleRowKey<?>>) value -> RuleRowKey.of(value.getRuleId()))
                    .transform("UdfRuleProcessorOperator", outTypeInfo, processorDiscovererFactory);
        }
    }


    public static <T, R> SingleOutputStreamOperator<R> dynamicCepRules(
            DataStream<T> input,
            RuleDiscovererFactory discovererFactory,
            TimeBehaviour timeBehaviour,
            TypeInformation<R> outTypeInfo,
            String ruleQueueId,
            String userLibDir,
            boolean keyBindingEnabled
    ) {
        final RuleDistributorOperatorFactory<T> distributorOperatorFactory =
                new RuleDistributorOperatorFactory<>(
                        ruleQueueId,
                        discovererFactory,
                        keyBindingEnabled
                );

        final CepRuleProcessorOperatorFactory<T, R> processorDiscovererFactory =
                new CepRuleProcessorOperatorFactory<>(input.getType().createSerializer(input.getExecutionConfig()),
                        ruleQueueId,
                        userLibDir,
                        timeBehaviour,
                        null,
                        null
                );
        TypeHint<EventRecord<T>> typeHint = new TypeHint<EventRecord<T>>() {
        };
        TypeInformation<EventRecord<T>> eventTypeInformation = TypeInformation.of(typeHint);
        if (input instanceof KeyedStream) {
            KeyedStream<T, ?> keyedStream = (KeyedStream<T, ?>) input;
            KeySelector<T, ?> keySelector = keyedStream.getKeySelector();
            return keyedStream.transform("RuleDistributorOperator", eventTypeInformation, distributorOperatorFactory)
                    .keyBy(ruleKeySelector(keySelector))
                    .transform("CepRuleProcessorOperator", outTypeInfo, processorDiscovererFactory);
        } else {
            return input.keyBy(RuleRowKey.nullRowKeySelector())
                    .transform("RuleDistributorOperator", eventTypeInformation, distributorOperatorFactory)
                    .forceNonParallel()
                    .keyBy((KeySelector<EventRecord<T>, RuleRowKey<?>>) value -> RuleRowKey.of(value.getRuleId()))
                    .transform("CepRuleProcessorOperator", outTypeInfo, processorDiscovererFactory);
        }
    }

    public static <K, T> KeySelector<EventRecord<T>, RuleRowKey<K>> ruleKeySelector(KeySelector<T, K> keySelector) {
        return new KeySelector<EventRecord<T>, RuleRowKey<K>>() {
            @Override
            public RuleRowKey<K> getKey(EventRecord<T> value) throws Exception {
                K userKey = keySelector.getKey(value.getEvent());
                return RuleRowKey.of(value.getRuleId(), userKey);

            }
        };
    }



}
