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

package org.apache.flink.cep.coordinator;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.RecreateOnResetOperatorCoordinator;

/**
 * The provider of {@link RuleProcessorCoordinator}.
 */
public class RuleProcessorCoordinatorProvider
        extends RecreateOnResetOperatorCoordinator.Provider {

    private static final long serialVersionUID = 1L;

    private final String operatorName;

    private final String ruleQueueId;

    /**
     * Construct the {@link RuleProcessorCoordinatorProvider}.
     *
     * @param operatorName The name of the operator.
     * @param operatorID   The ID of the operator this coordinator corresponds to.
     */
    public RuleProcessorCoordinatorProvider(
            String operatorName,
            OperatorID operatorID,
            String ruleQueueId) {
        super(operatorID);
        this.operatorName = operatorName;
        this.ruleQueueId = ruleQueueId;
    }

    @Override
    public OperatorCoordinator getCoordinator(OperatorCoordinator.Context context) {
        final String coordinatorThreadName = "RuleProcessorCoordinator-" + operatorName;
        CoordinatorExecutorThreadFactory coordinatorThreadFactory =
                new CoordinatorExecutorThreadFactory(
                        coordinatorThreadName, context.getUserCodeClassloader());

        CoordinatorContext coordinatorContext =
                new CoordinatorContext(coordinatorThreadFactory, context);
        return new RuleProcessorCoordinator(
                operatorName, ruleQueueId, coordinatorContext);
    }

}
