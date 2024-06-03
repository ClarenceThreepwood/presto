/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.presto.sql.planner;

import com.google.common.collect.ImmutableMap;

import static com.facebook.presto.SystemSessionProperties.CARDINALITY_ESTIMATION_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.HANDLE_COMPLEX_EQUI_JOINS;
import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.CardinalityEstimationStrategyWithTableConstraints;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinReorderingStrategy;

public class TestTpchCostBasedPlanWithTableConstraints
        extends TestTpchCostBasedPlan
{
    public TestTpchCostBasedPlanWithTableConstraints()
    {
        super(true,
                ImmutableMap.of(
                        "task_concurrency", "1", // these tests don't handle exchanges from local parallel
                        JOIN_REORDERING_STRATEGY, JoinReorderingStrategy.AUTOMATIC.name(),
                        JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name(),
                        HANDLE_COMPLEX_EQUI_JOINS, "true",
                        CARDINALITY_ESTIMATION_STRATEGY, CardinalityEstimationStrategyWithTableConstraints.SEED_PICK_MOST_SELECTIVE_AND_ATTENUATE.name()));
    }
}
