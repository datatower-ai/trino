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
package io.trino.plugin.datatower.functions;

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.datatower.functions.aggregation.behavior.UserEventSessionAggFunction;
import io.trino.plugin.datatower.functions.aggregation.collect.DateCollectFunction;
import io.trino.plugin.datatower.functions.aggregation.collect.DateValueArrayCollectFunction;
import io.trino.plugin.datatower.functions.aggregation.collect.DateValueCollectFunction;
import io.trino.plugin.datatower.functions.aggregation.funnel.FunnelDateMaxStepAggFunction;
import io.trino.plugin.datatower.functions.aggregation.funnel.FunnelTotalMaxStepAggFunction;
import io.trino.plugin.datatower.functions.aggregation.funnel.collect.FunnelPackedTimeCollectAggFunction;
import io.trino.plugin.datatower.functions.aggregation.interval.IntervalAggFunction;
import io.trino.plugin.datatower.functions.aggregation.interval.IntervalAggFunctionGrouped;
import io.trino.plugin.datatower.functions.aggregation.interval.IntervalAggFunctionGroupedNew;
import io.trino.plugin.datatower.functions.aggregation.retention.RetentionLostDateAggFunction;
import io.trino.plugin.datatower.functions.aggregation.retention.RetentionLostDateCollectAggFunction;
import io.trino.plugin.datatower.functions.aggregation.retention.RetentionLostDateQuotaArrayAggFunction;
import io.trino.plugin.datatower.functions.aggregation.retention.formula.RetentionLostDateSimStatFormulaCollectAggFunction;
import io.trino.plugin.datatower.functions.aggregation.retention.formula.RetentionLostDateSimStatFormulaCollectAggFunctionNew;
import io.trino.plugin.datatower.functions.aggregation.retention.formula.RetentionLostDateSimStatFormulaQuotaCollectAggFunction;
import io.trino.plugin.datatower.functions.aggregation.retention.formula.RetentionLostDateSimStatFormulaQuotaCollectAggFunctionNew;
import io.trino.plugin.datatower.functions.aggregation.retention.simstat.RetentionLostDateSimStatCollectAggFunction;
import io.trino.plugin.datatower.functions.aggregation.retention.simstat.RetentionLostDateSimStatQuotaCollectAggFunction;
import io.trino.plugin.datatower.functions.aggregation.retention.simstat.RetentionLostDateSimStatQuotaCollectAggFunctionNew;
import io.trino.plugin.datatower.functions.scalar.common.DateTimeFunctions;
import io.trino.plugin.datatower.functions.scalar.distribute.DistributeFunction;
import io.trino.plugin.datatower.functions.scalar.funnel.FunnelFunctionPacked;
import io.trino.plugin.datatower.functions.scalar.interval.IntervalFunctionGrouped;
import io.trino.plugin.datatower.functions.scalar.path.PathFunction;
import io.trino.plugin.datatower.functions.scalar.retention.RetentionSeqFunction;
import io.trino.spi.Plugin;

/**
 * @author liulin
 */
public class UdfPlugin
        implements Plugin
{
    @Override
    public ImmutableSet<Class<?>> getFunctions()
    {
        return ImmutableSet.of(
                FunnelPackedTimeCollectAggFunction.class,
                FunnelDateMaxStepAggFunction.class,
                FunnelTotalMaxStepAggFunction.class,
                FunnelFunctionPacked.class,
                DistributeFunction.class,
                DateTimeFunctions.class,
                DateCollectFunction.class,
                IntervalAggFunction.class,
                IntervalAggFunctionGrouped.class,
                IntervalAggFunctionGroupedNew.class,
                IntervalFunctionGrouped.class,
                UserEventSessionAggFunction.class,
                PathFunction.class,
                DateValueCollectFunction.class,
                DateValueArrayCollectFunction.class,
                RetentionSeqFunction.class,
                RetentionLostDateAggFunction.class,
                RetentionLostDateCollectAggFunction.class,
                RetentionLostDateQuotaArrayAggFunction.class,
                RetentionLostDateSimStatCollectAggFunction.class,
                RetentionLostDateSimStatQuotaCollectAggFunction.class,
                RetentionLostDateSimStatQuotaCollectAggFunctionNew.class,
                RetentionLostDateSimStatFormulaCollectAggFunction.class,
                RetentionLostDateSimStatFormulaCollectAggFunctionNew.class,
                RetentionLostDateSimStatFormulaQuotaCollectAggFunction.class,
                RetentionLostDateSimStatFormulaQuotaCollectAggFunctionNew.class);
    }
}
