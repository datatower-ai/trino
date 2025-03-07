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
package io.trino.plugin.datatower.functions.aggregation.retention.simstat;

import com.alibaba.fastjson2.JSON;
import io.airlift.slice.Slice;
import io.trino.plugin.datatower.functions.aggregation.collect.DateValueCollectFunction;
import io.trino.plugin.datatower.functions.aggregation.retention.RetentionLostDateCollectAggFunction;
import io.trino.plugin.datatower.functions.state.collect.DateSet;
import io.trino.plugin.datatower.functions.state.collect.DateValue;
import io.trino.plugin.datatower.functions.util.ChronologyUtil;
import io.trino.plugin.datatower.functions.util.DateTimes;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.VarcharType;
import org.apache.commons.lang3.mutable.MutableInt;
import org.joda.time.DateTimeField;
import org.joda.time.chrono.ISOChronology;

import java.util.Map;

/**
 * @author liulin
 */
@AggregationFunction("retention_lost_date_sim_stat_collect_agg")
public final class RetentionLostDateSimStatCollectAggFunction
{
    private RetentionLostDateSimStatCollectAggFunction()
    {
    }

    @InputFunction
    @LiteralParameters("x")
    public static void input(@AggregationState RetentionLostSimStatState state,
            @SqlType("varbinary") Slice initSlice,
            @SqlType("varbinary") Slice returnSlice,
            @SqlType("varbinary") Slice simStatSlice,
            @SqlType("bigint") long statType,
            @SqlType("integer") long retenNum0,
            @SqlType("varchar(x)") Slice unit)
    {
        DateTimeField dateTimeField = ChronologyUtil.getRetentionTimestampField(ISOChronology.getInstanceUTC(), unit);
        long[] initMillis = DateSet.deserialize(initSlice).toEpochMillisArray();
        long[] returnMillis = DateSet.deserialize(returnSlice).toEpochMillisArray();
        int initSize = initMillis.length;
        RetentionLostSimStatItem[] table = new RetentionLostSimStatItem[initSize];
        int retenNum = (int) retenNum0;
        int n = retenNum + 2;
        int renInit = 1;
        MutableInt returnStartIndex = new MutableInt();
        int simStatStartIndex = 0;

        for (int initIndex = 0; initIndex < initSize; ++initIndex) {
            long initDate = initMillis[initIndex];
            long[] row = RetentionLostDateCollectAggFunction.getRetentionLostForInitDate(returnMillis, retenNum, dateTimeField, returnStartIndex, initDate);
            double[] simStatVals = new double[n];
            if (simStatSlice != null) {
                DateValue[] simStatArray = DateValueCollectFunction.deserialize(simStatSlice);
                int simStatArraySize = simStatArray.length;

                for (int simStatIndex = simStatStartIndex; simStatIndex < simStatArraySize; ++simStatIndex) {
                    DateValue dateValue = simStatArray[simStatIndex];
                    long simStatDate = DateTimes.microsToMillis(dateValue.epochMicros);
                    if (simStatDate < initDate) {
                        ++simStatStartIndex;
                    }
                    else {
                        int gap = dateTimeField.getDifference(simStatDate, initDate);
                        if (gap > retenNum) {
                            break;
                        }

                        if (row[renInit + 1 + gap] == 1L) {
                            simStatVals[1 + gap] += dateValue.value;
                        }
                    }
                }
            }

            table[initIndex] = new RetentionLostSimStatItem(row, simStatVals);
        }

        RetentionLostSimStat retentionLostSimStat = new RetentionLostSimStat(table, initSize, statType);
        if (state.getData() == null) {
            state.setData(retentionLostSimStat);
        }
        else {
            RetentionLostSimStat merged = RetentionLostSimStat.merge(state.getData(), retentionLostSimStat);
            state.setData(merged);
        }
    }

    @CombineFunction
    public static void combine(@AggregationState RetentionLostSimStatState state, @AggregationState RetentionLostSimStatState otherState)
    {
        if (state.getData() == null) {
            state.setData(otherState.getData());
        }
        else if (otherState.getData() != null) {
            RetentionLostSimStat merged = RetentionLostSimStat.merge(state.getData(), otherState.getData());
            state.setData(merged);
        }
    }

    @OutputFunction("varchar")
    public static void output(@AggregationState RetentionLostSimStatState state, BlockBuilder out)
    {
        RetentionLostSimStat retentionLostSimStat = state.getData();
        if (retentionLostSimStat == null) {
            out.appendNull();
        }
        else {
            Map<Long, Object> map = retentionLostSimStat.toMap();
            String jsonString = JSON.toJSONString(map);
            VarcharType.createVarcharType(jsonString.length()).writeString(out, jsonString);
        }
    }
}
