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
package io.trino.plugin.datatower.functions.aggregation.retention.formula;

import com.alibaba.fastjson2.JSON;
import io.airlift.slice.Slice;
import io.trino.plugin.datatower.functions.aggregation.collect.DateValueArrayCollectFunction;
import io.trino.plugin.datatower.functions.aggregation.retention.RetentionLostDateCollectAggFunction;
import io.trino.plugin.datatower.functions.state.collect.DateSet;
import io.trino.plugin.datatower.functions.state.collect.DateValues;
import io.trino.plugin.datatower.functions.util.ChronologyUtil;
import io.trino.plugin.datatower.functions.util.DateTimes;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.VarcharType;
import org.apache.commons.lang3.mutable.MutableInt;
import org.joda.time.DateTimeField;
import org.joda.time.chrono.ISOChronology;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author liulin
 */

@AggregationFunction("retention_lost_date_sim_stat_formula_collect_agg")
public final class RetentionLostDateSimStatFormulaCollectAggFunction
{
    private RetentionLostDateSimStatFormulaCollectAggFunction()
    {
    }

    @InputFunction
    @LiteralParameters({"x", "y"})
    public static void input(@AggregationState RetentionLostSimStatFormulaState state,
            @SqlType("varbinary") Slice initSlice,
            @SqlType("varbinary") Slice returnSlice,
            @SqlType("varbinary") Slice simStatSlice,
            @SqlType("array(bigint)") Block statTypeBlock,
            @SqlType("integer") long retenNum0,
            @SqlType("varchar(x)") Slice unit,
            @SqlType("varchar(y)") Slice formula)
    {
        int formulaVarNum = statTypeBlock.getPositionCount();
        checkArgument(formulaVarNum > 0, "stat type array is empty");
        long[] statTypes = new long[formulaVarNum];

        for (int k = 0; k < formulaVarNum; ++k) {
            statTypes[k] = BigintType.BIGINT.getLong(statTypeBlock, k);
        }

        DateTimeField dateTimeField = ChronologyUtil.getRetentionTimestampField(ISOChronology.getInstanceUTC(), unit);
        long[] initMillis = DateSet.deserialize(initSlice).toEpochMillisArray();
        long[] returnMillis = DateSet.deserialize(returnSlice).toEpochMillisArray();
        int initSize = initMillis.length;
        RetentionLostSimStatFormulaItem[] table = new RetentionLostSimStatFormulaItem[initSize];
        int retenNum = (int) retenNum0;
        int n = retenNum + 2;
        int renInit = 1;
        MutableInt returnStartIndex = new MutableInt();
        int simStatStartIndex = 0;

        for (int initIndex = 0; initIndex < initSize; ++initIndex) {
            long initDate = initMillis[initIndex];
            long[] row = RetentionLostDateCollectAggFunction.getRetentionLostForInitDate(returnMillis, retenNum, dateTimeField, returnStartIndex, initDate);
            double[][] simStats = new double[formulaVarNum][];

            for (int k = 0; k < formulaVarNum; ++k) {
                simStats[k] = new double[n];
            }

            if (simStatSlice != null) {
                DateValues[] simStatArray = DateValueArrayCollectFunction.deserialize(simStatSlice, formulaVarNum);
                int simStatArraySize = simStatArray.length;

                for (int simStatIndex = simStatStartIndex; simStatIndex < simStatArraySize; ++simStatIndex) {
                    DateValues dateValues = simStatArray[simStatIndex];
                    long simStatDate = DateTimes.microsToMillis(dateValues.epochMicros);
                    if (simStatDate < initDate) {
                        ++simStatStartIndex;
                    }
                    else {
                        int gap = dateTimeField.getDifference(simStatDate, initDate);
                        if (gap > retenNum) {
                            break;
                        }

                        if (row[renInit + 1 + gap] == 1L) {
                            double[] values = dateValues.values;

                            for (int k = 0; k < formulaVarNum; ++k) {
                                double[] simStat = simStats[k];
                                double statVal = values[k];
                                simStat[1 + gap] += statVal;
                            }
                        }
                    }
                }
            }

            table[initIndex] = new RetentionLostSimStatFormulaItem(row, simStats);
        }

        RetentionLostSimStatFormula retentionLostSimStatFormula = new RetentionLostSimStatFormula(initSize, statTypes, table, formula);
        if (state.getData() == null) {
            state.setData(retentionLostSimStatFormula);
        }
        else {
            RetentionLostSimStatFormula merged = RetentionLostSimStatFormula.merge(state.getData(), retentionLostSimStatFormula);
            state.setData(merged);
        }
    }

    @CombineFunction
    public static void combine(@AggregationState RetentionLostSimStatFormulaState state, @AggregationState RetentionLostSimStatFormulaState otherState)
    {
        if (state.getData() == null) {
            state.setData(otherState.getData());
        }
        else if (otherState.getData() != null) {
            RetentionLostSimStatFormula merged = RetentionLostSimStatFormula.merge(state.getData(), otherState.getData());
            state.setData(merged);
        }
    }

    @OutputFunction("varchar")
    public static void output(@AggregationState RetentionLostSimStatFormulaState state, BlockBuilder out)
    {
        RetentionLostSimStatFormula retentionLostSimStatFormula = state.getData();
        if (retentionLostSimStatFormula == null) {
            out.appendNull();
            return;
        }
        Map<Long, Object> map = retentionLostSimStatFormula.toMap();
        String jsonString = JSON.toJSONString(map);
        VarcharType.createVarcharType(jsonString.length()).writeString(out, jsonString);
    }
}
