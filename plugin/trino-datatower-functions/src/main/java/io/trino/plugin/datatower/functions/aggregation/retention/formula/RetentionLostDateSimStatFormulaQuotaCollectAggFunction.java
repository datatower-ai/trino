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

import io.airlift.slice.Slice;
import io.trino.plugin.datatower.functions.aggregation.collect.DateValueArrayCollectFunction;
import io.trino.plugin.datatower.functions.aggregation.retention.QuotaType;
import io.trino.plugin.datatower.functions.aggregation.retention.RetentionLostDateCollectAggFunction;
import io.trino.plugin.datatower.functions.aggregation.retention.simstat.RetentionLostDateSimStatQuotaCollectAggFunction;
import io.trino.plugin.datatower.functions.state.collect.DateSet;
import io.trino.plugin.datatower.functions.state.collect.DateValues;
import io.trino.plugin.datatower.functions.util.ChronologyUtil;
import io.trino.plugin.datatower.functions.util.DateTimes;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.VarcharType;
import org.apache.commons.lang3.mutable.MutableInt;
import org.joda.time.DateTimeField;
import org.joda.time.chrono.ISOChronology;

import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author liulin
 */

@AggregationFunction("retention_lost_date_sim_stat_formula_quota_array_agg")
public final class RetentionLostDateSimStatFormulaQuotaCollectAggFunction
{
    private RetentionLostDateSimStatFormulaQuotaCollectAggFunction()
    {
    }

    @InputFunction
    @LiteralParameters({"x", "y"})
    public static void input(@AggregationState RetentionLostSimStatFormulaQuotaState state,
            @SqlType("varbinary") Slice initSlice,
            @SqlType("varbinary") Slice returnSlice,
            @SqlType("varbinary") Slice simStatSlice,
            @SqlType("array(bigint)") Block statTypeBlock,
            @SqlType("array(varchar)") Block quotaTypeBlock,
            @SqlType("array(bigint)") Block quotaDayBlock,
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

        int quotaTypeNum = quotaTypeBlock.getPositionCount();
        checkArgument(quotaTypeNum > 0, "quota type array is empty");
        int quotaIndexNum = quotaDayBlock.getPositionCount();
        checkArgument(quotaIndexNum > 0, "quota index array is empty");
        checkArgument(quotaIndexNum == quotaTypeNum, "quota type and quota quantity are not equal");
        long[] quotaTypeArr = new long[quotaTypeNum];
        for (int k = 0; k < quotaTypeNum; ++k) {
            String quotaTypeStr = VarcharType.VARCHAR.getSlice(quotaTypeBlock, k).toStringUtf8();
            QuotaType quotaType = QuotaType.getByName(quotaTypeStr);
            quotaTypeArr[k] = quotaType.getCode();
        }

        long[] quotaIndexArr = new long[quotaIndexNum];
        for (int k = 0; k < quotaIndexNum; ++k) {
            quotaIndexArr[k] = BigintType.BIGINT.getLong(quotaDayBlock, k);
        }

        RetentionLostSimStatFormulaQuota retentionLostSimStatFormulaQuota = new RetentionLostSimStatFormulaQuota(initSize, statTypes, table, formula, quotaTypeArr, quotaIndexArr);
        if (state.getData() == null) {
            state.setData(retentionLostSimStatFormulaQuota);
        }
        else {
            RetentionLostSimStatFormulaQuota merged = RetentionLostSimStatFormulaQuota.merge(state.getData(), retentionLostSimStatFormulaQuota);
            state.setData(merged);
        }
    }

    @CombineFunction
    public static void combine(@AggregationState RetentionLostSimStatFormulaQuotaState state, @AggregationState RetentionLostSimStatFormulaQuotaState otherState)
    {
        if (state.getData() == null) {
            state.setData(otherState.getData());
        }
        else if (otherState.getData() != null) {
            RetentionLostSimStatFormulaQuota merged = RetentionLostSimStatFormulaQuota.merge(state.getData(), otherState.getData());
            state.setData(merged);
        }
    }

    @OutputFunction("map(timestamp(6),array(double))")
    public static void output(@AggregationState RetentionLostSimStatFormulaQuotaState state, BlockBuilder out)
    {
        RetentionLostSimStatFormulaQuota retentionLostSimStatFormulaQuota = state.getData();
        if (retentionLostSimStatFormulaQuota == null) {
            out.appendNull();
            return;
        }
        else {
            Map<Long, Object> map = retentionLostSimStatFormulaQuota.toMap();
            long[] quotaTypeArr = retentionLostSimStatFormulaQuota.getQuotaType();
            long[] quotaIndexArr = retentionLostSimStatFormulaQuota.getQuotaIndex();

            ((MapBlockBuilder) out).buildEntry((keyBuilder, valueBuilder) -> {
                Set<Long> dateSet = ((Map<Long, long[]>) map.get(0L)).keySet();
                for (Long dateTs : dateSet) {
                    TimestampType.TIMESTAMP_MILLIS.writeLong(keyBuilder, DateTimes.millisToMicros(dateTs));
                    ((ArrayBlockBuilder) valueBuilder).buildEntry(elementBuilder -> {
                        for (int i = 0; i < quotaTypeArr.length; i++) {
                            long quotaTypeCode = quotaTypeArr[i];
                            QuotaType quotaType = QuotaType.getByCode((int) quotaTypeCode);
                            int quotaIndex = (int) quotaIndexArr[i] + 1;
                            double value = RetentionLostDateSimStatQuotaCollectAggFunction.getValue(quotaType, quotaIndex, dateTs, map);
                            DoubleType.DOUBLE.writeDouble(elementBuilder, value);
                        }
                    });
                }
            });

//            BlockBuilder blockBuilder = out.beginBlockEntry();
//            Set<Long> dateSet = ((Map<Long, long[]>) map.get(0L)).keySet();
//            for (Long dateTs : dateSet) {
//                TimestampType.TIMESTAMP_MILLIS.writeLong(out, DateTimes.millisToMicros(dateTs));
//                ArrayType valueArrayType = new ArrayType(DoubleType.DOUBLE);
//                int quotaTypeArrCount = quotaTypeArr.length;
//                BlockBuilder valueArrayBuilder = DoubleType.DOUBLE.createBlockBuilder(null, quotaTypeArrCount - 1, 8);
//                for (int i = 0; i < quotaTypeArr.length; i++) {
//                    long quotaTypeCode = quotaTypeArr[i];
//                    QuotaType quotaType = QuotaType.getByCode((int) quotaTypeCode);
//                    int quotaIndex = (int) quotaIndexArr[i] + 1;
//                    double value = RetentionLostDateSimStatQuotaCollectAggFunction.getValue(quotaType, quotaIndex, dateTs, map);
//                    DoubleType.DOUBLE.writeDouble(valueArrayBuilder, value);
//                }
//                valueArrayType.writeObject(out, valueArrayBuilder.build());
//            }
        }
//        out.closeEntry();
    }
}
