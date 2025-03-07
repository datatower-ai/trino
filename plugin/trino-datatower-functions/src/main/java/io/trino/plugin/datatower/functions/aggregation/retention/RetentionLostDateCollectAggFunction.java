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
package io.trino.plugin.datatower.functions.aggregation.retention;

import io.airlift.slice.Slice;
import io.trino.plugin.datatower.functions.state.collect.DateSet;
import io.trino.plugin.datatower.functions.util.ChronologyUtil;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import org.apache.commons.lang3.mutable.MutableInt;
import org.joda.time.DateTimeField;
import org.joda.time.chrono.ISOChronology;

/**
 * @author liulin
 */
@AggregationFunction("retention_lost_date_collect_agg")
public final class RetentionLostDateCollectAggFunction
{
    private RetentionLostDateCollectAggFunction()
    {
    }

    @InputFunction
    @LiteralParameters("x")
    public static void input(@AggregationState RetentionLostDataState state,
            @SqlType("varbinary") Slice initSlice,
            @SqlType("varbinary") Slice returnSlice,
            @SqlType("integer") long retenNum,
            @SqlType("varchar(x)") Slice unit)
    {
        //获取时间单位
        DateTimeField dateTimeField = ChronologyUtil.getRetentionTimestampField(ISOChronology.getInstanceUTC(), unit);
        //反序列化初始事件日期
        long[] initMillis = DateSet.deserialize(initSlice).toEpochMillisArray();
        //反序列化回访事件日期
        long[] returnMillis = DateSet.deserialize(returnSlice).toEpochMillisArray();
        //初始事件长度
        int initSize = initMillis.length;
        //创建一个二维数组
        long[][] table = new long[initSize][];
        //回访事件开始的索引
        MutableInt returnStartIndex = new MutableInt();
        //遍历初始日期
        for (int initIndex = 0; initIndex < initSize; ++initIndex) {
            long initDate = initMillis[initIndex];
            long[] row = getRetentionLostForInitDate(returnMillis, (int) retenNum, dateTimeField, returnStartIndex, initDate);
            table[initIndex] = row;
        }

        RetentionLostData retentionLostData;
        if (state.getData() == null) {
            retentionLostData = new RetentionLostData(table, initSize);
            state.setData(retentionLostData);
        }
        else {
            retentionLostData = RetentionLostData.merge(state.getData(), new RetentionLostData(table, initSize));
            state.setData(retentionLostData);
        }
    }

    public static long[] getRetentionLostForInitDate(long[] returnMillis, int retenNum, DateTimeField dateTimeField, MutableInt returnStartIndex, long initDate)
    {
        int n = retenNum + 2;
        int rowLength = 1 + n + n;
        int renInit = 1;
        int lostInit = 1 + n;
        long[] row = new long[rowLength];
        row[0] = initDate;
        row[renInit] = 1L;
        boolean lostFilled = false;
        if (returnMillis != null) {
            int returnMillisLength = returnMillis.length;
            for (int returnIndex = returnStartIndex.intValue(); returnIndex < returnMillisLength; ++returnIndex) {
                long returnDate = returnMillis[returnIndex];
                if (returnDate < initDate) {
                    returnStartIndex.increment();
                }
                else {
                    int gap = dateTimeField.getDifference(returnDate, initDate);
                    if (gap > retenNum) {
                        break;
                    }
                    row[renInit + 1 + gap] = 1L;
                    if (gap > 0 && !lostFilled) {
                        for (int i = lostInit; i < lostInit + 1 + gap; ++i) {
                            row[i] = 1L;
                        }
                        for (int i = lostInit + 1 + gap; i < rowLength; ++i) {
                            row[i] = 0L;
                        }
                        lostFilled = true;
                    }
                }
            }
        }
        if (!lostFilled) {
            for (int i = lostInit; i < rowLength; ++i) {
                row[i] = 1L;
            }
        }
        return row;
    }

    @CombineFunction
    public static void combine(@AggregationState RetentionLostDataState state,
            @AggregationState RetentionLostDataState otherState)
    {
        RetentionLostDateAggFunction.combine(state, otherState);
    }

    @OutputFunction("varchar")
    public static void output(@AggregationState RetentionLostDataState state, BlockBuilder out)
    {
        RetentionLostDateAggFunction.output(state, out);
    }
}
