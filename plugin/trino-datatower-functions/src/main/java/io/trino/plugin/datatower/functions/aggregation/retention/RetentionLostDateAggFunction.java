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

import com.alibaba.fastjson2.JSON;
import io.airlift.slice.Slice;
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
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.VarcharType;
import org.apache.commons.lang3.mutable.MutableInt;
import org.joda.time.DateTimeField;
import org.joda.time.chrono.ISOChronology;

import java.util.Map;

/**
 * @author liulin
 */
@AggregationFunction("retention_lost_date_agg")
public final class RetentionLostDateAggFunction
{
    private RetentionLostDateAggFunction()
    {
    }

    @LiteralParameters("x")
    @InputFunction
    public static void input(@AggregationState RetentionLostDataState state,
            @SqlType("array(timestamp(6))") Block initBlock,
            @SqlType("array(timestamp(6))") Block returnBlock,
            @SqlType("integer") long retenNum,
            @SqlType("varchar(x)") Slice unit)
    {
        DateTimeField dateTimeField = ChronologyUtil.getRetentionTimestampField(ISOChronology.getInstanceUTC(), unit);
        int initSize = initBlock.getPositionCount();
        long[][] table = new long[initSize][];
        MutableInt returnStartIndex = new MutableInt();

        for (int initIndex = 0; initIndex < initSize; ++initIndex) {
            long initDate = DateTimes.microsToMillis(TimestampType.TIMESTAMP_MILLIS.getLong(initBlock, initIndex));
            long[] row = getRetentionLostForInitDate(returnBlock, (int) retenNum, dateTimeField, returnStartIndex, initDate);
            table[initIndex] = row;
        }

        RetentionLostData retentionLostData;
        if (state.getData() == null) {
            retentionLostData = new RetentionLostData(table, initSize);
            state.setData(retentionLostData);
        }
        else {
            retentionLostData = RetentionLostData.merge((RetentionLostData) state.getData(), new RetentionLostData(table, initSize));
            state.setData(retentionLostData);
        }
    }

    public static long[] getRetentionLostForInitDate(Block returnBlock, int retenNum, DateTimeField dateTimeField, MutableInt returnStartIndex, long initDate)
    {
        int n = retenNum + 2;
        int rowLength = 1 + n + n;
        int renInit = 1;
        int lostInit = 1 + n;
        long[] row = new long[rowLength];
        row[0] = initDate;
        row[renInit] = 1L;
        boolean lostFilled = false;
        if (returnBlock != null) {
            int positionCount = returnBlock.getPositionCount();

            for (int returnIndex = returnStartIndex.intValue(); returnIndex < positionCount; ++returnIndex) {
                long returnDate = DateTimes.microsToMillis(TimestampType.TIMESTAMP_MILLIS.getLong(returnBlock, returnIndex));
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
    public static void combine(@AggregationState RetentionLostDataState state, @AggregationState RetentionLostDataState otherState)
    {
        if (state.getData() == null) {
            state.setData((RetentionLostData) otherState.getData());
        }
        else if (otherState.getData() != null) {
            RetentionLostData merged = RetentionLostData.merge((RetentionLostData) state.getData(), (RetentionLostData) otherState.getData());
            state.setData(merged);
        }
    }

    @OutputFunction("varchar")
    public static void output(@AggregationState RetentionLostDataState state, BlockBuilder out)
    {
        RetentionLostData retentionLostData = (RetentionLostData) state.getData();
        if (retentionLostData == null) {
            out.appendNull();
        }
        else {
            Map<Long, Map<Long, long[]>> map = retentionLostData.toMap();
            String jsonString = JSON.toJSONString(map);
            VarcharType.createVarcharType(jsonString.length()).writeString(out, jsonString);
        }
    }
}
