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
package io.trino.plugin.datatower.functions.scalar.retention;

import io.airlift.slice.Slice;
import io.trino.plugin.datatower.functions.aggregation.retention.RetentionLostDateAggFunction;
import io.trino.plugin.datatower.functions.aggregation.retention.RetentionLostDateCollectAggFunction;
import io.trino.plugin.datatower.functions.state.collect.DateSet;
import io.trino.plugin.datatower.functions.util.ChronologyUtil;
import io.trino.plugin.datatower.functions.util.DateTimes;
import io.trino.spi.block.Block;
import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.TimestampType;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.joda.time.DateTimeField;
import org.joda.time.chrono.ISOChronology;

public class RetentionSeqFunction
{
    private RetentionSeqFunction()
    {
    }

    @Description("returns the retention user matching sequence condition")
    @ScalarFunction("is_retention_seq_user")
    @LiteralParameters("x")
    @SqlType("boolean")
    public static boolean isRetentionSequenceUser(@SqlType("array(timestamp(6))") Block initBlock,
            @SqlNullable @SqlType("array(timestamp(6))") Block returnBlock,
            @SqlType("bigint") long retenNum,
            @SqlType("varchar(x)") Slice unit,
            @SqlType("bigint") long isLost)
    {
        DateTimeField dateTimeField = ChronologyUtil.getRetentionTimestampField(ISOChronology.getInstanceUTC(), unit);
        long[] retentionVals = new long[(int) (retenNum + 2L)];
        retentionVals[0] = 1L;
        long[] lostVals = new long[(int) (retenNum + 2L)];

        int initSize;
        for (initSize = 0; (long) initSize < retenNum + 2L; ++initSize) {
            lostVals[initSize] = 1L;
        }

        initSize = initBlock.getPositionCount();
        int returnStartIndex = 0;

        for (int initIndex = 0; initIndex < initSize; ++initIndex) {
            if (returnBlock != null) {
                long initDate = DateTimes.microsToMillis(TimestampType.TIMESTAMP_MILLIS.getLong(initBlock, initIndex));
                int returnSize = returnBlock.getPositionCount();

                for (int returnIndex = returnStartIndex; returnIndex < returnSize; ++returnIndex) {
                    long returnDate = DateTimes.microsToMillis(TimestampType.TIMESTAMP_MILLIS.getLong(returnBlock, returnIndex));
                    if (returnDate < initDate) {
                        ++returnStartIndex;
                    }
                    else {
                        int gap = dateTimeField.getDifference(returnDate, initDate);
                        if (gap >= 0 && (long) gap <= retenNum) {
                            retentionVals[gap + 1] = 1L;
                        }

                        if (gap > 0) {
                            for (int i = gap; (long) i <= retenNum; ++i) {
                                lostVals[i + 1] = 0L;
                            }
                        }
                    }
                }
            }
        }

        if (isLost == 0L && retentionVals[(int) retenNum] == 1L) {
            return true;
        }
        else {
            return isLost == 1L && lostVals[(int) retenNum] == 1L;
        }
    }

    @Description("returns the retention user matching sequence condition")
    @ScalarFunction("is_retention_seq_user")
    @LiteralParameters("x")
    @SqlType("boolean")
    public static boolean isRetentionSequenceUser(@SqlType("array(timestamp(6))") Block initBlock,
            @SqlNullable @SqlType("array(timestamp(6))") Block returnBlock,
            @SqlType("timestamp(6)") long initDateMicros,
            @SqlType("integer") long valIndex,
            @SqlType("varchar(x)") Slice unit,
            @SqlType("integer") long retentionType)
    {
        if (!contains(initBlock, initDateMicros)) {
            return false;
        }
        else {
            long initDate = DateTimes.microsToMillis(initDateMicros);
            DateTimeField dateTimeField = ChronologyUtil.getRetentionTimestampField(ISOChronology.getInstanceUTC(), unit);
            int i = (int) valIndex;
            int n = i + 1;
            int retenNum = n - 2;
            long[] row = RetentionLostDateAggFunction.getRetentionLostForInitDate(returnBlock, retenNum, dateTimeField, new MutableInt(), initDate);
            if (retentionType == 0L) {
                int renInit = 1;
                return row[renInit + i] == 1L;
            }
            else if (retentionType == 1L) {
                int lostInit = 1 + n;
                return row[lostInit + i] == 1L;
            }
            else {
                throw new IllegalArgumentException("invalid retention type: " + retentionType);
            }
        }
    }

    @Description("returns the retention user matching sequence condition")
    @ScalarFunction("is_retention_user_in_date_collect")
    @LiteralParameters("x")
    @SqlType("boolean")
    public static boolean isRetentionUserInDateCollect(@SqlType("varbinary") Slice initSlice,
            @SqlNullable @SqlType("varbinary") Slice returnSlice,
            @SqlType("timestamp(6)") long initDateMicros,
            @SqlType("integer") long valIndex,
            @SqlType("varchar(x)") Slice unit,
            @SqlType("integer") long retentionType)
    {
        long initDate = DateTimes.microsToMillis(initDateMicros);
        long[] initMillis = DateSet.deserialize(initSlice).toEpochMillisArray();
        if (!ArrayUtils.contains(initMillis, initDate)) {
            return false;
        }
        else {
            long[] returnMillis = returnSlice != null ? DateSet.deserialize(returnSlice).toEpochMillisArray() : null;
            DateTimeField dateTimeField = ChronologyUtil.getRetentionTimestampField(ISOChronology.getInstanceUTC(), unit);
            int i = (int) valIndex;
            int n = i + 1;
            int retenNum = n - 2;
            long[] row = RetentionLostDateCollectAggFunction.getRetentionLostForInitDate(returnMillis, retenNum, dateTimeField, new MutableInt(), initDate);
            if (retentionType == 0L) {
                int renInit = 1;
                return row[renInit + i] == 1L;
            }
            else if (retentionType == 1L) {
                int lostInit = 1 + n;
                return row[lostInit + i] == 1L;
            }
            else {
                throw new IllegalArgumentException("invalid retention type: " + retentionType);
            }
        }
    }

    private static boolean contains(Block initBlock, long initDateMicros)
    {
        int positionCount = initBlock.getPositionCount();

        for (int position = 0; position < positionCount; ++position) {
            long t = TimestampType.TIMESTAMP_MILLIS.getLong(initBlock, position);
            if (t == initDateMicros) {
                return true;
            }
        }

        return false;
    }
}
