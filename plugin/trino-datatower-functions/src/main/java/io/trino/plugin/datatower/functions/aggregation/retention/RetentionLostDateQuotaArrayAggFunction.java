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
import io.trino.plugin.datatower.functions.util.DateTimes;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author liulin
 */
@AggregationFunction("retention_lost_date_quota_array_agg")
public final class RetentionLostDateQuotaArrayAggFunction
{
    private RetentionLostDateQuotaArrayAggFunction()
    {
    }

    @InputFunction
    @LiteralParameters("x")
    //SELECT retention_lost_date_quota_array_agg(init_date_array, COALESCE(return_date_array, X''), ARRAY['RETENTION_RATE'], ARRAY[5], 5, 'day') retention_lost_data
    public static void input(@AggregationState RetentionLostQuotaDataState state,
            @SqlType("varbinary") Slice initSlice,
            @SqlType("varbinary") Slice returnSlice,
            @SqlType("array(varchar)") Block quotaTypeBlock,
            @SqlType("array(bigint)") Block quotaDayBlock,
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
            long[] row = RetentionLostDateCollectAggFunction.getRetentionLostForInitDate(returnMillis, (int) retenNum, dateTimeField, returnStartIndex, initDate);
            table[initIndex] = row;
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

        RetentionLostQuotaData retentionLostQuotaData;
        if (state.getData() == null) {
            retentionLostQuotaData = new RetentionLostQuotaData(table, initSize, quotaTypeArr, quotaIndexArr);
            state.setData(retentionLostQuotaData);
        }
        else {
            retentionLostQuotaData = RetentionLostQuotaData.merge(state.getData(), new RetentionLostQuotaData(table, initSize, quotaTypeArr, quotaIndexArr));
            state.setData(retentionLostQuotaData);
        }
    }

    @CombineFunction
    public static void combine(@AggregationState RetentionLostQuotaDataState state,
            @AggregationState RetentionLostQuotaDataState otherState)
    {
        if (state.getData() == null) {
            state.setData(otherState.getData());
        }
        else if (otherState.getData() != null) {
            RetentionLostQuotaData merged = RetentionLostQuotaData.merge(state.getData(), otherState.getData());
            state.setData(merged);
        }
    }

    @OutputFunction("map(timestamp(6),array(double))")
    public static void output(@AggregationState RetentionLostQuotaDataState state, BlockBuilder out)
    {
        RetentionLostQuotaData retentionLostQuotaData = state.getData();
        if (retentionLostQuotaData == null) {
            out.appendNull();
            return;
        }
        else {
            Map<Long, Map<Long, long[]>> map = retentionLostQuotaData.toMap();
            long[] quotaTypeArr = retentionLostQuotaData.getQuotaType();
            long[] quotaIndexArr = retentionLostQuotaData.getQuotaIndex();

            ((MapBlockBuilder) out).buildEntry((keyBuilder, valueBuilder) -> {
                Set<Long> dateSet = map.get(0L).keySet();
                for (Long dateTs : dateSet) {
                    TimestampType.TIMESTAMP_MILLIS.writeLong(keyBuilder, DateTimes.millisToMicros(dateTs));
                    ((ArrayBlockBuilder) valueBuilder).buildEntry(elementBuilder -> {
                        for (int i = 0; i < quotaTypeArr.length; i++) {
                            long quotaTypeCode = quotaTypeArr[i];
                            QuotaType quotaType = QuotaType.getByCode((int) quotaTypeCode);
                            int quotaIndex = (int) quotaIndexArr[i] + 1;
                            double value = getValue(quotaType, quotaIndex, dateTs, map);
                            DoubleType.DOUBLE.writeDouble(elementBuilder, value);
                        }
                    });
                }
            });

//            BlockBuilder blockBuilder = out.beginBlockEntry();
//            Set<Long> dateSet = map.get(0L).keySet();
//            for (Long dateTs : dateSet) {
//                TimestampType.TIMESTAMP_MILLIS.writeLong(out, DateTimes.millisToMicros(dateTs));
//                ArrayType valueArrayType = new ArrayType(DoubleType.DOUBLE);
//                int quotaTypeArrCount = quotaTypeArr.length;
//                BlockBuilder valueArrayBuilder = DoubleType.DOUBLE.createBlockBuilder(null, quotaTypeArrCount - 1, 8);
//                for (int i = 0; i < quotaTypeArr.length; i++) {
//                    long quotaTypeCode = quotaTypeArr[i];
//                    QuotaType quotaType = QuotaType.getByCode((int) quotaTypeCode);
//                    int quotaIndex = (int) quotaIndexArr[i] + 1;
//                    double value = getValue(quotaType, quotaIndex, dateTs, map);
//                    DoubleType.DOUBLE.writeDouble(valueArrayBuilder, value);
//                }
//                valueArrayType.writeObject(out, valueArrayBuilder.build());
//            }
        }
//        out.closeEntry();
    }

    private static double getValue(QuotaType quotaType, int quotaIndex, Long dateTs, Map<Long, Map<Long, long[]>> map)
    {
        switch (quotaType) {
            case RETENTION_NUM -> {
                Map<Long, long[]> retentionMap = map.get(0L);
                long[] results = retentionMap.get(dateTs);
                return results[quotaIndex];
            }
            case RETENTION_RATE -> {
                Map<Long, long[]> retentionMap = map.get(0L);
                long[] results = retentionMap.get(dateTs);
                long initNum = results[0];
                if (0 == initNum) {
                    return 0D;
                }
                else {
                    long result = results[quotaIndex];
                    return new BigDecimal((double) result / initNum).setScale(8, RoundingMode.HALF_UP).doubleValue();
                }
            }
            case LOST_NUM -> {
                Map<Long, long[]> lostMap = map.get(1L);
                long[] results = lostMap.get(dateTs);
                return results[quotaIndex];
            }
            case LOST_RATE -> {
                Map<Long, long[]> lostMap = map.get(1L);
                long[] results = lostMap.get(dateTs);
                long initNum = results[0];
                if (0 == initNum) {
                    return 0D;
                }
                else {
                    long result = results[quotaIndex];
                    return new BigDecimal((double) result / initNum).setScale(8, RoundingMode.HALF_UP).doubleValue();
                }
            }
            default ->
                    throw new TrinoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "'" + quotaType.getName() + "' is not a valid Quota Type Name for function retention_lost_date_quota_array_agg");
        }
    }
}
