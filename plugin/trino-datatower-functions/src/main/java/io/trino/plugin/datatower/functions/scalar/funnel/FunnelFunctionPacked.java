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
package io.trino.plugin.datatower.functions.scalar.funnel;

import io.trino.plugin.datatower.functions.util.DateTimes;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BufferedMapValueBuilder;
import io.trino.spi.block.SqlMap;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TimestampType;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.joda.time.DateTimeField;
import org.joda.time.chrono.ISOChronology;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.toIntExact;

/**
 * @author liulin
 */
public class FunnelFunctionPacked
{
    static final int FUNNEL_INDEX_MASK = 4095;
    static final int MILLIS_SHIFT = 12;
    private static final long DAY_GAP_MILLIS = TimeUnit.DAYS.toMillis(1L);

    @Description("Pack funnel index and event time")
    @ScalarFunction("funnel_pack_time")
    @SqlType("bigint")
    public static long funnelPackTime(@SqlType("timestamp(6)") long timestamp,
            @SqlType("integer") long funnelIndex)
    {
        if (funnelIndex > FUNNEL_INDEX_MASK) {
            throw new IllegalArgumentException("funnelIndex overflow: " + funnelIndex);
        }
        else {
            long millis = DateTimes.microsToMillis(timestamp);
            long shiftedMillis = millis << MILLIS_SHIFT;
            if (shiftedMillis >> MILLIS_SHIFT != millis) {
                throw new IllegalArgumentException("Millis overflow: " + millis);
            }
            else {
                return shiftedMillis | funnelIndex & FUNNEL_INDEX_MASK;
            }
        }
    }

    @Description("returns the max step of user's event time list map")
    @ScalarFunction("funnel_max_step")
    @SqlType("bigint")
    public static long maxFunnelStep(@SqlType("array(bigint)") Block arrayBlock,
            @SqlType("bigint") long windowsGap,
            @SqlType("integer") long maxFunnelIndex)
    {
        if (arrayBlock.getPositionCount() <= 0) {
            return 0L;
        }
        else {
            long[][] eventMap = toMatrix(arrayBlock, toIntExact(maxFunnelIndex));
            int maxKey = -1;
            while (maxKey + 1 < maxFunnelIndex && eventMap[maxKey + 1] != null) {
                ++maxKey;
            }

            if (maxKey <= 0) {
                return maxKey + 1;
            }
            else {
                int[] positions = new int[maxKey + 1];
                Arrays.fill(positions, -1);
                return (new MaxStepFinder(eventMap, maxKey, positions, windowsGap)).find() + 1;
            }
        }
    }

    private static int adjustStartPositions(long[][] stepTimestamps, int[] positions, int maxKey)
    {
        long startTime = stepTimestamps[0][0];

        for (int step = 1; step <= maxKey; ++step) {
            long[] timestamps = stepTimestamps[step];

            int p = positions[step];
            while (p + 1 < timestamps.length && timestamps[p + 1] <= startTime) {
                ++p;
            }

            if (p == timestamps.length - 1) {
                return step - 1;
            }

            positions[step] = p;
        }

        return maxKey;
    }

    static long[][] toMatrix(@SqlType("array(bigint)") Block arrayBlock, int maxFunnelIndex)
    {
        if (maxFunnelIndex > FUNNEL_INDEX_MASK) {
            throw new IllegalArgumentException("maxFunnelIndex overflow: " + maxFunnelIndex);
        }
        else {
            int entryCount = arrayBlock.getPositionCount();
            int listSize = entryCount / maxFunnelIndex;
            Map<Integer, LongArrayList> map = new HashMap<>(maxFunnelIndex);

            for (int i = 0; i < entryCount; ++i) {
                long packedTimeMillis = BigintType.BIGINT.getLong(arrayBlock, i);
                long millis = packedTimeMillis >> MILLIS_SHIFT;
                int funnelIndex = (int) (packedTimeMillis & FUNNEL_INDEX_MASK);
                if (funnelIndex >= 1 && funnelIndex <= maxFunnelIndex) {
                    LongArrayList list = map.computeIfAbsent(funnelIndex, key -> new LongArrayList(listSize));
                    list.add(millis);
                }
            }

            long[][] eventMap = new long[maxFunnelIndex][];

            for (int i = 0; i < maxFunnelIndex; ++i) {
                int funnelIndex = i + 1;
                LongArrayList list = map.get(funnelIndex);
                if (list != null) {
                    long[] timeStampList = list.toLongArray();
                    Arrays.sort(timeStampList);
                    eventMap[i] = timeStampList;
                }
            }

            return eventMap;
        }
    }

    static SqlMap getBlockFromMap(MapType toMapType, Map<Long, Long> map)
    {
        BufferedMapValueBuilder buffered = BufferedMapValueBuilder.createBuffered(toMapType);
        return buffered.build(map.size(), (keyBuilder, valueBuilder) -> map.forEach((key, value) -> {
            TimestampType.TIMESTAMP_MILLIS.writeLong(keyBuilder, DateTimes.millisToMicros(key));
            BigintType.BIGINT.writeLong(valueBuilder, value);
        }));
    }

    @Description("returns the max step of user's event time list map")
    @ScalarFunction("funnel_max_step_date")
    @SqlType("map(timestamp(6),bigint)")
    public SqlMap maxFunnelStepForDate(@TypeParameter("map(timestamp(6),bigint)") MapType toMapType,
            @SqlType("array(bigint)") Block arrayBlock,
            @SqlType("bigint") long windowsGap,
            @SqlType("integer") long maxFunnelIndex,
            @SqlType("timestamp(6)") long startTimestamp0,
            @SqlType("timestamp(6)") long endTimestamp0)
    {
        Map<Long, Long> resultMap = new LinkedHashMap<>();
        long key = DateTimes.microsToMillis(startTimestamp0);

        for (long endTimestamp = DateTimes.microsToMillis(endTimestamp0); key <= endTimestamp; key += DAY_GAP_MILLIS) {
            resultMap.put(key, 0L);
        }

        if (arrayBlock.getPositionCount() > 0) {
            long[][] stepTimestamps = toMatrix(arrayBlock, toIntExact(maxFunnelIndex));
            int maxKey = -1;
            while ((long) (maxKey + 1) < maxFunnelIndex && stepTimestamps[maxKey + 1] != null) {
                ++maxKey;
            }

            if (maxKey >= 0) {
                int[] positions = new int[maxKey + 1];
                Arrays.fill(positions, -1);
                DateTimeField dateTimeField = ISOChronology.getInstanceUTC().dayOfMonth();
                Map<Long, LongArrayList> step0Map = new LinkedHashMap<>();
                long[] sTs0 = stepTimestamps[0];

                for (long timestamp : sTs0) {
                    long group = dateTimeField.roundFloor(timestamp);
                    LongArrayList list = step0Map.computeIfAbsent(group, k -> new LongArrayList());
                    list.add(timestamp);
                }

                for (Map.Entry<Long, LongArrayList> entry : step0Map.entrySet()) {
                    long maxStep;
                    if (maxKey <= 0) {
                        maxStep = 1L;
                    }
                    else {
                        stepTimestamps[0] = entry.getValue().toLongArray();
                        maxKey = adjustStartPositions(stepTimestamps, positions, maxKey);
                        int[] copy = Arrays.copyOf(positions, maxKey + 1);
                        maxStep = (new MaxStepFinder(stepTimestamps, maxKey, copy, windowsGap)).find() + 1;
                    }
                    resultMap.put(entry.getKey(), maxStep);
                }
            }
        }
        return getBlockFromMap(toMapType, resultMap);
    }

    public static class MaxStepFinder
    {
        private final long[][] stepTimestamps;
        private final int maxRow;
        private final int[] positions;
        private final long[] limitFromStep0;
        private final long limitMax;
        private boolean visitAnyStepMaxValidTimestamp;
        private int currentStep = -1;

        MaxStepFinder(long[][] stepTimestamps, int maxRow, int[] positions, long windowsGap)
        {
            this.stepTimestamps = stepTimestamps;
            this.maxRow = maxRow;
            this.positions = positions;
            long[] step0Timestamps = stepTimestamps[0];
            int length = step0Timestamps.length;
            long[] limit = new long[length];

            for (int i = 0; i < length; ++i) {
                long startTime = step0Timestamps[i];
                if (windowsGap > 0L) {
                    limit[i] = startTime + windowsGap;
                }
                else {
                    if (windowsGap != -1L) {
                        throw new TrinoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "window gap: " + windowsGap + " is not a valid number");
                    }
                    long startTimeTruncDay = ISOChronology.getInstanceUTC().dayOfMonth().roundFloor(startTime);
                    limit[i] = startTimeTruncDay + FunnelFunctionPacked.DAY_GAP_MILLIS;
                }
            }

            this.limitFromStep0 = limit;
            this.limitMax = limit[length - 1];
        }

        int find()
        {
            while (true) {
                if (this.advanceStep()) {
                    this.adjustPrevious(this.currentStep);
                }
                else if (this.currentStep == this.maxRow || !this.moveCurrentStepPositionAndAdjust()) {
                    return this.currentStep;
                }
            }
        }

        boolean moveCurrentStepPositionAndAdjust()
        {
            while (!this.visitAnyStepMaxValidTimestamp) {
                long[] timestamps = this.stepTimestamps[this.currentStep];
                int p = this.positions[this.currentStep];
                checkState(p < timestamps.length - 1, "already visited the max timestamp of step " + this.currentStep);
                this.positions[this.currentStep] = p + 1;
                long currentTimestamp = this.stepTimestamps[this.currentStep][this.positions[this.currentStep]];
                if (p + 1 == timestamps.length - 1 || currentTimestamp >= this.limitMax) {
                    this.visitAnyStepMaxValidTimestamp = true;
                    if (currentTimestamp > this.limitMax) {
                        return false;
                    }
                }
                adjustPrevious(this.currentStep);
                long limit0 = this.limitFromStep0[this.positions[0]];
                if (currentTimestamp < limit0) {
                    return true;
                }
            }
            return false;
        }

        boolean advanceStep()
        {
            int step = this.currentStep + 1;
            if (step > this.maxRow) {
                return false;
            }
            else {
                long[] timestamps = this.stepTimestamps[step];
                int p = this.positions[step];
                checkState(p < timestamps.length - 1, "already visited the max timestamp of step " + step);
                int np;
                if (step > 0) {
                    long currentTimestamp = this.stepTimestamps[this.currentStep][this.positions[this.currentStep]];

                    int maxLe = p;
                    while (maxLe + 1 < timestamps.length && timestamps[maxLe + 1] <= currentTimestamp) {
                        ++maxLe;
                    }

                    this.positions[step] = maxLe;
                    if (maxLe == timestamps.length - 1) {
                        this.visitAnyStepMaxValidTimestamp = true;
                        np = maxLe;
                    }
                    else {
                        long limit0 = this.limitFromStep0[this.positions[0]];
                        long timestamp = timestamps[maxLe + 1];
                        if (timestamp < limit0) {
                            np = maxLe + 1;
                        }
                        else {
                            np = maxLe;
                            if (timestamp >= this.limitMax) {
                                this.visitAnyStepMaxValidTimestamp = true;
                            }
                        }
                    }
                }
                else {
                    np = p + 1;
                }

                if (np > this.positions[step]) {
                    this.currentStep = step;
                    this.positions[step] = np;
                    if (np == timestamps.length - 1) {
                        this.visitAnyStepMaxValidTimestamp = true;
                    }

                    return true;
                }
                else {
                    return false;
                }
            }
        }

        void adjustPrevious(int step)
        {
            while (true) {
                if (step > 0) {
                    long time = this.stepTimestamps[step][this.positions[step]];
                    int pStep = step - 1;
                    long[] pStepTimestamps = this.stepTimestamps[pStep];

                    int pStepIndex = this.positions[pStep];
                    while (pStepIndex + 1 < pStepTimestamps.length && pStepTimestamps[pStepIndex + 1] < time) {
                        ++pStepIndex;
                    }

                    if (pStepIndex > this.positions[pStep]) {
                        this.positions[pStep] = pStepIndex;
                        if (pStepIndex == pStepTimestamps.length - 1) {
                            this.visitAnyStepMaxValidTimestamp = true;
                        }

                        step = pStep;
                        continue;
                    }
                }

                return;
            }
        }
    }
}
