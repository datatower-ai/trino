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
package io.trino.plugin.datatower.functions.aggregation.interval;

import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.trino.plugin.datatower.functions.state.inverval.IntervalState;
import io.trino.plugin.datatower.functions.util.DateTimes;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.Description;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.TimestampType;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @author liulin
 */
@AggregationFunction("build_user_interval_agg")
@Description("returns interval event_time/interval map")
public class IntervalAggFunction
{
    public static final int HEADER_LEN = 16;
    public static final int EVENT_ITEM_LEN_NON_SELF = 9;
    public static final int EVENT_ITEM_LEN_SELF = 8;
    public static final int EVENT_TYPE_START = 0;
    public static final int EVENT_TYPE_END = 1;
    public static final int INTERVAL_UNIT_SEC = 0;
    public static final int INTERVAL_UNIT_MILLI_SEC = 1;

    private IntervalAggFunction()
    {
    }

    @InputFunction
    public static void input(@AggregationState IntervalState state,
            @SqlType("bigint") long eventId,
            @SqlType("timestamp(6)") long eventTime,
            @SqlType("bigint") long sessionFlag,
            @SqlType("bigint") long windowsGap,
            @SqlType("bigint") long intervalUnit)
    {
        DynamicSliceOutput sliceOutput = state.getData();
        if (null == sliceOutput) {
            sliceOutput = new DynamicSliceOutput(HEADER_LEN + getEventItemLen(sessionFlag));
            sliceOutput.writeLong(sessionFlag | (intervalUnit << 1));
            sliceOutput.writeLong(windowsGap);
            state.setData(sliceOutput);
        }
        sliceOutput.writeLong(DateTimes.microsToMillis(eventTime));
        if (!getIsSelfCaseBySessionFlag(sessionFlag)) {
            sliceOutput.writeByte((int) eventId);
        }
    }

    private static int getEventItemLen(long sessionFlag)
    {
        if (getIsSelfCaseBySessionFlag(sessionFlag)) {
            return EVENT_ITEM_LEN_SELF;
        }
        return EVENT_ITEM_LEN_NON_SELF;
    }

    @CombineFunction
    public static void combine(@AggregationState IntervalState state, @AggregationState IntervalState otherState)
    {
        DynamicSliceOutput sliceOutput = state.getData();
        DynamicSliceOutput otherSliceOutput = otherState.getData();
        if (null == sliceOutput) {
            state.setData(otherSliceOutput);
            return;
        }
        Slice otherSlice = otherSliceOutput.slice();
        int writeLen = otherSlice.length();
        sliceOutput.writeBytes(otherSlice, HEADER_LEN, writeLen - HEADER_LEN);
    }

    @OutputFunction("map(timestamp(6),bigint)")
    public static void output(@AggregationState IntervalState state, BlockBuilder out)
    {
        DynamicSliceOutput sliceOutput = state.getData();
        if (null == sliceOutput) {
            out.appendNull();
        }
        else {
            Slice slice = sliceOutput.slice();
            try (BasicSliceInput sliceInput = new BasicSliceInput(sliceOutput.slice())) {
                SessionEntity sessionEntity = new SessionEntity();
                long sessionFlag = sliceInput.readLong();
                sessionEntity.setSelfCase(getIsSelfCaseBySessionFlag(sessionFlag));
                sessionEntity.setIntervalUnit((int) getIntervalUnit(sessionFlag));
                sessionEntity.setWindowGapMs(sliceInput.readLong());
                List<EventEntity> eventEntities = new ArrayList<>();

                for (int index = HEADER_LEN; index < slice.length(); index += getEventItemLen(sessionFlag)) {
                    EventEntity eventEntity = new EventEntity();
                    eventEntity.setEventTime(sliceInput.readLong());
                    if (!sessionEntity.isSelfCase()) {
                        eventEntity.setEventId(sliceInput.readByte());
                    }
                    else {
                        eventEntity.setEventId(EVENT_TYPE_START);
                    }

                    eventEntities.add(eventEntity);
                }

                if (eventEntities.size() < 1) {
                    out.appendNull();
                }
                else {
                    eventEntities.sort(Comparator.comparingLong(EventEntity::getEventTime).thenComparing(EventEntity::getEventId, Comparator.reverseOrder()));
                    sessionEntity.setEventEntities(eventEntities);
                    if (sessionEntity.isSelfCase()) {
                        buildSelfCaseUserEventInterval(out, sessionEntity);
                    }
                    else {
                        buildNonSelfCaseUserEventInterval(out, sessionEntity);
                    }
                }
            }
        }
    }

    private static boolean getIsSelfCaseBySessionFlag(long sessionFlag)
    {
        return (sessionFlag & 1) == 1;
    }

    private static long getIntervalUnit(long sessionFlag)
    {
        return (sessionFlag >> 1) & 3;
    }

    private static void buildSelfCaseUserEventInterval(BlockBuilder out, SessionEntity sessionEntity)
    {
        ((MapBlockBuilder) out).buildEntry(((keyBuilder, valueBuilder) -> {
            long startTime = -1L;
            for (EventEntity eventEntity : sessionEntity.eventEntities) {
                if (-1L == startTime) {
                    startTime = eventEntity.getEventTime();
                }
                else if (startTime > 0L) {
                    long interval = eventEntity.getEventTime() - startTime;
                    if (interval > 0L && interval <= sessionEntity.getWindowGapMs()) {
                        TimestampType.TIMESTAMP_MILLIS.writeLong(keyBuilder, DateTimes.millisToMicros(startTime));
                        BigintType.BIGINT.writeLong(valueBuilder, getOutputInterval(sessionEntity, interval));
                    }
                    startTime = eventEntity.getEventTime();
                }
            }
        }));

//        BlockBuilder blockBuilder = out.beginBlockEntry();
//        long startTime = -1L;
//        for (EventEntity eventEntity : sessionEntity.eventEntities) {
//            if (-1L == startTime) {
//                startTime = eventEntity.getEventTime();
//            } else if (startTime > 0L) {
//                long interval = eventEntity.getEventTime() - startTime;
//                if (interval > 0L && interval <= sessionEntity.getWindowGapMs()) {
//                    TimestampType.TIMESTAMP_MILLIS.writeLong(out, DateTimes.millisToMicros(startTime));
//                    BigintType.BIGINT.writeLong(out, getOutputInterval(sessionEntity, interval));
//                }
//                startTime = eventEntity.getEventTime();
//            }
//        }
//        out.closeEntry();
    }

    public static void buildNonSelfCaseUserEventInterval(BlockBuilder out, SessionEntity sessionEntity)
    {
        ((MapBlockBuilder) out).buildEntry(((keyBuilder, valueBuilder) -> {
            long startTime = -1L;
            for (EventEntity eventEntity : sessionEntity.eventEntities) {
                if (eventEntity.getEventId() == EVENT_TYPE_START) {
                    startTime = eventEntity.getEventTime();
                }
                else {
                    long interval = eventEntity.getEventTime() - startTime;
                    if (interval > 0L) {
                        if (interval <= sessionEntity.getWindowGapMs() && startTime > 0L) {
                            TimestampType.TIMESTAMP_MILLIS.writeLong(keyBuilder, DateTimes.millisToMicros(startTime));
                            BigintType.BIGINT.writeLong(valueBuilder, getOutputInterval(sessionEntity, interval));
                        }
                        startTime = -1L;
                    }
                }
            }
        }));

//        BlockBuilder blockBuilder = out.beginBlockEntry();
//        long startTime = -1L;
//        for (EventEntity eventEntity : sessionEntity.eventEntities) {
//            if (eventEntity.getEventId() == EVENT_TYPE_START) {
//                startTime = eventEntity.getEventTime();
//            } else {
//                long interval = eventEntity.getEventTime() - startTime;
//                if (interval > 0L) {
//                    if (interval <= sessionEntity.getWindowGapMs() && startTime > 0L) {
//                        TimestampType.TIMESTAMP_MILLIS.writeLong(out, DateTimes.millisToMicros(startTime));
//                        BigintType.BIGINT.writeLong(out, getOutputInterval(sessionEntity, interval));
//                    }
//                    startTime = -1L;
//                }
//            }
//        }
//        out.closeEntry();
    }

    private static long getOutputInterval(SessionEntity sessionEntity, long interval)
    {
        if (sessionEntity.getIntervalUnit() == INTERVAL_UNIT_SEC) {
            interval = DateTimes.millisToSecsCeil(interval);
        }
        return interval;
    }

    public static class SessionEntity
    {
        boolean isSelfCase;
        long windowGapMs;
        int intervalUnit;
        List<EventEntity> eventEntities;

        public List<EventEntity> getEventEntities()
        {
            return this.eventEntities;
        }

        public void setEventEntities(List<EventEntity> eventEntities)
        {
            this.eventEntities = eventEntities;
        }

        public boolean isSelfCase()
        {
            return this.isSelfCase;
        }

        public void setSelfCase(boolean selfCase)
        {
            this.isSelfCase = selfCase;
        }

        public long getWindowGapMs()
        {
            return this.windowGapMs;
        }

        public void setWindowGapMs(long windowGapMs)
        {
            this.windowGapMs = windowGapMs;
        }

        public int getIntervalUnit()
        {
            return this.intervalUnit;
        }

        public void setIntervalUnit(int intervalUnit)
        {
            this.intervalUnit = intervalUnit;
        }
    }

    public static class EventEntity
    {
        private Integer eventId;
        private long eventTime;

        public int getEventId()
        {
            return this.eventId;
        }

        public void setEventId(int eventId)
        {
            this.eventId = eventId;
        }

        public long getEventTime()
        {
            return this.eventTime;
        }

        public void setEventTime(long eventTime)
        {
            this.eventTime = eventTime;
        }
    }
}
