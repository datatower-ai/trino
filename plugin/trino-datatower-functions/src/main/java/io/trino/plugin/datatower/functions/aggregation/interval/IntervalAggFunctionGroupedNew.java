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
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.Description;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.VarcharType;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @author liulin
 */
@AggregationFunction("build_user_interval_agg_grouped_new")
@Description("returns  event_time,interval,groups row")
public class IntervalAggFunctionGroupedNew
{
    public static final int HEADER_LEN = 16;
    public static final int EVENT_ITEM_LEN_NON_SELF = 9;
    public static final int EVENT_ITEM_LEN_SELF = 8;
    public static final int EVENT_TYPE_START = 0;
    public static final int EVENT_TYPE_END = 1;
    public static final int INTERVAL_UNIT_SEC = 0;
    public static final int INTERVAL_UNIT_MILLI_SEC = 1;

    private IntervalAggFunctionGroupedNew()
    {
    }

    @InputFunction
    public static void input(@AggregationState IntervalState state,
            @SqlType("bigint") long eventId,
            @SqlType("timestamp(6)") long eventTime,
            @SqlType("array(varchar)") Block initGroupArray,
            @SqlType("array(bigint)") Block paramArray)
    {
        DynamicSliceOutput sliceOutput = state.getData();
        long sessionFlag = BigintType.BIGINT.getLong(paramArray, 1);
        if (null == sliceOutput) {
            long numOfGroups = BigintType.BIGINT.getLong(paramArray, 0);
            long windowsGap = BigintType.BIGINT.getLong(paramArray, 2);
            long intervalUnit = BigintType.BIGINT.getLong(paramArray, 3);
            sliceOutput = new DynamicSliceOutput(HEADER_LEN + getEventItemFixedLen(sessionFlag));
            sliceOutput.writeLong(sessionFlag | (intervalUnit << 1) | (numOfGroups << 4));
            sliceOutput.writeLong(windowsGap);
            state.setData(sliceOutput);
        }
        sliceOutput.writeLong(DateTimes.microsToMillis(eventTime));
        if (!getIsSelfCaseBySessionFlag(sessionFlag)) {
            sliceOutput.writeByte((int) eventId);
        }
        if (eventId == EVENT_TYPE_START && initGroupArray != null && initGroupArray.getPositionCount() > 0) {
            for (int i = 0; i < initGroupArray.getPositionCount(); i++) {
                if (!initGroupArray.isNull(i)) {
                    Slice itemSlice = VarcharType.VARCHAR.getSlice(initGroupArray, i);
                    sliceOutput.writeInt(itemSlice.length());
                    if (itemSlice.length() > 0) {
                        sliceOutput.writeBytes(itemSlice);
                    }
                }
                else {
                    sliceOutput.writeInt(0);
                }
            }
        }
    }

    private static int getEventItemFixedLen(long sessionFlag)
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

    @OutputFunction("array(row(timestamp(6),bigint, array(varchar)))")
    public static void output(@AggregationState IntervalState state, BlockBuilder out)
    {
        DynamicSliceOutput sliceOutput = (DynamicSliceOutput) state.getData();
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
                sessionEntity.setInitGroupCount((int) getInitGroupCount(sessionFlag));
                sessionEntity.setWindowGapMs(sliceInput.readLong());
                List<EventEntity> eventEntities = new ArrayList<>();

                int groupLen;
                for (int index = HEADER_LEN; index < slice.length(); index += getEventItemFixedLen(sessionFlag) + groupLen) {
                    EventEntity eventEntity = new EventEntity();
                    eventEntity.setEventTime(sliceInput.readLong());
                    if (!sessionEntity.isSelfCase()) {
                        eventEntity.setEventId(sliceInput.readByte());
                    }
                    else {
                        eventEntity.setEventId(EVENT_TYPE_START);
                    }

                    groupLen = 0;
                    if (eventEntity.getEventId() == EVENT_TYPE_START && sessionEntity.getInitGroupCount() > 0) {
                        BlockBuilder builder = VarcharType.VARCHAR.createBlockBuilder((BlockBuilderStatus) null, 2);

                        for (int i = 0; i < sessionEntity.getInitGroupCount(); ++i) {
                            int sliceLen = sliceInput.readInt();
                            groupLen += 4;
                            groupLen += sliceLen;
                            VarcharType.VARCHAR.writeSlice(builder, sliceInput.readSlice(sliceLen));
                        }

                        eventEntity.setGroupBlock(builder.build());
                    }

                    eventEntities.add(eventEntity);
                }

                if (eventEntities.isEmpty()) {
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
        return (sessionFlag & EVENT_TYPE_END) == EVENT_TYPE_END;
    }

    private static long getIntervalUnit(long sessionFlag)
    {
        return (sessionFlag >> 1) & 3;
    }

    private static long getInitGroupCount(long sessionFlag)
    {
        return (sessionFlag >> 4) & 255;
    }

    private static void buildSelfCaseUserEventInterval(BlockBuilder out, SessionEntity sessionEntity)
    {
        ((ArrayBlockBuilder) out).buildEntry(elementBuilder -> {
            Block startGroupBlock = null;
            long startTime = -1L;
            ArrayType arrayType = new ArrayType(VarcharType.VARCHAR);
            for (EventEntity eventEntity : sessionEntity.eventEntities) {
                if (-1L == startTime) {
                    startTime = eventEntity.getEventTime();
                    startGroupBlock = eventEntity.getGroupBlock();
                }
                else if (startTime > 0L) {
                    long interval = eventEntity.getEventTime() - startTime;
                    extractDuplicateMethod(sessionEntity, elementBuilder, startGroupBlock, arrayType, startTime, interval, interval > 0L, interval <= sessionEntity.getWindowGapMs());
                    startTime = eventEntity.getEventTime();
                    startGroupBlock = eventEntity.getGroupBlock();
                }
            }
        });
    }

    private static long getOutputInterval(SessionEntity sessionEntity, long interval)
    {
        if (sessionEntity.getIntervalUnit() == INTERVAL_UNIT_SEC) {
            interval = DateTimes.millisToSecsCeil(interval);
        }

        return interval;
    }

    public static void buildNonSelfCaseUserEventInterval(BlockBuilder out, SessionEntity sessionEntity)
    {
        ((ArrayBlockBuilder) out).buildEntry(elementBuilder -> {
            long startTime = -1L;
            Block startGroupBlock = null;
            ArrayType arrayType = new ArrayType(VarcharType.VARCHAR);

            for (EventEntity eventEntity : sessionEntity.eventEntities) {
                if (eventEntity.getEventId() == EVENT_TYPE_START) {
                    startTime = eventEntity.getEventTime();
                    startGroupBlock = eventEntity.getGroupBlock();
                }
                else {
                    long interval = eventEntity.getEventTime() - startTime;
                    if (interval > 0L) {
                        extractDuplicateMethod(sessionEntity, elementBuilder, startGroupBlock, arrayType, startTime, interval, interval <= sessionEntity.getWindowGapMs(), startTime > 0L);
                        startTime = -1L;
                    }
                }
            }
        });

//        BlockBuilder outBuilder = out.beginBlockEntry();
//        long startTime = -1L;
//        Block startGroupBlock = null;
//        ArrayType arrayType = new ArrayType(VarcharType.VARCHAR);
//
//        for (EventEntity eventEntity : sessionEntity.eventEntities) {
//            if (eventEntity.getEventId() == EVENT_TYPE_START) {
//                startTime = eventEntity.getEventTime();
//                startGroupBlock = eventEntity.getGroupBlock();
//            } else {
//                long interval = eventEntity.getEventTime() - startTime;
//                if (interval > 0L) {
//                    extractDuplicateMethod(sessionEntity, out, startGroupBlock, arrayType, startTime, interval, interval <= sessionEntity.getWindowGapMs(), startTime > 0L);
//                    startTime = -1L;
//                }
//            }
//        }
//        out.closeEntry();
    }

    private static void extractDuplicateMethod(SessionEntity sessionEntity, BlockBuilder outBuilder, Block startGroupBlock, ArrayType arrayType, long startTime, long interval, boolean b, boolean b2)
    {
        if (b && b2) {
            //array(row(timestamp(6),bigint, array(varchar)))
            ((RowBlockBuilder) outBuilder).buildEntry(fieldBuilders -> {
                TimestampType.TIMESTAMP_MILLIS.writeLong(fieldBuilders.get(0), DateTimes.millisToMicros(startTime));
                BigintType.BIGINT.writeLong(fieldBuilders.get(1), getOutputInterval(sessionEntity, interval));
                if (startGroupBlock != null) {
                    arrayType.writeObject(fieldBuilders.get(2), startGroupBlock);
                }
                else {
                    fieldBuilders.get(2).build();
                }
            });

//            BlockBuilder rowOutputBuilder = outBuilder.beginBlockEntry();
//            TimestampType.TIMESTAMP_MILLIS.writeLong(outBuilder, DateTimes.millisToMicros(startTime));
//            BigintType.BIGINT.writeLong(outBuilder, getOutputInterval(sessionEntity, interval));
//            if (startGroupBlock != null) {
//                arrayType.writeObject(outBuilder, startGroupBlock);
//            } else {
//                outBuilder.appendNull();
//            }
//            outBuilder.closeEntry();
        }
    }

    public static class SessionEntity
    {
        boolean isSelfCase;
        long windowGapMs;
        int intervalUnit;
        int initGroupCount;
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

        public int getInitGroupCount()
        {
            return this.initGroupCount;
        }

        public void setInitGroupCount(int initGroupCount)
        {
            this.initGroupCount = initGroupCount;
        }
    }

    public static class EventEntity
    {
        private Integer eventId;
        private long eventTime;
        private Block groupBlock;

        public int getEventId()
        {
            return this.eventId;
        }

        public void setEventId(int eventId)
        {
            this.eventId = eventId;
        }

        public void setEventId(Integer eventId)
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

        public Block getGroupBlock()
        {
            return this.groupBlock;
        }

        public void setGroupBlock(Block groupBlock)
        {
            this.groupBlock = groupBlock;
        }
    }
}
