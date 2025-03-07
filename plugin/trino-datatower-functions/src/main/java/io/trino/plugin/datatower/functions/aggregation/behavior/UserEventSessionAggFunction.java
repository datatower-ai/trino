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
package io.trino.plugin.datatower.functions.aggregation.behavior;

import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.trino.plugin.datatower.functions.state.behavior.DynamicSliceOutputState;
import io.trino.plugin.datatower.functions.util.DateTimes;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.VarcharType;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * @author liulin
 */
@AggregationFunction("build_user_event_session_agg")
public class UserEventSessionAggFunction
{
    public static int headerLen = 16;
    public static int eventItemFixLen = 16;

    private UserEventSessionAggFunction() {}

    @InputFunction
    public static void input(
            @AggregationState DynamicSliceOutputState state,
            @SqlType("bigint") long eventId,
            @SqlType("timestamp(6)") long eventTime,
            @SqlType("varchar") Slice eventAttr,
            @SqlType("bigint") long sessionFlag,
            @SqlType("bigint") long eventFlag,
            @SqlType("bigint") long sessionGapMsp)
    {
        DynamicSliceOutput sliceOutput = state.getData();
        if (null == sliceOutput) {
            sliceOutput = new DynamicSliceOutput(headerLen + eventItemFixLen);
            sliceOutput.writeLong(sessionFlag);
            sliceOutput.writeLong(sessionGapMsp);
            state.setData(sliceOutput);
        }
        sliceOutput.writeLong(DateTimes.microsToMillis(eventTime));
        sliceOutput.writeShort((short) ((int) eventId));
        sliceOutput.writeShort((short) ((int) eventFlag));
        sliceOutput.writeInt(eventAttr.length());
        if (eventAttr.length() > 0) {
            sliceOutput.writeBytes(eventAttr);
        }
    }

    @CombineFunction
    public static void combine(
            @AggregationState DynamicSliceOutputState state,
            @AggregationState DynamicSliceOutputState otherState)
    {
        DynamicSliceOutput sliceOutput = state.getData();
        DynamicSliceOutput otherSliceOutput = otherState.getData();
        if (null == sliceOutput) {
            state.setData(otherSliceOutput);
        }
        else {
            Slice otherSlice = otherSliceOutput.slice();
            int writeLen = otherSlice.length();
            sliceOutput.writeBytes(otherSlice, headerLen, writeLen - headerLen);
        }
    }

    @OutputFunction("array(array(array(varchar)))")
    public static void output(@AggregationState DynamicSliceOutputState state, BlockBuilder out)
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
                boolean isTerminal = (sessionFlag & 65536L) == 65536L;
                sessionEntity.setSessionMaxNum((int) (sessionFlag & 65535L));
                sessionEntity.setSessionGapMsp(sliceInput.readLong());
                List<EventEntity> eventEntities = new ArrayList<>();

                int attrLen;
                for (int index = headerLen;
                        index < slice.length();
                        index += eventItemFixLen + attrLen) {
                    EventEntity eventEntity = new EventEntity();
                    eventEntity.setEventTime(sliceInput.readLong());
                    eventEntity.setEventId(sliceInput.readShort());
                    short eventFlag = sliceInput.readShort();
                    eventEntity.setInitEvent((eventFlag & 1) == 1);
                    attrLen = sliceInput.readInt();
                    if (attrLen > 0) {
                        eventEntity.setEventAttr(sliceInput.readSlice(attrLen).toStringUtf8());
                    }

                    eventEntities.add(eventEntity);
                }

                if (eventEntities.isEmpty()) {
                    out.appendNull();
                }
                else {
                    if (isTerminal) {
                        eventEntities.sort(Comparator.comparingLong(EventEntity::getEventTime).reversed());
                    }
                    else {
                        eventEntities.sort(Comparator.comparingLong(EventEntity::getEventTime));
                    }

                    sessionEntity.setEventEntities(eventEntities);
                    buildUserEventSession(out, sessionEntity);
                }
            }
        }
    }

    private static void buildUserEventSession(BlockBuilder out, SessionEntity sessionEntity)
    {
        long lastEventTime = 0L;
        boolean bFoundInit = false;
        List<Block> sessionList = new ArrayList<>();
        List<Block> sessionItemList = new ArrayList<>();
        ArrayType arrayType = new ArrayType(VarcharType.VARCHAR);
        Iterator<EventEntity> iterator = sessionEntity.eventEntities.iterator();

        while (true) {
            EventEntity eventEntity;
            do {
                if (!iterator.hasNext()) {
                    if (!sessionItemList.isEmpty()) {
                        sessionList.add(buildSession(arrayType, sessionItemList));
                    }

                    buildSessionList(out, sessionList);
                    return;
                }

                eventEntity = iterator.next();
                bFoundInit = eventEntity.isInitEvent();
            }
            while (!bFoundInit);

            // 将赋值操作从条件表达式中提取出来
            long newEventTime = buildSessionEvent(
                    eventEntity,
                    sessionItemList,
                    lastEventTime,
                    sessionEntity.getSessionMaxNum(),
                    sessionEntity.getSessionGapMsp());

            if (newEventTime < 0L) {
                if (!sessionItemList.isEmpty()) {
                    sessionList.add(buildSession(arrayType, sessionItemList));
                }

                sessionItemList.clear();
                bFoundInit = eventEntity.isInitEvent();
                if (bFoundInit) {
                    lastEventTime = buildSessionEvent(
                            eventEntity,
                            sessionItemList,
                            0L,
                            sessionEntity.getSessionMaxNum(),
                            sessionEntity.getSessionGapMsp());
                }
                else {
                    lastEventTime = 0L;
                }
            }
            else {
                lastEventTime = newEventTime;
            }
        }
    }

    private static long buildSessionEvent(
            EventEntity eventEntity,
            List<Block> sessionItemList,
            long lastEventTime,
            long sessionMaxNum,
            long sessionGapMs)
    {
        long eventTime = eventEntity.getEventTime();
        if (lastEventTime != 0L && Math.abs(eventTime - lastEventTime) > sessionGapMs) {
            return -1L;
        }
        else {
            if ((long) sessionItemList.size() < sessionMaxNum) {
                sessionItemList.add(
                        buildEventItemArray(eventEntity.getEventId(), eventEntity.getEventAttr()));
            }

            return eventTime;
        }
    }

    private static Block buildEventItemArray(long eventId, String attrName)
    {
        BlockBuilder builder = VarcharType.VARCHAR.createBlockBuilder(null, 2);
        VarcharType.VARCHAR.writeString(builder, "" + eventId);
        if (attrName != null && !attrName.isEmpty()) {
            VarcharType.VARCHAR.writeString(builder, attrName);
        }
        return builder.build();
    }

    private static Block buildSession(ArrayType arrayType, List<Block> eventItems)
    {
        BlockBuilder builder = arrayType.createBlockBuilder(null, 10);
        for (Block eventItem : eventItems) {
            arrayType.writeObject(builder, eventItem);
        }
        return builder.build();
    }

    private static void buildSessionList(BlockBuilder out, List<Block> sessionBlocks)
    {
        ArrayType arrayType = new ArrayType(new ArrayType(VarcharType.VARCHAR));
        ((ArrayBlockBuilder) out)
                .buildEntry(
                        elementBuilder -> {
                            if (Objects.isNull(sessionBlocks) || sessionBlocks.isEmpty()) {
                                elementBuilder.build();
                            }
                            else {
                                for (Block sessionBlock : sessionBlocks) {
                                    arrayType.writeObject(elementBuilder, sessionBlock);
                                }
                            }
                        });
    }

    private static class EventEntity
    {
        private int eventId;
        private long eventTime;
        private String eventAttr;
        private boolean isInitEvent;

        private EventEntity() {}

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

        public String getEventAttr()
        {
            return this.eventAttr;
        }

        public void setEventAttr(String eventAttr)
        {
            this.eventAttr = eventAttr;
        }

        public boolean isInitEvent()
        {
            return this.isInitEvent;
        }

        public void setInitEvent(boolean initEvent)
        {
            this.isInitEvent = initEvent;
        }
    }

    private static class SessionEntity
    {
        List<EventEntity> eventEntities;
        private int sessionMaxNum;
        private long sessionGapMsp;

        private SessionEntity() {}

        public int getSessionMaxNum()
        {
            return this.sessionMaxNum;
        }

        public void setSessionMaxNum(int sessionMaxNum)
        {
            this.sessionMaxNum = sessionMaxNum;
        }

        public long getSessionGapMsp()
        {
            return this.sessionGapMsp;
        }

        public void setSessionGapMsp(long sessionGapMsp)
        {
            this.sessionGapMsp = sessionGapMsp;
        }

        public List<EventEntity> getEventEntities()
        {
            return this.eventEntities;
        }

        public void setEventEntities(List<EventEntity> eventEntities)
        {
            this.eventEntities = eventEntities;
        }
    }
}
