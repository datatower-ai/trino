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
package io.trino.plugin.datatower.functions.scalar.interval;

import io.trino.plugin.datatower.functions.aggregation.interval.IntervalAggFunction;
import io.trino.plugin.datatower.functions.util.DateTimes;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.SqlMap;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.MapType;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @author liulin
 */
public class IntervalFunctionGrouped
{
    private IntervalFunctionGrouped()
    {
    }

    @Description("returns interval event_time/interval map")
    @ScalarFunction("build_user_interval_event_grouped")
    @SqlType("map(timestamp(6),bigint)")
    public static SqlMap buildUserIntervalEventGrouped(@TypeParameter("map(timestamp(6),bigint)") MapType toMapType,
            @SqlType("array(timestamp(6))") Block initBlock,
            @SqlType("array(timestamp(6))") Block returnBlock,
            @SqlType("bigint") long windowsGap,
            @SqlType("bigint") long intervalUnit)
    {
        BlockBuilder out = toMapType.createBlockBuilder(null, 1);
        if (initBlock.getPositionCount() > 0 && returnBlock.getPositionCount() > 0) {
            IntervalAggFunction.SessionEntity sessionEntity = new IntervalAggFunction.SessionEntity();
            sessionEntity.setSelfCase(false);
            sessionEntity.setIntervalUnit((int) intervalUnit);
            sessionEntity.setWindowGapMs(windowsGap);
            List<IntervalAggFunction.EventEntity> eventEntities = new ArrayList<>();

            int k;
            IntervalAggFunction.EventEntity eventEntity;
            long microEventTime;
            for (k = 0; k < initBlock.getPositionCount(); ++k) {
                eventEntity = new IntervalAggFunction.EventEntity();
                eventEntity.setEventId(0);
                microEventTime = BigintType.BIGINT.getLong(initBlock, k);
                eventEntity.setEventTime(DateTimes.microsToMillis(microEventTime));
                eventEntities.add(eventEntity);
            }

            for (k = 0; k < returnBlock.getPositionCount(); ++k) {
                eventEntity = new IntervalAggFunction.EventEntity();
                eventEntity.setEventId(1);
                microEventTime = BigintType.BIGINT.getLong(returnBlock, k);
                eventEntity.setEventTime(DateTimes.microsToMillis(microEventTime));
                eventEntities.add(eventEntity);
            }

            eventEntities.sort(Comparator.comparingLong(IntervalAggFunction.EventEntity::getEventTime).thenComparing(IntervalAggFunction.EventEntity::getEventId));
            sessionEntity.setEventEntities(eventEntities);
            IntervalAggFunction.buildNonSelfCaseUserEventInterval(out, sessionEntity);
        }
        else {
            out.appendNull();
        }
        return toMapType.getObject(out.build(), out.getPositionCount() - 1);
    }
}
