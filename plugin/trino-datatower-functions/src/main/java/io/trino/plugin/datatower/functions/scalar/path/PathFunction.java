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
package io.trino.plugin.datatower.functions.scalar.path;

import com.google.common.collect.ImmutableList;
import io.trino.spi.block.Block;
import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.VarcharType;

import java.util.ArrayList;
import java.util.List;

/**
 * @author liulin
 */
public class PathFunction
{
    private PathFunction()
    {
    }

    @SqlType("boolean")
    @ScalarFunction("is_match_event_session_pattern")
    @Description("check if user has event sessions pattern")
    @LiteralParameters("x")
    public static boolean isMatchEventSession(@SqlType("array(array(array(varchar)))") Block userSessionArrayBlock,
            @SqlType("bigint") long sessionLevel,
            @SqlType("boolean") boolean sliceIsMore,
            @SqlType("array(array(varchar))") Block sliceEventArray,
            @SqlType("bigint") long sliceType,
            @SqlType("boolean") boolean nextSliceIsMore,
            @SqlNullable @SqlType("array(array(varchar))") Block nextSliceEventArray)
    {
        int eventSize = userSessionArrayBlock.getPositionCount();
        ArrayType arrayType = new ArrayType(VarcharType.VARCHAR);
        ArrayType sessionArrayType = new ArrayType(new ArrayType(VarcharType.VARCHAR));
        List<List<String>> sliceEvents = parseSliceEvent(arrayType, sliceEventArray);
        List<List<String>> nextSliceEvents = parseSliceEvent(arrayType, nextSliceEventArray);
        for (int i = 0; i < eventSize; ++i) {
            Block arrayBlock = sessionArrayType.getObject(userSessionArrayBlock, i);
            List<List<String>> sessionEvents = parseSliceEvent(arrayType, arrayBlock);
            boolean matched = isSessionPatternMatch(sessionEvents, (int) sessionLevel, sliceIsMore, sliceEvents, sliceType, nextSliceIsMore, nextSliceEvents);
            if (matched) {
                return true;
            }
        }
        return false;
    }

    private static List<List<String>> parseSliceEvent(ArrayType type, Block sliceEventArray)
    {
        List<List<String>> events = new ArrayList<>();
        int eventSize = sliceEventArray.getPositionCount();
        for (int i = 0; i < eventSize; ++i) {
            Block itemBlock = type.getObject(sliceEventArray, i);
            String eventName = VarcharType.VARCHAR.getSlice(itemBlock, 0).toStringUtf8();
            if (itemBlock.getPositionCount() > 1) {
                String attrName = VarcharType.VARCHAR.getSlice(itemBlock, 1).toStringUtf8();
                events.add(ImmutableList.of(eventName, attrName));
            }
            else {
                events.add(ImmutableList.of(eventName));
            }
        }
        return events;
    }

    public static boolean isSessionPatternMatch(List<List<String>> sessionList, int sessionLevel, boolean sliceIsMore, List<List<String>> sliceEvents, long sliceType, boolean nextSliceIsMore, List<List<String>> nextSliceEvents)
    {
        if (!isInvalidSessionLevel(sessionList, sessionLevel)) {
            List<String> currentEvent = sessionList.get(sessionLevel);
            if (isSliceNoneMoreCase(sliceIsMore, sliceEvents, currentEvent) || isSliceMoreCase(sliceIsMore, sliceEvents, currentEvent)) {
                switch ((int) sliceType) {
                    case 1:
                        return true;
                    case 2:
                        return sessionList.size() > sessionLevel + 1;
                    case 3:
                        return sessionList.size() == sessionLevel + 1;
                    case 4:
                        if (sessionList.size() == sessionLevel + 1) {
                            return false;
                        }

                        if (nextSliceEvents == null) {
                            return false;
                        }
                        List<String> nextEvent = sessionList.get(sessionLevel + 1);
                        if (isSliceNoneMoreCase(nextSliceIsMore, nextSliceEvents, nextEvent)) {
                            return true;
                        }
                        return isSliceMoreCase(nextSliceIsMore, nextSliceEvents, nextEvent);
                }
            }
        }
        return false;
    }

    private static boolean isSliceNoneMoreCase(boolean sliceIsMore, List<List<String>> sliceEvents, List<String> currentEvent)
    {
        return !sliceIsMore && sliceEvents.contains(currentEvent);
    }

    private static boolean isSliceMoreCase(boolean sliceIsMore, List<List<String>> sliceEvents, List<String> currentEvent)
    {
        return sliceIsMore && !sliceEvents.contains(currentEvent);
    }

    private static boolean isInvalidSessionLevel(List<List<String>> sessionList, long sessionLevel)
    {
        return (long) sessionList.size() < sessionLevel + 1L;
    }
}
