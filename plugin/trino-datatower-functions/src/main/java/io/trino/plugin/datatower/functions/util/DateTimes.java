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
package io.trino.plugin.datatower.functions.util;

import org.joda.time.DateTimeField;

/**
 * @author liulin
 */
public final class DateTimes
{
    public static final int MICROSECONDS_PER_MILLISECOND = 1000;
    public static final double MILLISECOND_PER_SECOND = 1000.0d;

    private DateTimes()
    {
    }

    public static long microsToMillis(long epochMicros)
    {
        return Math.floorDiv(epochMicros, 1000);
    }

    public static long millisToSecsCeil(long epochMicros)
    {
        return (long) Math.ceil(epochMicros / 1000.0d);
    }

    public static long millisToMicros(long epochMillis)
    {
        return Math.multiplyExact(epochMillis, 1000);
    }

    public static long roundFloorMicros(DateTimeField dateTimeField, long epochMicros)
    {
        return millisToMicros(dateTimeField.roundFloor(microsToMillis(epochMicros)));
    }
}
