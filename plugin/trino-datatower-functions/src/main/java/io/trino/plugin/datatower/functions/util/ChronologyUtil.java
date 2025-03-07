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

import io.airlift.slice.Slice;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import org.joda.time.DateTimeField;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;

import java.util.Locale;

/**
 * @author liulin
 */
public class ChronologyUtil
{
    public static DateTimeZone defaultZone = DateTimeZone.getDefault();

    private ChronologyUtil()
    {
    }

    public static DateTimeField getTimestampField(ISOChronology chronology, String unitString)
    {
        return switch (unitString) {
            case "millisecond" -> chronology.millisOfSecond();
            case "second" -> chronology.secondOfMinute();
            case "minute" -> chronology.minuteOfHour();
            case "hour" -> chronology.hourOfDay();
            case "day" -> chronology.dayOfMonth();
            case "week" -> chronology.weekOfWeekyear();
            case "month" -> chronology.monthOfYear();
            case "quarter" -> QuarterOfYearDateTimeField.QUARTER_OF_YEAR.getField(chronology);
            case "year" -> chronology.year();
            default -> throw new TrinoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "'" + unitString + "' is not a valid Timestamp field");
        };
    }

    public static DateTimeField getRetentionTimestampField(ISOChronology chronology, Slice unit)
    {
        String unitString = unit.toStringUtf8().toLowerCase(Locale.ENGLISH);
        return switch (unitString) {
            case "day", "week", "month" -> getTimestampField(chronology, unitString);
            default -> throw new TrinoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "'" + unitString + "' is not a valid retention Timestamp field");
        };
    }
}
