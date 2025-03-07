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
package io.trino.plugin.datatower.functions.scalar.common;

import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.Slice;
import io.trino.plugin.datatower.functions.util.ChronologyUtil;
import io.trino.plugin.datatower.functions.util.DateTimes;
import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import org.joda.time.DateTimeField;
import org.joda.time.LocalDate;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Calendar;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

/**
 * @author liulin
 */
public class DateTimeFunctions
{
    public static final DateTimeFormatter DEFAULT_DATE_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd");
    private static final String WEEK_STR = "week";
    private static final int YMD_DATE_LENGTH = 10;

    private DateTimeFunctions()
    {
    }

    @SqlType("integer")
    @ScalarFunction("rq_day_of_week")
    @Description("Returns the day of week from a date string(yyyy-MM-dd)")
    public static long dayOfWeek(@SqlType("varchar") Slice string)
    {
        try {
            LocalDate localDate = LocalDate.parse(string.toStringUtf8(), DEFAULT_DATE_FORMATTER);
            return localDate.getDayOfWeek();
        }
        catch (Exception e) {
            return -1L;
        }
    }

    @SqlType("integer")
    @ScalarFunction("rq_day_of_week")
    @Description("Returns the day of week from a date")
    public static long dayOfWeek(@SqlType("date") long date)
    {
        try {
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(TimeUnit.DAYS.toMillis(date));
            LocalDate localDate = LocalDate.fromCalendarFields(calendar);
            return localDate.getDayOfWeek();
        }
        catch (Exception e) {
            return -1L;
        }
    }

    @SqlType("bigint")
    @ScalarFunction("rq_to_epoch_milli")
    public static long toEpochMilli(@SqlType("timestamp(6)") long timestamp)
    {
        return ChronologyUtil.defaultZone.convertLocalToUTC(DateTimes.microsToMillis(timestamp), false);
    }

    @SqlType("timestamp(6)")
    @ScalarFunction("rq_date_trunc")
    @Description("Truncate to the specified precision in the session timezone")
    @LiteralParameters("x")
    public static long truncateTimestamp(@SqlType("varchar(x)") Slice unit, @SqlType("timestamp(6)") long timestamp, @SqlType("integer") long firstDayOfWeek)
    {
        String unitString = unit.toStringUtf8().toLowerCase(Locale.ENGLISH);
        long result = doTruncateTimestamp(ISOChronology.getInstanceUTC(), unitString, DateTimes.microsToMillis(timestamp), (int) firstDayOfWeek);
        return DateTimes.millisToMicros(result);
    }

    @SqlType("integer")
    @ScalarFunction("rq_to_date_int")
    public static long toDateInt(@SqlType("timestamp(6)") long epochMicros)
    {
        long epochMillis = DateTimes.microsToMillis(epochMicros);
        ISOChronology chronology = ISOChronology.getInstanceUTC();
        int year = chronology.year().get(epochMillis);
        int month = chronology.monthOfYear().get(epochMillis);
        int day = chronology.dayOfMonth().get(epochMillis);
        return (year * 10000L) + (month * 100L) + day;
    }

    @SqlType("integer")
    @ScalarFunction("rq_parse_date_int")
    public static long parseDateInt(@SqlType("varchar") Slice slice)
    {
        int length = slice.length();
        if (length != YMD_DATE_LENGTH) {
            throw new IllegalArgumentException("Invalid data string " + slice.toStringUtf8());
        }
        int n = 0;
        for (int i = 0; i < YMD_DATE_LENGTH; i++) {
            char c = (char) slice.getByte(i);
            if (i == 4 || i == 7) {
                if (c != '-') {
                    throw new IllegalArgumentException("Invalid data string " + slice.toStringUtf8());
                }
            }
            else if (c < '0' || c > '9') {
                throw new IllegalArgumentException("Invalid data string " + slice.toStringUtf8());
            }
            else {
                n = (n * 10) + (c - '0');
            }
        }
        return n;
    }

    @VisibleForTesting
    static long doTruncateTimestamp(ISOChronology chronology, String unitString, long epochMillis, int firstDayOfWeek)
    {
        long truncated = ChronologyUtil.getTimestampField(chronology, unitString).roundFloor(epochMillis);
        if (!WEEK_STR.equals(unitString) || firstDayOfWeek == 1) {
            return truncated;
        }
        checkArgument(firstDayOfWeek > 1 && firstDayOfWeek <= 7, "firstDayOfWeek should be in [1, 7]");
        int gap = firstDayOfWeek - 1;
        DateTimeField dateTimeField = chronology.dayOfMonth();
        long adjusted = dateTimeField.add(truncated, gap);
        if (adjusted <= epochMillis) {
            return adjusted;
        }
        int newGap = gap - 7;
        long newAdjusted = dateTimeField.add(truncated, newGap);
        checkState(newAdjusted <= epochMillis, "newAdjusted should be less or equal to original timestamp");
        return newAdjusted;
    }
}
