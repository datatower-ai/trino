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
package io.trino.plugin.datatower.functions.state.collect;

import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.datatower.functions.util.DateTimes;
import org.joda.time.chrono.ISOChronology;

import java.util.Arrays;

import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.instanceSize;

/**
 * @author liulin
 */
public class DateSet
{
    public static final int ALIGN_SIZE = 8;
    private static final long INSTANCE_SIZE = instanceSize(DateSet.class);
    protected int[] yearMonths;
    protected int[] daysBitSets;
    protected int size;

    public DateSet()
    {
        this.yearMonths = new int[ALIGN_SIZE];
        this.daysBitSets = new int[ALIGN_SIZE];
        this.size = 0;
    }

    private DateSet(int[] yearMonths, int[] daysBitSets, int size)
    {
        this.yearMonths = yearMonths;
        this.daysBitSets = daysBitSets;
        this.size = size;
    }

    public static DateSet merge(DateSet a, DateSet b)
    {
        if (a.size >= b.size) {
            a.combine(b);
            return a;
        }
        else {
            b.combine(a);
            return b;
        }
    }

    public static DateSet deserialize(Slice slice)
    {
        int length = slice.length();
        int size = length >> 3;
        if (size << 3 != length) {
            throw new IllegalArgumentException("Illegal slice length " + length);
        }
        else {
            int[] yearMonths = new int[size];
            int j = 0;

            for (int i = 0; i < size; ++i) {
                yearMonths[i] = slice.getInt(j * 4);
                ++j;
            }

            int[] daysBitSets = new int[size];

            for (int i = 0; i < size; ++i) {
                daysBitSets[i] = slice.getInt(j * 4);
                ++j;
            }

            return new DateSet(yearMonths, daysBitSets, size);
        }
    }

    int estimateMemorySize()
    {
        return (int) ((long) INSTANCE_SIZE + SizeOf.sizeOf(this.yearMonths) * 2L);
    }

    public void addEpochMicros(long epochMicros)
    {
        //转13位时间戳
        long millis = DateTimes.microsToMillis(epochMicros);
        ISOChronology chronology = ISOChronology.getInstanceUTC();
        //获取年
        int year = chronology.year().get(millis);
        //获取月
        int month = chronology.monthOfYear().get(millis);
        //获取日
        int day = chronology.dayOfMonth().get(millis);
        //组合成int类型 202211 格式的月份
        int yearMonth = year * 100 + month;
        //多少天就左移多少位 一个 int 32位，刚好可以表示所有日期
        int dayBitSet = 1 << day;
        this.addDate(yearMonth, dayBitSet);
    }

    public void addDate(int yearMonth, int dayBitSet)
    {
        if (this.size == 0) {
            //无数据
            this.yearMonths[0] = yearMonth;
            this.daysBitSets[0] = dayBitSet;
            ++this.size;
        }
        else {
            //有数据
            //找到年月的位置，搜不到返回-1
            int index = Arrays.binarySearch(this.yearMonths, 0, this.size, yearMonth);
            if (index >= 0) {
                //找到
                int[] daysBitSetsCopy = this.daysBitSets;
                //对应位置赋值1
                daysBitSetsCopy[index] |= dayBitSet;
            }
            else {
                //没找到,说明是一个新的月日
                if (this.yearMonths.length == this.size) {
                    //年月数量 = DateSet 数量

                    int length = this.ceilingAlign(this.size + ALIGN_SIZE);
                    int[] t = new int[length];
                    System.arraycopy(this.yearMonths, 0, t, 0, this.size);
                    this.yearMonths = t;
                    t = new int[length];
                    System.arraycopy(this.daysBitSets, 0, t, 0, this.size);
                    this.daysBitSets = t;
                }

                index = -index - 1;
                if (index != this.size) {
                    System.arraycopy(this.yearMonths, index, this.yearMonths, index + 1, this.size - index);
                    System.arraycopy(this.daysBitSets, index, this.daysBitSets, index + 1, this.size - index);
                }

                this.yearMonths[index] = yearMonth;
                this.daysBitSets[index] = dayBitSet;
                ++this.size;
            }
        }
    }

    public long[] toEpochMillisArray()
    {
        ISOChronology chronology = ISOChronology.getInstanceUTC();
        int n = 0;

        for (int i = 0; i < this.size; ++i) {
            n += Integer.bitCount(this.daysBitSets[i]);
        }

        long[] millisArray = new long[n];
        int j = 0;

        for (int i = 0; i < this.size; ++i) {
            int yearMonth = this.yearMonths[i];
            int year = yearMonth / 100;
            int month = yearMonth % 100;
            int daysBitSet = this.daysBitSets[i];

            for (int day = 1; day <= 31; ++day) {
                int shifted = daysBitSet >>> day;
                if (shifted == 0) {
                    break;
                }

                if ((shifted & 1) == 1) {
                    long millis = chronology.getDateTimeMillis(year, month, day, 0);
                    millisArray[j++] = millis;
                }
            }
        }

        verify(j == millisArray.length, "micros not full filled", new Object[0]);
        return millisArray;
    }

    public long[] toEpochMicrosArray()
    {
        long[] timestamps = this.toEpochMillisArray();

        for (int i = 0; i < timestamps.length; ++i) {
            timestamps[i] = DateTimes.millisToMicros(timestamps[i]);
        }

        return timestamps;
    }

    private void combine(DateSet other)
    {
        int[] yearMonths = other.yearMonths;
        int[] daysBitSet = other.daysBitSets;
        int size = other.size;

        for (int i = 0; i < size; ++i) {
            this.addDate(yearMonths[i], daysBitSet[i]);
        }
    }

    public Slice serialize()
    {
        Slice slice = Slices.allocate(this.size * ALIGN_SIZE);
        int j = 0;

        int i;
        for (i = 0; i < this.size; ++i) {
            slice.setInt(j * 4, this.yearMonths[i]);
            ++j;
        }

        for (i = 0; i < this.size; ++i) {
            slice.setInt(j * 4, this.daysBitSets[i]);
            ++j;
        }

        return slice;
    }

    int ceilingAlign(int cap)
    {
        return cap + ALIGN_SIZE - 1 >> 3 << 3;
    }
}
