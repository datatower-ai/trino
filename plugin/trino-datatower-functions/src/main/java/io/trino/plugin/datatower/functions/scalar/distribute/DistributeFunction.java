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
package io.trino.plugin.datatower.functions.scalar.distribute;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * @author liulin
 */
public class DistributeFunction
{
    private static final double MIN_DIFFERENCE = 6.0D;
    private static final double MAX_DIFFERENCE = 10.0D;

    private DistributeFunction()
    {
    }

    @SqlType("varchar")
    @ScalarFunction("get_distribute_group_str")
    @Description("returns the distribute group of stat value")
    public static Slice getDistributeGroup(@SqlType("double") double minVal,
            @SqlType("double") double maxVal,
            @SqlType("integer") long discreteLimit,
            @SqlType("bigint") long num,
            @SqlType("double") double statVal)
    {
        long d;
        if (num <= discreteLimit) {
            return Slices.utf8Slice(BigDecimal.valueOf(statVal).stripTrailingZeros().toPlainString());
        }
        double difference = maxVal - minVal;
        if (difference <= MAX_DIFFERENCE) {
            return Slices.utf8Slice(getSimpleGroupStr(statVal, minVal, maxVal));
        }
        long dMax = (long) (difference / MIN_DIFFERENCE);
        long dMin = (long) (difference / MAX_DIFFERENCE);
        int[] dMaxArray = getNumLengthAndFirstVal(dMax);
        int[] dMinArray = getNumLengthAndFirstVal(dMin);
        int dMaxLength = dMaxArray[0];
        int dMaxFirstVal = dMaxArray[1];
        int dMinLength = dMinArray[0];
        int dMinFirstVal = dMinArray[1];
        if (dMaxLength != dMinLength) {
            d = (long) Math.pow(MAX_DIFFERENCE, dMaxLength - 1);
        }
        else if (dMaxFirstVal != dMinFirstVal) {
            d = (long) (dMaxFirstVal * Math.pow(MAX_DIFFERENCE, dMaxLength - 1));
        }
        else {
            d = (long) ((dMaxFirstVal + 0.5d) * Math.pow(MAX_DIFFERENCE, dMaxLength - 1));
        }
        return Slices.utf8Slice(getGroupStr(statVal, d, minVal, maxVal));
    }

    private static int[] getNumLengthAndFirstVal(long num)
    {
        int count = 1;

        int firstDigtitNum;
        for (firstDigtitNum = (int) num; (num /= 10L) != 0L; firstDigtitNum = (int) num) {
            ++count;
        }

        return new int[] {count, firstDigtitNum};
    }

    private static String getGroupStr(double statVal, long d, double minVal, double maxVal)
    {
        long minIndex = getGroupIndex(minVal, d);
        long maxIndex = getGroupIndex(maxVal, d);
        long index = getGroupIndex(statVal, d);
        if (index <= minIndex) {
            return "," + (index + 1L) * d;
        }
        else {
            return index >= maxIndex ? index * d + "," : index * d + "," + (index + 1L) * d;
        }
    }

    private static long getGroupIndex(double val, long d)
    {
        if (val < 0.0D) {
            return val % (double) d == 0.0D ? (long) (val / (double) d) : (long) (val / (double) d - 1.0D);
        }
        else {
            return (long) (val / (double) d);
        }
    }

    public static String getSimpleGroupStr(double statVal0, double minVal0, double maxVal0)
    {
        BigDecimal min = (new BigDecimal(minVal0)).setScale(1, RoundingMode.FLOOR).stripTrailingZeros();
        BigDecimal max = (new BigDecimal(maxVal0)).setScale(1, RoundingMode.CEILING).stripTrailingZeros();
        BigDecimal statVal = (new BigDecimal(statVal0)).setScale(2, RoundingMode.HALF_UP).stripTrailingZeros();
        int defParts = 10;
        BigDecimal step = max.subtract(min).divide(new BigDecimal(defParts), 2, RoundingMode.HALF_UP).stripTrailingZeros();
        if (step.compareTo(BigDecimal.ZERO) > 0) {
            BigDecimal lowLimit = min;

            for (int i = 0; i < defParts; ++i) {
                BigDecimal upLimit = lowLimit.add(step);
                if (i == 0) {
                    if (statVal.compareTo(upLimit) < 0) {
                        return "," + upLimit;
                    }
                }
                else if (i == defParts - 1) {
                    if (lowLimit.compareTo(statVal) <= 0) {
                        return lowLimit + ",";
                    }
                }
                else if (lowLimit.compareTo(statVal) <= 0 && statVal.compareTo(upLimit) < 0) {
                    return lowLimit + "," + upLimit;
                }

                lowLimit = upLimit;
            }
        }
        return min + "," + max;
    }
}
