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
package io.trino.plugin.datatower.functions.aggregation.retention.simstat;

import io.airlift.slice.SizeOf;
import io.trino.plugin.datatower.functions.util.ChronologyUtil;
import io.trino.plugin.datatower.functions.util.CommonUtil;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.airlift.slice.SizeOf.instanceSize;

/**
 * @author liulin
 */
public class RetentionLostSimStat
{
    private static final long INSTANCE_SIZE = instanceSize(RetentionLostSimStat.class);
    private final RetentionLostSimStatItem[] table;
    private final int rowCount;
    private final long statType;

    RetentionLostSimStat(RetentionLostSimStatItem[] table, int rowCount, long statType)
    {
        this.table = table;
        this.rowCount = rowCount;
        this.statType = statType;
    }

    static RetentionLostSimStat merge(RetentionLostSimStat left, RetentionLostSimStat right)
    {
        int leftRowCount = left.rowCount;
        int rightRowCount = right.rowCount;
        if (rightRowCount == 0) {
            return left;
        }
        else if (leftRowCount == 0) {
            return right;
        }
        else {
            int rowCount = leftRowCount + rightRowCount;
            RetentionLostSimStatItem[] table = new RetentionLostSimStatItem[rowCount];
            RetentionLostSimStatItem[] leftTable = left.table;
            RetentionLostSimStatItem[] rightTable = right.table;
            int leftIndex = 0;
            int rightIndex = 0;
            int i = 0;

            while (leftIndex < leftRowCount || rightIndex < rightRowCount) {
                if (leftIndex == leftRowCount) {
                    table[i++] = rightTable[rightIndex++];
                }
                else if (rightIndex == rightRowCount) {
                    table[i++] = leftTable[leftIndex++];
                }
                else {
                    RetentionLostSimStatItem leftRow = leftTable[leftIndex];
                    RetentionLostSimStatItem rightRow = rightTable[rightIndex];
                    long leftDate = leftRow.getRetentionLost()[0];
                    long rightDate = rightRow.getRetentionLost()[0];
                    if (leftDate < rightDate) {
                        table[i++] = leftRow;
                        ++leftIndex;
                    }
                    else if (leftDate > rightDate) {
                        table[i++] = rightRow;
                        ++rightIndex;
                    }
                    else {
                        long[] leftRetentionLost = leftRow.getRetentionLost();
                        long[] rightRetentionLost = rightRow.getRetentionLost();
                        int length = leftRetentionLost.length;

                        for (int j = 1; j < length; ++j) {
                            leftRetentionLost[j] += rightRetentionLost[j];
                        }

                        double[] leftSimStat = leftRow.getSimStat();
                        double[] rightSimStat = rightRow.getSimStat();
                        length = leftSimStat.length;

                        for (int j = 0; j < length; ++j) {
                            leftSimStat[j] += rightSimStat[j];
                        }

                        table[i++] = leftRow;
                        ++leftIndex;
                        ++rightIndex;
                    }
                }
            }
            return new RetentionLostSimStat(table, i, left.getStatType());
        }
    }

    RetentionLostSimStatItem[] getTable()
    {
        return this.table;
    }

    int getRowCount()
    {
        return this.rowCount;
    }

    public long getStatType()
    {
        return this.statType;
    }

    int estimateMemorySize()
    {
        long rowSize = 0L;
        if (this.rowCount > 0) {
            rowSize = this.table[0].estimateMemorySize();
        }

        return (int) ((long) INSTANCE_SIZE + SizeOf.sizeOf(this.table) + rowSize * (long) this.rowCount);
    }

    Map<Long, Object> toMap()
    {
        Map<Long, long[]> retentionMap = new LinkedHashMap<>();
        Map<Long, long[]> lostMap = new LinkedHashMap<>();
        Map<Long, double[]> simStatMap = new LinkedHashMap<>();
        int n = 0;

        long initNum;

        for (int i = 0; i < this.rowCount; ++i) {
            RetentionLostSimStatItem item = this.table[i];
            double[] vals = item.getSimStat();
            if (i == 0) {
                n = vals.length;
            }

            long[] row = item.getRetentionLost();
            initNum = ChronologyUtil.defaultZone.convertLocalToUTC(row[0], false);
            retentionMap.put(initNum, Arrays.copyOfRange(row, 1, 1 + n));
            lostMap.put(initNum, Arrays.copyOfRange(row, 1 + n, row.length));
            simStatMap.put(initNum, vals);
        }

        if (StatType.TIME_UNIT_AVG_BY_USER == this.statType) {
            for (Map.Entry<Long, double[]> entry : simStatMap.entrySet()) {
                Long date = entry.getKey();
                double[] vals = entry.getValue();
                long[] nums = retentionMap.get(date);
                for (int i = 1; i < vals.length; ++i) {
                    if (nums[i] == 0L) {
                        vals[i] = 0.0;
                    }
                    else {
                        vals[i] = CommonUtil.setScale(vals[i] / (double) nums[i], 8);
                    }
                }
            }
        }
        else if (StatType.TIME_PHASE_SUM == this.statType) {
            for (Map.Entry<Long, double[]> entry : simStatMap.entrySet()) {
                double[] vals = entry.getValue();
                double totalVal = 0.0;
                for (int i = 1; i < vals.length; ++i) {
                    vals[i] += totalVal;
                    totalVal = vals[i];
                }
            }
        }
        else if (StatType.TIME_PHASE_AVG_BY_USER == this.statType) {
            for (Map.Entry<Long, double[]> entry : simStatMap.entrySet()) {
                Long date = entry.getKey();
                double[] vals = entry.getValue();
                initNum = ((long[]) retentionMap.get(date))[0];
                double totalVal = 0.0;
                for (int i = 1; i < vals.length; ++i) {
                    totalVal += vals[i];
                    vals[i] = CommonUtil.setScale(totalVal / (double) initNum, 8);
                }
            }
        }

        Map<Long, Object> resultMap = new HashMap<>(4);
        resultMap.put(0L, retentionMap);
        resultMap.put(1L, lostMap);
        resultMap.put(2L, simStatMap);
        return resultMap;
    }
}
