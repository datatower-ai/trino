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
package io.trino.plugin.datatower.functions.aggregation.retention;

import io.airlift.slice.SizeOf;
import io.trino.plugin.datatower.functions.util.ChronologyUtil;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.airlift.slice.SizeOf.instanceSize;

class RetentionLostData
{
    private static final long INSTANCE_SIZE = instanceSize(RetentionLostData.class);
    private final long[][] table;
    private final int rowCount;

    public RetentionLostData(long[][] table, int rowCount)
    {
        this.table = table;
        this.rowCount = rowCount;
    }

    static RetentionLostData merge(RetentionLostData left, RetentionLostData right)
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
            long[][] table = new long[rowCount][];
            long[][] leftTable = left.table;
            long[][] rightTable = right.table;
            int leftIndex = 0;
            int rightIndex = 0;
            int i = 0;

            while (true) {
                while (leftIndex < leftRowCount || rightIndex < rightRowCount) {
                    if (leftIndex == leftRowCount) {
                        table[i++] = rightTable[rightIndex++];
                    }
                    else if (rightIndex == rightRowCount) {
                        table[i++] = leftTable[leftIndex++];
                    }
                    else {
                        long[] leftRow = leftTable[leftIndex];
                        long[] rightRow = rightTable[rightIndex];
                        long leftDate = leftRow[0];
                        long rightDate = rightRow[0];
                        if (leftDate < rightDate) {
                            table[i++] = leftRow;
                            ++leftIndex;
                        }
                        else if (leftDate > rightDate) {
                            table[i++] = rightRow;
                            ++rightIndex;
                        }
                        else {
                            int length = leftRow.length;

                            for (int j = 1; j < length; ++j) {
                                leftRow[j] += rightRow[j];
                            }

                            table[i++] = leftRow;
                            ++leftIndex;
                            ++rightIndex;
                        }
                    }
                }
                return new RetentionLostData(table, i);
            }
        }
    }

    public long[][] getTable()
    {
        return this.table;
    }

    public int getRowCount()
    {
        return this.rowCount;
    }

    int estimateMemorySize()
    {
        long rowSize = 0L;
        if (this.rowCount > 0) {
            rowSize = SizeOf.sizeOf(this.table[0]);
        }

        return (int) ((long) INSTANCE_SIZE + SizeOf.sizeOf(this.table) + rowSize * (long) this.rowCount);
    }

    Map<Long, Map<Long, long[]>> toMap()
    {
        Map<Long, long[]> retentionMap = new LinkedHashMap<>();
        Map<Long, long[]> lostMap = new LinkedHashMap<>();
        int n = 0;

        for (int i = 0; i < this.rowCount; ++i) {
            long[] row = this.table[i];
            if (i == 0) {
                n = (row.length - 1) / 2;
            }

            long date = ChronologyUtil.defaultZone.convertLocalToUTC(row[0], false);
            retentionMap.put(date, Arrays.copyOfRange(row, 1, 1 + n));
            lostMap.put(date, Arrays.copyOfRange(row, 1 + n, row.length));
        }

        Map<Long, Map<Long, long[]>> resultMap = new HashMap<>();
        resultMap.put(0L, retentionMap);
        resultMap.put(1L, lostMap);
        return resultMap;
    }
}
