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
package io.trino.plugin.datatower.functions.aggregation.retention.formula;

import io.airlift.slice.SizeOf;

import java.util.Arrays;

import static io.airlift.slice.SizeOf.instanceSize;

/**
 * @author liulin
 */
class RetentionLostSimStatFormulaItem
{
    private static final long INSTANCE_SIZE = instanceSize(RetentionLostSimStatFormulaItem.class);
    private final long[] retentionLost;
    private final double[][] simStats;

    public RetentionLostSimStatFormulaItem(long[] retentionLost, double[][] simStats)
    {
        this.retentionLost = retentionLost;
        this.simStats = simStats;
    }

    public long[] getRetentionLost()
    {
        return this.retentionLost;
    }

    public double[][] getSimStats()
    {
        return this.simStats;
    }

    public int estimateMemorySize()
    {
        return (int) (INSTANCE_SIZE + SizeOf.sizeOf(this.retentionLost) + SizeOf.sizeOf(this.simStats) + (SizeOf.sizeOf(this.simStats[0]) * this.simStats.length));
    }

    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RetentionLostSimStatFormulaItem that = (RetentionLostSimStatFormulaItem) o;
        return Arrays.equals(this.retentionLost, that.retentionLost) && Arrays.deepEquals(this.simStats, that.simStats);
    }

    public int hashCode()
    {
        int result = Arrays.hashCode(this.retentionLost);
        return (31 * result) + Arrays.hashCode(this.simStats);
    }
}
