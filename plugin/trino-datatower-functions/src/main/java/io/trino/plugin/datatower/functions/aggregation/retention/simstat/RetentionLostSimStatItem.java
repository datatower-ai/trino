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

import static io.airlift.slice.SizeOf.instanceSize;

class RetentionLostSimStatItem
{
    private static final long INSTANCE_SIZE = instanceSize(RetentionLostSimStat.class);
    private final long[] retentionLost;
    private final double[] simStat;

    RetentionLostSimStatItem(long[] retentionLost, double[] simStat)
    {
        this.retentionLost = retentionLost;
        this.simStat = simStat;
    }

    long[] getRetentionLost()
    {
        return this.retentionLost;
    }

    double[] getSimStat()
    {
        return this.simStat;
    }

    int estimateMemorySize()
    {
        return (int) ((long) INSTANCE_SIZE + SizeOf.sizeOf(this.retentionLost) + SizeOf.sizeOf(this.simStat));
    }
}
