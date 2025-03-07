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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;

import static java.util.Objects.requireNonNull;

public class RetentionLostSimStatQuotaStateSerializer
        implements AccumulatorStateSerializer<RetentionLostSimStatQuotaState>
{
    static Slice serialize(RetentionLostSimStatQuota input)
    {
        long[] quotaTypeArr = input.getQuotaType();
        long[] quotaIndexArr = input.getQuotaIndex();
        int quotaTypeLength = quotaTypeArr.length;
        int quotaIndexLength = quotaIndexArr.length;
        int rowCount = input.getRowCount();
        RetentionLostSimStatItem[] table = input.getTable();
        int columnCount = 0;
        int simStatColumnCount = 0;
        if (rowCount > 0) {
            columnCount = table[0].getRetentionLost().length;
            simStatColumnCount = table[0].getSimStat().length;
            if (1 + simStatColumnCount * 2 != columnCount) {
                throw new IllegalStateException("bad RetentionLostSimStatItem");
            }
        }

        int totalSlots = 5 + quotaTypeLength + quotaIndexLength + rowCount * (columnCount + simStatColumnCount);
        Slice slice = Slices.allocate(totalSlots * 8);
        int sliceIndex = 0;
        slice.setLong(0, rowCount);
        ++sliceIndex;
        slice.setLong(sliceIndex * 8, simStatColumnCount);
        ++sliceIndex;
        slice.setLong(sliceIndex * 8, input.getStatType());
        ++sliceIndex;
        slice.setLong(sliceIndex * 8, quotaTypeLength);
        ++sliceIndex;
        slice.setLong(sliceIndex * 8, quotaIndexLength);
        ++sliceIndex;

        for (long quotaType : quotaTypeArr) {
            slice.setLong(sliceIndex * 8, quotaType);
            ++sliceIndex;
        }
        for (long quotaIndex : quotaIndexArr) {
            slice.setLong(sliceIndex * 8, quotaIndex);
            ++sliceIndex;
        }

        for (int i = 0; i < rowCount; ++i) {
            long[] retentionLost = table[i].getRetentionLost();

            for (int j = 0; j < columnCount; ++j) {
                slice.setLong(sliceIndex * 8, retentionLost[j]);
                ++sliceIndex;
            }

            double[] simStat = table[i].getSimStat();

            for (int j = 0; j < simStatColumnCount; ++j) {
                slice.setDouble(sliceIndex * 8, simStat[j]);
                ++sliceIndex;
            }
        }

        return slice;
    }

    static RetentionLostSimStatQuota deserialize(Slice slice)
    {
        requireNonNull(slice, "slice is null");
        int totalSlots = slice.length() / 8;
        int sliceIndex = 0;
        int rowCount = (int) slice.getLong(0);
        ++sliceIndex;
        int simStatColumnCount = (int) slice.getLong(sliceIndex * 8);
        ++sliceIndex;
        long statType = slice.getLong(sliceIndex * 8);
        ++sliceIndex;
        int quotaTypeLength = (int) slice.getLong(sliceIndex * 8);
        ++sliceIndex;
        int quotaIndexLength = (int) slice.getLong(sliceIndex * 8);
        ++sliceIndex;

        long[] quotaTypeArr = new long[quotaTypeLength];
        for (int i = 0; i < quotaTypeLength; i++) {
            int quotaType = (int) slice.getLong(sliceIndex * 8);
            quotaTypeArr[i] = quotaType;
            ++sliceIndex;
        }

        long[] quotaIndexArr = new long[quotaIndexLength];
        for (int i = 0; i < quotaIndexLength; i++) {
            int quotaIndex = (int) slice.getLong(sliceIndex * 8);
            quotaIndexArr[i] = quotaIndex;
            ++sliceIndex;
        }

        int columnCount = simStatColumnCount * 2 + 1;

        if (rowCount * (simStatColumnCount + columnCount) + 5 + quotaTypeLength + quotaIndexLength != totalSlots) {
            throw new IllegalStateException("bad RetentionLostSimStat");
        }
        else {
            RetentionLostSimStatItem[] table = new RetentionLostSimStatItem[rowCount];
            for (int i = 0; i < rowCount; ++i) {
                long[] retentionLost = new long[columnCount];

                for (int j = 0; j < columnCount; ++j) {
                    retentionLost[j] = slice.getLong(sliceIndex * 8);
                    ++sliceIndex;
                }

                double[] simStat = new double[simStatColumnCount];

                for (int j = 0; j < simStatColumnCount; ++j) {
                    simStat[j] = slice.getDouble(sliceIndex * 8);
                    ++sliceIndex;
                }

                table[i] = new RetentionLostSimStatItem(retentionLost, simStat);
            }

            return new RetentionLostSimStatQuota(table, rowCount, statType, quotaTypeArr, quotaIndexArr);
        }
    }

    public Type getSerializedType()
    {
        return VarbinaryType.VARBINARY;
    }

    public void serialize(RetentionLostSimStatQuotaState state, BlockBuilder out)
    {
        RetentionLostSimStatQuota retentionLostSimStatQuota = state.getData();
        if (retentionLostSimStatQuota != null && retentionLostSimStatQuota.getRowCount() != 0) {
            Slice slice = serialize(state.getData());
            VarbinaryType.VARBINARY.writeSlice(out, slice);
        }
        else {
            out.appendNull();
        }
    }

    public void deserialize(Block block, int index, RetentionLostSimStatQuotaState state)
    {
        RetentionLostSimStatQuota retentionLostSimStatQuota = deserialize(VarbinaryType.VARBINARY.getSlice(block, index));
        state.setData(retentionLostSimStatQuota);
    }
}
