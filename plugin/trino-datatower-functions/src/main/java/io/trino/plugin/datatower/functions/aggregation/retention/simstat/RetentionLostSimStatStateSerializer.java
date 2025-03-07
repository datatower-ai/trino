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

public class RetentionLostSimStatStateSerializer
        implements AccumulatorStateSerializer<RetentionLostSimStatState>
{
    static Slice serialize(RetentionLostSimStat input)
    {
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

        int totalSlots = 3 + rowCount * (columnCount + simStatColumnCount);
        Slice slice = Slices.allocate(totalSlots * 8);
        int sliceIndex = 0;
        slice.setLong(0, rowCount);
        ++sliceIndex;
        slice.setLong(sliceIndex * 8, simStatColumnCount);
        ++sliceIndex;
        slice.setLong(sliceIndex * 8, input.getStatType());
        ++sliceIndex;

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

    static RetentionLostSimStat deserialize(Slice slice)
    {
        requireNonNull(slice, "slice is null");
        int totalSlots = slice.length() / 8;
        int sliceIndex = 0;
        int rowCount = (int) slice.getLong(0);
        ++sliceIndex;
        int simStatColumnCount = (int) slice.getLong(sliceIndex * 8);
        ++sliceIndex;
        int columnCount = simStatColumnCount * 2 + 1;
        if (rowCount * (simStatColumnCount + columnCount) + 3 != totalSlots) {
            throw new IllegalStateException("bad RetentionLostSimStat");
        }
        else {
            long statType = slice.getLong(sliceIndex * 8);
            ++sliceIndex;
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

            return new RetentionLostSimStat(table, rowCount, statType);
        }
    }

    public Type getSerializedType()
    {
        return VarbinaryType.VARBINARY;
    }

    public void serialize(RetentionLostSimStatState state, BlockBuilder out)
    {
        RetentionLostSimStat retentionLostData = state.getData();
        if (retentionLostData != null && retentionLostData.getRowCount() != 0) {
            Slice slice = serialize(state.getData());
            VarbinaryType.VARBINARY.writeSlice(out, slice);
        }
        else {
            out.appendNull();
        }
    }

    public void deserialize(Block block, int index, RetentionLostSimStatState state)
    {
        RetentionLostSimStat retentionLostData = deserialize(VarbinaryType.VARBINARY.getSlice(block, index));
        state.setData(retentionLostData);
    }
}
