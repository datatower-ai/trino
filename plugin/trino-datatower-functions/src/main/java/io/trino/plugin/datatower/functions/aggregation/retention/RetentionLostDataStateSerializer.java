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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;

import static java.util.Objects.requireNonNull;

/**
 * @author liulin
 */
public class RetentionLostDataStateSerializer
        implements AccumulatorStateSerializer<RetentionLostDataState>
{
    static Slice serialize(RetentionLostData input)
    {
        int rowCount = input.getRowCount();
        long[][] table = input.getTable();
        int columnCount = 0;
        if (rowCount > 0) {
            columnCount = table[0].length;
        }

        int totalSlots = 2 + rowCount * columnCount;
        Slice slice = Slices.allocate(totalSlots * 8);
        int sliceIndex = 0;
        slice.setLong(sliceIndex * 8, rowCount);
        ++sliceIndex;
        slice.setLong(sliceIndex * 8, columnCount);
        ++sliceIndex;

        for (int i = 0; i < rowCount; ++i) {
            for (int j = 0; j < columnCount; ++j) {
                slice.setLong(sliceIndex * 8, table[i][j]);
                ++sliceIndex;
            }
        }

        return slice;
    }

    static RetentionLostData deserialize(Slice slice)
    {
        requireNonNull(slice, "slice is null");
        int totalSlots = slice.length() / 8;
        int sliceIndex = 0;
        int rowCount = (int) slice.getLong(sliceIndex * 8);
        ++sliceIndex;
        int columnCount = (int) slice.getLong(sliceIndex * 8);
        ++sliceIndex;
        if (rowCount * columnCount + 2 != totalSlots) {
            throw new IllegalStateException("bad RetentionLostData");
        }
        else {
            long[][] table = new long[rowCount][];

            for (int i = 0; i < rowCount; ++i) {
                table[i] = new long[columnCount];

                for (int j = 0; j < columnCount; ++j) {
                    table[i][j] = slice.getLong(sliceIndex * 8);
                    ++sliceIndex;
                }
            }

            return new RetentionLostData(table, rowCount);
        }
    }

    @Override
    public Type getSerializedType()
    {
        return VarbinaryType.VARBINARY;
    }

    @Override
    public void serialize(RetentionLostDataState state, BlockBuilder out)
    {
        RetentionLostData retentionLostData = state.getData();
        if (retentionLostData != null && retentionLostData.getRowCount() != 0) {
            Slice slice = serialize(state.getData());
            VarbinaryType.VARBINARY.writeSlice(out, slice);
        }
        else {
            out.appendNull();
        }
    }

    @Override
    public void deserialize(Block block, int index, RetentionLostDataState state)
    {
        RetentionLostData retentionLostData =
                deserialize(VarbinaryType.VARBINARY.getSlice(block, index));
        state.setData(retentionLostData);
    }
}
