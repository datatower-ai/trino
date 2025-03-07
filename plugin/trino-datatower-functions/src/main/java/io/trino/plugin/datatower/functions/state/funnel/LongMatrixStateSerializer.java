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
package io.trino.plugin.datatower.functions.state.funnel;

import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * @author liulin
 */
public class LongMatrixStateSerializer
        implements AccumulatorStateSerializer<LongMatrixState>
{
    public LongMatrixStateSerializer()
    {
    }

    @VisibleForTesting
    static Slice serialize(long[][] data)
    {
        int rowCount = data.length;
        int columnCount = data[0].length;
        int totalSlot = 1 + rowCount * columnCount;
        Slice slice = Slices.allocate(totalSlot * 8);
        slice.setLong(0, rowCount);

        for (int i = 0; i < rowCount; ++i) {
            for (int j = 0; j < columnCount; ++j) {
                int index = 1 + i * columnCount + j;
                slice.setLong(index * 8, data[i][j]);
            }
        }

        return slice;
    }

    @VisibleForTesting
    static long[][] deserialize(Slice slice)
    {
        requireNonNull(slice, "slice is null");
        int totalSlot = slice.length() / 8;
        int rowCount = (int) slice.getLong(0);
        int columnCount = (totalSlot - 1) / rowCount;
        checkState(rowCount * columnCount + 1 == totalSlot, "bad LongMatrix data");
        long[][] data = new long[rowCount][];

        for (int i = 0; i < rowCount; ++i) {
            data[i] = new long[columnCount];

            for (int j = 0; j < columnCount; ++j) {
                int index = 1 + i * columnCount + j;
                data[i][j] = slice.getLong(index * 8);
            }
        }

        return data;
    }

    public Type getSerializedType()
    {
        return VarbinaryType.VARBINARY;
    }

    public void serialize(LongMatrixState state, BlockBuilder out)
    {
        long[][] data = state.getData();
        if (data == null) {
            out.appendNull();
        }
        else {
            Slice slice = serialize(data);
            VarbinaryType.VARBINARY.writeSlice(out, slice);
        }
    }

    public void deserialize(Block block, int index, LongMatrixState state)
    {
        long[][] data = deserialize(VarbinaryType.VARBINARY.getSlice(block, index));
        state.setData(data);
    }
}
