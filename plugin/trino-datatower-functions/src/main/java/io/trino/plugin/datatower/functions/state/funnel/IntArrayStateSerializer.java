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

import static java.util.Objects.requireNonNull;

/**
 * @author liulin
 */
public class IntArrayStateSerializer
        implements AccumulatorStateSerializer<IntArrayState>
{
    @VisibleForTesting
    static Slice serialize(int[] data)
    {
        int length = data.length;
        Slice slice = Slices.allocate(length * 4);
        for (int i = 0; i < length; i++) {
            slice.setInt(i * 4, data[i]);
        }
        return slice;
    }

    @VisibleForTesting
    static int[] deserialize(Slice slice)
    {
        requireNonNull(slice, "slice is null");
        int length = slice.length() / 4;
        int[] data = new int[length];
        for (int i = 0; i < length; i++) {
            data[i] = slice.getInt(i * 4);
        }
        return data;
    }

    public Type getSerializedType()
    {
        return VarbinaryType.VARBINARY;
    }

    public void serialize(IntArrayState state, BlockBuilder out)
    {
        int[] data = state.getData();
        if (data == null) {
            out.appendNull();
            return;
        }
        Slice slice = serialize(data);
        VarbinaryType.VARBINARY.writeSlice(out, slice);
    }

    public void deserialize(Block block, int index, IntArrayState state)
    {
        int[] data = deserialize(VarbinaryType.VARBINARY.getSlice(block, index));
        state.setData(data);
    }
}
