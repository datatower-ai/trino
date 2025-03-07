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
package io.trino.plugin.datatower.functions.state.behavior;

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;

/**
 * @author liulin
 */
public class DynamicSliceOutputStateSerializer
        implements AccumulatorStateSerializer<DynamicSliceOutputState>
{
    public Type getSerializedType()
    {
        return VarbinaryType.VARBINARY;
    }

    public void serialize(DynamicSliceOutputState state, BlockBuilder out)
    {
        DynamicSliceOutput sliceOutput = state.getData();
        if (sliceOutput == null) {
            out.appendNull();
        }
        else {
            VarbinaryType.VARBINARY.writeSlice(out, sliceOutput.slice());
        }
    }

    public void deserialize(Block block, int index, DynamicSliceOutputState state)
    {
        Slice slice = VarbinaryType.VARBINARY.getSlice(block, index);
        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(slice.length());
        sliceOutput.writeBytes(slice);
        state.setData(sliceOutput);
    }
}
