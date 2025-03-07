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
package io.trino.plugin.datatower.functions.state.funnel.collect;

import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.Type;

import java.util.Objects;

public class FunnelPackedTimeCollectStateSerializer
        implements AccumulatorStateSerializer<FunnelPackedTimeCollectState>
{
    private final Type arrayType = new ArrayType(BigintType.BIGINT);

    public Type getSerializedType()
    {
        return this.arrayType;
    }

    public void serialize(FunnelPackedTimeCollectState state, BlockBuilder out)
    {
        BlockBuilder stateBlockBuilder = state.getData();
        if (Objects.isNull(stateBlockBuilder)) {
            out.appendNull();
            return;
        }

        ((ArrayBlockBuilder) out).buildEntry(elementBuilder -> {
            for (int i = 0; i < stateBlockBuilder.getPositionCount(); i++) {
                BigintType.BIGINT.appendTo(stateBlockBuilder.build(), i, elementBuilder);
            }
        });

//        BlockBuilder entryBuilder = out.beginBlockEntry();
//        for (int i = 0; i < stateBlock.getPositionCount(); i++) {
//            BigintType.BIGINT.appendTo(stateBlock, i, out);
//        }
//        out.closeEntry();
    }

    public void deserialize(Block block, int index, FunnelPackedTimeCollectState state)
    {
        Block stateBlock = (Block) this.arrayType.getObject(block, index);
        BlockBuilder blockBuilder = BigintType.BIGINT.createBlockBuilder(null, stateBlock.getPositionCount());
        for (int i = 0; i < stateBlock.getPositionCount(); i++) {
            BigintType.BIGINT.appendTo(stateBlock, i, blockBuilder);
        }
        state.setData(blockBuilder);
    }
}
