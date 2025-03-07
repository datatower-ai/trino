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
package io.trino.plugin.datatower.functions.aggregation.funnel.collect;

import io.trino.plugin.datatower.functions.scalar.funnel.FunnelFunctionPacked;
import io.trino.plugin.datatower.functions.state.funnel.collect.FunnelPackedTimeCollectState;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.BigintType;

import java.util.Objects;

/**
 * @author liulin
 */
@AggregationFunction("funnel_packed_time_collect")
public final class FunnelPackedTimeCollectAggFunction
{
    private FunnelPackedTimeCollectAggFunction()
    {
    }

    @InputFunction
    public static void input(@AggregationState FunnelPackedTimeCollectState state, @SqlType("timestamp(6)") long timestamp, @SqlType("bigint") long funnelIndexBitSet)
    {
        BlockBuilder blockBuilder = state.getData();
        if (Objects.isNull(blockBuilder)) {
            blockBuilder = BigintType.BIGINT.createBlockBuilder(null, 16);
            state.setData(blockBuilder);
        }
        addPackedTimes(timestamp, funnelIndexBitSet, blockBuilder, 1);
    }

    public static void addPackedTimes(long timestamp, long funnelIndexBitSet, BlockBuilder blockBuilder, int funnelIndexStart)
    {
        for (int i = 0; i <= 63; i++) {
            long shifted = funnelIndexBitSet >>> i;
            if (shifted != 0) {
                if ((shifted & 1) == 1) {
                    long packedTime = FunnelFunctionPacked.funnelPackTime(timestamp, funnelIndexStart + i);
                    BigintType.BIGINT.writeLong(blockBuilder, packedTime);
                }
            }
            else {
                return;
            }
        }
    }

    @CombineFunction
    public static void combine(@AggregationState FunnelPackedTimeCollectState state, @AggregationState FunnelPackedTimeCollectState otherState)
    {
        BlockBuilder blockBuilder = state.getData();
        BlockBuilder otherBlockBuilder = otherState.getData();
        if (blockBuilder == null) {
            state.setData(otherBlockBuilder);
        }
        else if (otherBlockBuilder != null) {
            for (int i = 0; i < otherBlockBuilder.getPositionCount(); i++) {
                BigintType.BIGINT.appendTo(otherBlockBuilder.build(), i, blockBuilder);
            }
        }
    }

    @OutputFunction("array(bigint)")
    public static void output(@AggregationState FunnelPackedTimeCollectState state, BlockBuilder out)
    {
        BlockBuilder stateBlockBuilder = state.getData();
        if (stateBlockBuilder == null) {
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
}
