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
package io.trino.plugin.datatower.functions.aggregation.funnel;

import io.trino.plugin.datatower.functions.state.funnel.IntArrayState;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.IntegerType;

/**
 * @author liulin
 */
@AggregationFunction("funnel_flow_array")
public final class FunnelTotalMaxStepAggFunction
{
    private FunnelTotalMaxStepAggFunction()
    {
    }

    @InputFunction
    public static void input(@AggregationState IntArrayState state, @SqlType("bigint") long value, @SqlType("bigint") long totalStep)
    {
        int[] data = state.getData();
        if (data == null) {
            data = new int[(int) totalStep];
        }
        for (int i = 0; i < value; i++) {
            data[i] = data[i] + 1;
        }
        state.setData(data);
    }

    @CombineFunction
    public static void combine(@AggregationState IntArrayState state, @AggregationState IntArrayState otherState)
    {
        if (state.getData() == null) {
            state.setData(otherState.getData());
            return;
        }
        int[] otherData = otherState.getData();
        int[] data = state.getData();
        int length = data.length;
        for (int i = 0; i < length; i++) {
            data[i] = data[i] + otherData[i];
        }
    }

    @OutputFunction("array(integer)")
    public static void output(@AggregationState IntArrayState state, BlockBuilder out)
    {
        int[] data = state.getData();
        if (data == null) {
            out.appendNull();
            return;
        }
        ((ArrayBlockBuilder) out).buildEntry(elementBuilder -> {
            for (int datum : data) {
                IntegerType.INTEGER.writeLong(elementBuilder, datum);
            }
        });

//        BlockBuilder blockBuilder = out.beginBlockEntry();
//        for (int datum : data) {
////            IntegerType.INTEGER.writeLong(blockBuilder, datum);
//            IntegerType.INTEGER.writeLong(out, datum);
//        }
//        out.closeEntry();
    }
}
