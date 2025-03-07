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
package io.trino.plugin.datatower.functions.aggregation.collect;

import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.trino.plugin.datatower.functions.state.behavior.DynamicSliceOutputState;
import io.trino.plugin.datatower.functions.state.collect.DateValue;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;

import java.util.Arrays;
import java.util.Comparator;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;

/**
 * @author liulin
 */
@AggregationFunction("rq_date_value_collect")
public final class DateValueCollectFunction
{
    private DateValueCollectFunction()
    {
    }

    @InputFunction
    public static void input(@AggregationState DynamicSliceOutputState state,
            @SqlType("timestamp(6)") long timestamp,
            @SqlType("double") double value)
    {
        DynamicSliceOutput sliceOutput = state.getData();
        if (sliceOutput == null) {
            sliceOutput = new DynamicSliceOutput(16);
            state.setData(sliceOutput);
        }
        sliceOutput.appendLong(timestamp);
        sliceOutput.appendDouble(value);
    }

    @CombineFunction
    public static void combine(@AggregationState DynamicSliceOutputState state, @AggregationState DynamicSliceOutputState otherState)
    {
        DateValueArrayCollectFunction.combine(state, otherState);
    }

    @OutputFunction("varbinary")
    public static void output(@AggregationState DynamicSliceOutputState state, BlockBuilder out)
    {
        DateValueArrayCollectFunction.output(state, out);
    }

    public static DateValue[] deserialize(Slice slice)
    {
        int sliceSize = slice.length();
        checkArgument(sliceSize % 16 == 0, "sliceSize % not match unitSize %", sliceSize, 16);
        int count = sliceSize / 16;
        DateValue[] r = new DateValue[count];
        try (BasicSliceInput sliceInput = new BasicSliceInput(slice)) {
            for (int i = 0; i < count; i++) {
                long time = sliceInput.readLong();
                double value = sliceInput.readDouble();
                r[i] = new DateValue(time, value);
            }
            verify(!sliceInput.isReadable(), "sliceInput is still readable");
        }
        Arrays.sort(r, Comparator.comparingLong(dv -> dv.epochMicros));
        return r;
    }
}
