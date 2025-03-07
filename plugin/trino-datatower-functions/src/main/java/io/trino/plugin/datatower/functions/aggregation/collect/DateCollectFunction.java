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

import io.airlift.slice.Slice;
import io.trino.plugin.datatower.functions.state.collect.DateSet;
import io.trino.plugin.datatower.functions.state.collect.DateSetState;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.VarbinaryType;

/**
 * @author liulin
 */
@AggregationFunction("rq_date_collect")
public class DateCollectFunction
{
    private DateCollectFunction()
    {
    }

    @InputFunction
    public static void input(@AggregationState DateSetState state,
            @SqlType("timestamp(6)") long timestamp)
    {
        if (state.getData() == null) {
            DateSet dateSet = new DateSet();
            dateSet.addEpochMicros(timestamp);
            state.setData(dateSet);
        }
        else {
            state.getData().addEpochMicros(timestamp);
        }
    }

    @CombineFunction
    public static void combine(@AggregationState DateSetState state, @AggregationState DateSetState otherState)
    {
        if (state.getData() == null) {
            state.setData(otherState.getData());
        }
        else if (otherState.getData() != null) {
            DateSet merged = DateSet.merge(state.getData(), otherState.getData());
            state.setData(merged);
        }
    }

    @OutputFunction("varbinary")
    public static void output(@AggregationState DateSetState state, BlockBuilder out)
    {
        DateSet data = state.getData();
        if (data == null) {
            out.appendNull();
        }
        else {
            Slice slice = data.serialize();
            VarbinaryType.VARBINARY.writeSlice(out, slice);
        }
    }
}
