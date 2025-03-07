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

import io.trino.plugin.datatower.functions.state.funnel.LongMatrixState;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.SqlMap;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.TimestampType;

import java.util.Objects;

/**
 * @author liulin
 */
@AggregationFunction("funnel_flow_array_date")
public final class FunnelDateMaxStepAggFunction
{
    private FunnelDateMaxStepAggFunction()
    {
    }

    @InputFunction
    public static void input(@AggregationState LongMatrixState state, @SqlType("map(timestamp(6),bigint)") SqlMap mapBlock, @SqlType("bigint") long totalStep)
    {
        int dateDayNum = mapBlock.getSize();
        int rawOffset = mapBlock.getRawOffset();
        long[][] data = state.getData();
        if (Objects.isNull(data)) {
            data = new long[dateDayNum][];
        }

        for (int i = 0; i < dateDayNum; ++i) {
            int columnCount = (int) totalStep + 1;
            if (data[i] == null) {
                data[i] = new long[columnCount];
            }
            long dateTimestamp = TimestampType.TIMESTAMP_MILLIS.getLong(mapBlock.getUnderlyingKeyBlock(), i + rawOffset);
            long maxStep = BigintType.BIGINT.getLong(mapBlock.getRawValueBlock(), i + rawOffset);
            data[i][0] = dateTimestamp;

            for (int j = 1; (long) j <= maxStep; ++j) {
                ++data[i][j];
            }
        }

        state.setData(data);
    }

    @CombineFunction
    public static void combine(@AggregationState LongMatrixState state, @AggregationState LongMatrixState otherState)
    {
        if (state.getData() == null) {
            state.setData(otherState.getData());
            return;
        }
        long[][] data = state.getData();
        long dateDayNum = data.length;
        if (dateDayNum != 0) {
            long[][] otherData = otherState.getData();
            long columnCount = data[0].length;
            for (int i = 0; i < dateDayNum; i++) {
                for (int j = 1; j < columnCount; j++) {
                    data[i][j] = data[i][j] + otherData[i][j];
                }
            }
        }
    }

    @OutputFunction("map(timestamp(6),array(bigint))")
    public static void output(@AggregationState LongMatrixState state, BlockBuilder out)
    {
        long[][] data = state.getData();
        if (data == null) {
            out.appendNull();
            return;
        }
        ((MapBlockBuilder) out).buildEntry(((keyBuilder, valueBuilder) -> {
            for (long[] datum : data) {
                long dateTimestamp = datum[0];
                TimestampType.TIMESTAMP_MILLIS.writeLong(keyBuilder, dateTimestamp);

                ((ArrayBlockBuilder) valueBuilder).buildEntry(elementBuilder -> {
                    int columnCount = datum.length;
                    for (int j = 1; j < columnCount; j++) {
                        BigintType.BIGINT.writeLong(elementBuilder, datum[j]);
                    }
                });
            }
        }));

//        BlockBuilder blockBuilder = out.beginBlockEntry();
//        for (long[] datum : data) {
//            long dateTimestamp = datum[0];
////            TimestampType.TIMESTAMP_MILLIS.writeLong(blockBuilder, dateTimestamp);
//            TimestampType.TIMESTAMP_MILLIS.writeLong(out, dateTimestamp);
//            ArrayType valueArrayType = new ArrayType(BigintType.BIGINT);
//            int columnCount = datum.length;
//
//            BlockBuilder valueArrayBuilder = BigintType.BIGINT.createBlockBuilder(null, columnCount - 1, 8);
//            for (int j = 1; j < columnCount; j++) {
//                BigintType.BIGINT.writeLong(valueArrayBuilder, datum[j]);
////                valueArrayBuilder.writeLong(datum[j]);
//            }
//            valueArrayType.writeObject(out, valueArrayBuilder.build());
//        }
//        out.closeEntry();
    }
}
