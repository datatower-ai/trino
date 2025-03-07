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
public class RetentionLostQuotaDataStateSerializer
        implements AccumulatorStateSerializer<RetentionLostQuotaDataState>
{
    static Slice serialize(RetentionLostQuotaData input)
    {
        long[] quotaTypeArr = input.getQuotaType();
        long[] quotaIndexArr = input.getQuotaIndex();
        int quotaTypeLength = quotaTypeArr.length;
        int quotaIndexLength = quotaIndexArr.length;
        int rowCount = input.getRowCount();
        long[][] table = input.getTable();
        int columnCount = 0;
        if (rowCount > 0) {
            columnCount = table[0].length;
        }

        int totalSlots = 4 + quotaTypeLength + quotaIndexLength + rowCount * columnCount;
        Slice slice = Slices.allocate(totalSlots * 8);
        int sliceIndex = 0;
        slice.setLong(0, rowCount);
        ++sliceIndex;
        slice.setLong(sliceIndex * 8, columnCount);
        ++sliceIndex;
        slice.setLong(sliceIndex * 8, quotaTypeLength);
        ++sliceIndex;
        slice.setLong(sliceIndex * 8, quotaIndexLength);
        ++sliceIndex;

        for (long quotaType : quotaTypeArr) {
            slice.setLong(sliceIndex * 8, quotaType);
            ++sliceIndex;
        }
        for (long quotaIndex : quotaIndexArr) {
            slice.setLong(sliceIndex * 8, quotaIndex);
            ++sliceIndex;
        }

        for (int i = 0; i < rowCount; ++i) {
            for (int j = 0; j < columnCount; ++j) {
                slice.setLong(sliceIndex * 8, table[i][j]);
                ++sliceIndex;
            }
        }

        return slice;
    }

    static RetentionLostQuotaData deserialize(Slice slice)
    {
        requireNonNull(slice, "slice is null");
        int totalSlots = slice.length() / 8;
        int sliceIndex = 0;
        int rowCount = (int) slice.getLong(0);
        ++sliceIndex;
        int columnCount = (int) slice.getLong(sliceIndex * 8);
        ++sliceIndex;
        int quotaTypeLength = (int) slice.getLong(sliceIndex * 8);
        ++sliceIndex;
        int quotaIndexLength = (int) slice.getLong(sliceIndex * 8);
        ++sliceIndex;

        long[] quotaTypeArr = new long[quotaTypeLength];
        for (int i = 0; i < quotaTypeLength; i++) {
            int quotaType = (int) slice.getLong(sliceIndex * 8);
            quotaTypeArr[i] = quotaType;
            ++sliceIndex;
        }

        long[] quotaIndexArr = new long[quotaIndexLength];
        for (int i = 0; i < quotaIndexLength; i++) {
            int quotaIndex = (int) slice.getLong(sliceIndex * 8);
            quotaIndexArr[i] = quotaIndex;
            ++sliceIndex;
        }

        if (rowCount * columnCount + 4 + quotaTypeLength + quotaIndexLength != totalSlots) {
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

            return new RetentionLostQuotaData(table, rowCount, quotaTypeArr, quotaIndexArr);
        }
    }

    public Type getSerializedType()
    {
        return VarbinaryType.VARBINARY;
    }

    public void serialize(RetentionLostQuotaDataState state, BlockBuilder out)
    {
        RetentionLostQuotaData retentionLostQuotaData = state.getData();
        if (retentionLostQuotaData != null && retentionLostQuotaData.getRowCount() != 0) {
            Slice slice = serialize(retentionLostQuotaData);
            VarbinaryType.VARBINARY.writeSlice(out, slice);
        }
        else {
            out.appendNull();
        }
    }

    public void deserialize(Block block, int index, RetentionLostQuotaDataState state)
    {
        RetentionLostQuotaData retentionLostQuotaData = deserialize(VarbinaryType.VARBINARY.getSlice(block, index));
        state.setData(retentionLostQuotaData);
    }
}
