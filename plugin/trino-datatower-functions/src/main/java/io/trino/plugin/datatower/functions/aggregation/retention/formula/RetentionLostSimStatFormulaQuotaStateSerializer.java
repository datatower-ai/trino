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
package io.trino.plugin.datatower.functions.aggregation.retention.formula;

import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
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
public class RetentionLostSimStatFormulaQuotaStateSerializer
        implements AccumulatorStateSerializer<RetentionLostSimStatFormulaQuotaState>
{
    static Slice serialize(RetentionLostSimStatFormulaQuota input)
    {
        int rowCount = input.getRowCount();
        long[] statTypes = input.getStatTypes();
        RetentionLostSimStatFormulaItem[] table = input.getTable();
        Slice formula = input.getFormula();
        long[] quotaTypeArr = input.getQuotaType();
        long[] quotaIndexArr = input.getQuotaIndex();
        int columnCount = 0;
        int simStatColumnCount = 0;
        int formulaVarNum = statTypes.length;
        int formulaLength = formula.length();
        int quotaTypeLength = quotaTypeArr.length;
        int quotaIndexLength = quotaIndexArr.length;
        if (rowCount > 0) {
            columnCount = table[0].getRetentionLost().length;
            double[][] simStats = table[0].getSimStats();
            checkState(simStats.length == formulaVarNum, "simStats.length not equal to statTypes.length");
            simStatColumnCount = simStats[0].length;
            checkState(1 + (simStatColumnCount * 2) == columnCount, "bad RetentionLostSimStatFormulaItem");
        }
        int tableCellCount = rowCount * (columnCount + (formulaVarNum * simStatColumnCount));
        int estimatedSize = 24 + (quotaTypeLength * 8) + (quotaIndexLength * 8) + (formulaVarNum * 8) + (tableCellCount * 8) + formulaLength;
        DynamicSliceOutput output = new DynamicSliceOutput(estimatedSize);
        output.writeInt(rowCount);
        output.writeInt(simStatColumnCount);
        output.writeInt(formulaVarNum);
        output.writeInt(formulaLength);
        output.writeInt(quotaTypeLength);
        output.writeInt(quotaIndexLength);
        for (long statType : statTypes) {
            output.writeLong(statType);
        }
        for (long quotaType : quotaTypeArr) {
            output.writeLong(quotaType);
        }
        for (long quotaIndex : quotaIndexArr) {
            output.writeLong(quotaIndex);
        }
        for (int i = 0; i < rowCount; i++) {
            RetentionLostSimStatFormulaItem row = table[i];
            long[] retentionLost = row.getRetentionLost();
            checkState(retentionLost.length == columnCount, "the retentionLost array length is not equal to columnCount");
            for (long l : retentionLost) {
                output.writeLong(l);
            }
            double[][] simStats2 = row.getSimStats();
            checkState(simStats2.length == formulaVarNum, "simStats length not equal to formulaVarNum");
            for (double[] simStat : simStats2) {
                checkState(simStat.length == simStatColumnCount, "simStat length not equal to simStatColumnCount");
                for (double v : simStat) {
                    output.writeDouble(v);
                }
            }
        }
        output.writeBytes(formula);
        return output.slice();
    }

    static RetentionLostSimStatFormulaQuota deserialize(Slice slice)
    {
        requireNonNull(slice, "slice is null");
        int rowCount;
        long[] statTypes;
        long[] quotaTypeArr;
        long[] quotaIndexArr;
        RetentionLostSimStatFormulaItem[] table;
        Slice formula;
        try (BasicSliceInput input = new BasicSliceInput(slice)) {
            rowCount = input.readInt();
            int simStatColumnCount = input.readInt();
            int formulaVarNum = input.readInt();
            int formulaLength = input.readInt();
            int quotaTypeLength = input.readInt();
            int quotaIndexLength = input.readInt();
            int columnCount = simStatColumnCount * 2 + 1;
            int tableCellCount = rowCount * (columnCount + formulaVarNum * simStatColumnCount);
            int estimatedSize = 24 + (quotaTypeLength * 8) + (quotaIndexLength * 8) + formulaVarNum * 8 + tableCellCount * 8 + formulaLength;
            checkState(slice.length() == estimatedSize, "slice length not correct");
            statTypes = new long[formulaVarNum];

            for (int k = 0; k < formulaVarNum; ++k) {
                statTypes[k] = input.readLong();
            }
            quotaTypeArr = new long[quotaTypeLength];
            for (int i = 0; i < quotaTypeLength; i++) {
                quotaTypeArr[i] = input.readLong();
            }

            quotaIndexArr = new long[quotaIndexLength];
            for (int i = 0; i < quotaIndexLength; i++) {
                quotaIndexArr[i] = input.readLong();
            }

            table = new RetentionLostSimStatFormulaItem[rowCount];

            for (int i = 0; i < rowCount; ++i) {
                long[] retentionLost = new long[columnCount];

                for (int j = 0; j < columnCount; ++j) {
                    retentionLost[j] = input.readLong();
                }

                double[][] simStats = new double[formulaVarNum][];

                for (int k = 0; k < formulaVarNum; ++k) {
                    simStats[k] = new double[simStatColumnCount];

                    for (int j = 0; j < simStatColumnCount; ++j) {
                        simStats[k][j] = input.readDouble();
                    }
                }

                table[i] = new RetentionLostSimStatFormulaItem(retentionLost, simStats);
            }

            formula = input.readSlice(formulaLength);
        }
        return new RetentionLostSimStatFormulaQuota(rowCount, statTypes, table, formula, quotaTypeArr, quotaIndexArr);
    }

    public Type getSerializedType()
    {
        return VarbinaryType.VARBINARY;
    }

    public void serialize(RetentionLostSimStatFormulaQuotaState state, BlockBuilder out)
    {
        RetentionLostSimStatFormulaQuota retentionLostSimStatFormulaQuota = state.getData();
        if (retentionLostSimStatFormulaQuota == null || retentionLostSimStatFormulaQuota.getRowCount() == 0) {
            out.appendNull();
            return;
        }
        Slice slice = serialize(state.getData());
        VarbinaryType.VARBINARY.writeSlice(out, slice);
    }

    public void deserialize(Block block, int index, RetentionLostSimStatFormulaQuotaState state)
    {
        RetentionLostSimStatFormulaQuota retentionLostData = deserialize(VarbinaryType.VARBINARY.getSlice(block, index));
        state.setData(retentionLostData);
    }
}
