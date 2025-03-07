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

import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.trino.cache.SafeCaches;
import io.trino.plugin.datatower.functions.util.ChronologyUtil;
import io.trino.plugin.datatower.functions.util.CommonUtil;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import org.openjdk.nashorn.api.scripting.NashornScriptEngine;

import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleBindings;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.instanceSize;

/**
 * @author liulin
 */
public class RetentionLostSimStatFormula
{
    private static final long INSTANCE_SIZE = instanceSize(RetentionLostSimStatFormula.class);
    private static final ScriptEngineManager SCRIPT_ENGINE_MANAGER = new ScriptEngineManager();
    private static final ScriptEngine SCRIPT_ENGINE = SCRIPT_ENGINE_MANAGER.getEngineByName("nashorn");

    private static final LoadingCache<String, CompiledScript> SCRIPT_LOADING_CACHE = SafeCaches.emptyLoadingCache(new CacheLoader<>()
    {
        @Override
        public CompiledScript load(String key)
                throws ScriptException
        {
            return ((Compilable) SCRIPT_ENGINE).compile(key);
        }
    }, true);
    private final int rowCount;
    private final long[] statTypes;
    private final RetentionLostSimStatFormulaItem[] table;
    private final Slice formula;

    RetentionLostSimStatFormula(int rowCount, long[] statTypes, RetentionLostSimStatFormulaItem[] table, Slice formula)
    {
        this.rowCount = rowCount;
        this.statTypes = statTypes;
        this.table = table;
        this.formula = formula;
    }

    static RetentionLostSimStatFormula merge(RetentionLostSimStatFormula left, RetentionLostSimStatFormula right)
    {
        int leftRowCount = left.rowCount;
        int rightRowCount = right.rowCount;
        if (rightRowCount == 0) {
            return left;
        }
        else if (leftRowCount == 0) {
            return right;
        }
        else {
            int rowCount = leftRowCount + rightRowCount;
            RetentionLostSimStatFormulaItem[] table = new RetentionLostSimStatFormulaItem[rowCount];
            RetentionLostSimStatFormulaItem[] leftTable = left.table;
            RetentionLostSimStatFormulaItem[] rightTable = right.table;
            int leftIndex = 0;
            int rightIndex = 0;
            int i = 0;

            while (true) {
                while (leftIndex < leftRowCount || rightIndex < rightRowCount) {
                    if (leftIndex == leftRowCount) {
                        table[i++] = rightTable[rightIndex++];
                    }
                    else if (rightIndex == rightRowCount) {
                        table[i++] = leftTable[leftIndex++];
                    }
                    else {
                        RetentionLostSimStatFormulaItem leftRow = leftTable[leftIndex];
                        RetentionLostSimStatFormulaItem rightRow = rightTable[rightIndex];
                        long leftDate = leftRow.getRetentionLost()[0];
                        long rightDate = rightRow.getRetentionLost()[0];
                        if (leftDate < rightDate) {
                            table[i++] = leftRow;
                            ++leftIndex;
                        }
                        else if (leftDate > rightDate) {
                            table[i++] = rightRow;
                            ++rightIndex;
                        }
                        else {
                            long[] leftRetentionLost = leftRow.getRetentionLost();
                            long[] rightRetentionLost = rightRow.getRetentionLost();
                            int length = leftRetentionLost.length;

                            for (int j = 1; j < length; ++j) {
                                leftRetentionLost[j] += rightRetentionLost[j];
                            }

                            double[][] leftSimStats = leftRow.getSimStats();
                            double[][] rightSimStats = rightRow.getSimStats();

                            for (int k = 0; k < leftSimStats.length; ++k) {
                                double[] leftSimStat = leftSimStats[k];
                                double[] rightSimStat = rightSimStats[k];
                                length = leftSimStat.length;

                                for (int j = 0; j < length; ++j) {
                                    leftSimStat[j] += rightSimStat[j];
                                }
                            }

                            table[i++] = leftRow;
                            ++leftIndex;
                            ++rightIndex;
                        }
                    }
                }

                return new RetentionLostSimStatFormula(i, left.getStatTypes(), table, left.getFormula());
            }
        }
    }

    int getRowCount()
    {
        return this.rowCount;
    }

    long[] getStatTypes()
    {
        return this.statTypes;
    }

    RetentionLostSimStatFormulaItem[] getTable()
    {
        return this.table;
    }

    public Slice getFormula()
    {
        return this.formula;
    }

    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        else if (o != null && this.getClass() == o.getClass()) {
            RetentionLostSimStatFormula that = (RetentionLostSimStatFormula) o;
            return this.rowCount == that.rowCount && Arrays.equals(this.statTypes, that.statTypes) && Arrays.equals(this.table, that.table) && this.formula.equals(that.formula);
        }
        else {
            return false;
        }
    }

    public int hashCode()
    {
        int result = Objects.hash(new Object[] {this.rowCount, this.formula});
        result = 31 * result + Arrays.hashCode(this.statTypes);
        result = 31 * result + Arrays.hashCode(this.table);
        return result;
    }

    int estimateMemorySize()
    {
        long rowSize = 0L;
        if (this.rowCount > 0) {
            rowSize = (long) this.table[0].estimateMemorySize();
        }

        return (int) ((long) INSTANCE_SIZE + SizeOf.sizeOf(this.statTypes) + SizeOf.sizeOf(this.table) + rowSize * (long) this.rowCount + (long) this.formula.length());
    }

    RetentionLostSimStatFormula compact()
    {
        if (this.rowCount < this.table.length) {
            RetentionLostSimStatFormulaItem[] compactTable = Arrays.copyOfRange(this.table, 0, this.rowCount);
            return new RetentionLostSimStatFormula(this.rowCount, this.statTypes, compactTable, this.formula);
        }
        else {
            return this;
        }
    }

    Map<Long, Object> toMap()
    {
        Map<Long, long[]> retentionMap = new LinkedHashMap<>();
        Map<Long, long[]> lostMap = new LinkedHashMap<>();
        Map<Long, double[][]> simStatsMap = new LinkedHashMap<>();
        Map<Long, double[]> simStatMap = new LinkedHashMap<>();
        int formulaVarNum = this.statTypes.length;
        int n = 0;

        for (int i = 0; i < this.rowCount; ++i) {
            RetentionLostSimStatFormulaItem item = this.table[i];
            double[][] simStatsRow = item.getSimStats();
            checkState(formulaVarNum == simStatsRow.length, "stat type array length not equal to sim stat row length");
            if (i == 0) {
                n = simStatsRow[0].length;
            }

            long[] row = item.getRetentionLost();
            long date = ChronologyUtil.defaultZone.convertLocalToUTC(row[0], false);
            retentionMap.put(date, Arrays.copyOfRange(row, 1, 1 + n));
            lostMap.put(date, Arrays.copyOfRange(row, 1 + n, row.length));
            simStatsMap.put(date, simStatsRow);
        }

        String formulaStr = this.formula.toStringUtf8();

        CompiledScript compiledScript;
        try {
            compiledScript = SCRIPT_LOADING_CACHE.get(formulaStr);
        }
        catch (ExecutionException e) {
            throw new TrinoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "'" + formulaStr + "' is not a valid formula expression");
        }

        for (Map.Entry<Long, double[][]> entry : simStatsMap.entrySet()) {
            Long date = entry.getKey();
            double[][] simStatsRow = entry.getValue();

            for (int k = 0; k < formulaVarNum; ++k) {
                long statType = this.statTypes[k];
                double[] vals = simStatsRow[k];
                if (statType == 2L) {
                    long[] nums = retentionMap.get(date);

                    for (int i = 1; i < vals.length; ++i) {
                        if (nums[i] == 0L) {
                            vals[i] = 0.0;
                        }
                        else {
                            vals[i] /= (double) nums[i];
                        }
                    }
                }
                else if (statType == 3L) {
                    double totalVal = 0.0;

                    for (int i = 1; i < vals.length; ++i) {
                        vals[i] += totalVal;
                        totalVal = vals[i];
                    }
                }
                else if (statType == 4L) {
                    long initNum = ((long[]) retentionMap.get(date))[0];
                    double totalVal = 0.0;

                    for (int i = 1; i < vals.length; ++i) {
                        totalVal += vals[i];
                        vals[i] = totalVal / (double) initNum;
                    }
                }
            }

            double[] simStatRow = new double[n];
            Map<String, Object> values = new HashMap<>(formulaVarNum);
            SimpleBindings bindings = new SimpleBindings(values);

            for (int i = 1; i < n; ++i) {
                for (int k = 0; k < formulaVarNum; ++k) {
                    values.put("a" + k, simStatsRow[k][i]);
                }

                try {
                    Double v0 = (Double) compiledScript.eval(bindings);
                    double v = v0.isInfinite() ? Double.NaN : v0;
                    simStatRow[i] = CommonUtil.setScale(v, 8);
                }
                catch (ScriptException var22) {
                    values.remove(NashornScriptEngine.NASHORN_GLOBAL);
                    throw new TrinoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "" + values + " is not a valid binding for formula '" + formulaStr + "'");
                }
            }

            simStatMap.put(date, simStatRow);
        }

        Map<Long, Object> resultMap = new HashMap<>();
        resultMap.put(0L, retentionMap);
        resultMap.put(1L, lostMap);
        resultMap.put(2L, simStatMap);
        return resultMap;
    }
}
