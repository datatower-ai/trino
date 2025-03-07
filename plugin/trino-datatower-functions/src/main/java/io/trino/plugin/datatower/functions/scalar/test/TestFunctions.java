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
package io.trino.plugin.datatower.functions.scalar.test;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

/**
 * TODO
 *
 * @author LiuLin
 * @date 2022/9/2 18:45
 **/
public class TestFunctions
{
    private TestFunctions()
    {
    }

    @SqlType(StandardTypes.VARCHAR)
    @ScalarFunction("dt_udf_test_timestamp6")
    @Description("Returns the number string of timestamp value")
    public static Slice testTimestamp6(@SqlType("timestamp(6)") long ts)
    {
        String tsStr = String.valueOf(ts);
        return Slices.utf8Slice(tsStr);
    }
}
