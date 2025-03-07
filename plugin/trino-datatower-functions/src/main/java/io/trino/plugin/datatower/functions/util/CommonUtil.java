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
package io.trino.plugin.datatower.functions.util;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * @author liulin
 */
public class CommonUtil
{
    private CommonUtil()
    {
    }

    public static double setScale(double v, int newScale)
    {
        if (Double.isNaN(v) || Double.isInfinite(v)) {
            return v;
        }
        return BigDecimal.valueOf(v).setScale(newScale, RoundingMode.HALF_UP).doubleValue();
    }
}
