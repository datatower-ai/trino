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

import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;

public enum QuotaType
{
    RETENTION_RATE("RETENTION_RATE", 1),
    RETENTION_NUM("RETENTION_NUM", 2),
    LOST_RATE("LOST_RATE", 3),
    LOST_NUM("LOST_NUM", 4),
    SIM_STAT("SIM_STAT", 5);

    private final String name;
    private final int code;

    QuotaType(String name, int code)
    {
        this.name = name;
        this.code = code;
    }

    public static QuotaType getByCode(int code)
    {
        return switch (code) {
            case 1 -> RETENTION_RATE;
            case 2 -> RETENTION_NUM;
            case 3 -> LOST_RATE;
            case 4 -> LOST_NUM;
            case 5 -> SIM_STAT;
            default -> throw new TrinoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "'" + code + "' is not a valid Quota Type Code");
        };
    }

    public static QuotaType getByName(String name)
    {
        return switch (name) {
            case "RETENTION_RATE" -> RETENTION_RATE;
            case "RETENTION_NUM" -> RETENTION_NUM;
            case "LOST_RATE" -> LOST_RATE;
            case "LOST_NUM" -> LOST_NUM;
            case "SIM_STAT" -> SIM_STAT;
            default -> throw new TrinoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "'" + name + "' is not a valid Quota Type Name");
        };
    }

    public String getName()
    {
        return name;
    }

    public int getCode()
    {
        return code;
    }
}
