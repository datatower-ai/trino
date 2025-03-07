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
package io.trino.plugin.datatower.functions.state;

import io.trino.spi.function.AccumulatorState;

/**
 * @author liulin
 */
public interface GenericAccumulatorState<T>
        extends AccumulatorState
{
    T getData();

    void setData(T t);

    long sizeOfNonNull(T t);

    default long sizeOfNullable(T data)
    {
        if (data == null) {
            return 0L;
        }
        return sizeOfNonNull(data);
    }
}
