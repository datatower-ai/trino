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

import io.trino.array.ObjectBigArray;
import io.trino.spi.function.GroupedAccumulatorState;

import static io.airlift.slice.SizeOf.instanceSize;

/**
 * @author liulin
 */
public abstract class GroupedGenericAccumulatorState<T>
        implements GenericAccumulatorState<T>, GroupedAccumulatorState
{
    private static final long INSTANCE_SIZE = instanceSize(GroupedGenericAccumulatorState.class);
    private final ObjectBigArray<T> datas = new ObjectBigArray<>();
    private int groupId;
    private long size;

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE + this.size + this.datas.sizeOf();
    }

    @Override
    public T getData()
    {
        return this.datas.get(this.groupId);
    }

    @Override
    public void setData(T data)
    {
        T previousValue = this.datas.get(this.groupId);
        this.size -= sizeOfNullable(previousValue);
        this.size += sizeOfNullable(data);
        this.datas.set(this.groupId, data);
    }

    @Override
    public final void setGroupId(int groupId)
    {
        this.groupId = groupId;
    }

    @Override
    public void ensureCapacity(int size)
    {
        this.datas.ensureCapacity(size);
    }
}
