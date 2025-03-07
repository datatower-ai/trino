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
package io.trino.plugin.datatower.functions.aggregation.retention.simstat;

import io.trino.plugin.datatower.functions.state.GroupedGenericAccumulatorState;
import io.trino.plugin.datatower.functions.state.SimpleGenericAccumulatorState;
import io.trino.spi.function.AccumulatorStateFactory;

/**
 * @author liulin
 */
public class RetentionLostSimStatStateFactory
        implements AccumulatorStateFactory<RetentionLostSimStatState>
{
    public RetentionLostSimStatState createSingleState()
    {
        return new SingleState();
    }

    public Class<? extends RetentionLostSimStatState> getSingleStateClass()
    {
        return SingleState.class;
    }

    public RetentionLostSimStatState createGroupedState()
    {
        return new GroupedState();
    }

    public Class<? extends RetentionLostSimStatState> getGroupedStateClass()
    {
        return GroupedState.class;
    }

    public static class SingleState
            extends SimpleGenericAccumulatorState<RetentionLostSimStat>
            implements RetentionLostSimStatState
    {
        public SingleState()
        {
        }

        public long sizeOfNonNull(RetentionLostSimStat data)
        {
            return data.estimateMemorySize();
        }
    }

    public static class GroupedState
            extends GroupedGenericAccumulatorState<RetentionLostSimStat>
            implements RetentionLostSimStatState
    {
        public GroupedState()
        {
        }

        public long sizeOfNonNull(RetentionLostSimStat data)
        {
            return data.estimateMemorySize();
        }
    }
}
