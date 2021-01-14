/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests for the {@link AnyMatchingTaskExecutorMatchingStrategy}. */
public class AnyMatchingTaskExecutorMatchingStrategyTest extends TestLogger {
    private final InstanceID instanceIdOfLargeTaskExecutor = new InstanceID();
    private final InstanceID instanceIdOfSmallTaskExecutor = new InstanceID();
    private final ResourceProfile largeResourceProfile = ResourceProfile.fromResources(10.2, 42);
    private final ResourceProfile smallResourceProfile = ResourceProfile.fromResources(1, 1);
    private Map<InstanceID, TaskExecutorInfo> taskExecutors = null;
    private Set<InstanceID> candidates = null;

    @Before
    public void setup() {
        taskExecutors = new HashMap<>();
        candidates = new HashSet<>();
        taskExecutors.put(
                instanceIdOfSmallTaskExecutor,
                TestingTaskExecutorInfo.newBuilder()
                        .withAvailableResource(smallResourceProfile)
                        .build());
        taskExecutors.put(
                instanceIdOfLargeTaskExecutor,
                TestingTaskExecutorInfo.newBuilder()
                        .withAvailableResource(largeResourceProfile)
                        .build());
        candidates.add(instanceIdOfLargeTaskExecutor);
        candidates.add(instanceIdOfSmallTaskExecutor);
    }

    @Test
    public void testReturnFulfillingTaskExecutorForFulfillableRequest() {
        final Optional<InstanceID> optionalTaskExecutor =
                AnyMatchingTaskExecutorMatchingStrategy.INSTANCE.findMatchingTaskExecutor(
                        largeResourceProfile, taskExecutors);

        assertTrue(optionalTaskExecutor.isPresent());
        assertThat(optionalTaskExecutor.get(), is(instanceIdOfLargeTaskExecutor));
    }

    @Test
    public void testReturnEmptyResultForUnfulfillableRequest() {
        final Optional<InstanceID> optionalMatchingSlot =
                AnyMatchingTaskExecutorMatchingStrategy.INSTANCE.findMatchingTaskExecutor(
                        ResourceProfile.fromResources(Double.MAX_VALUE, Integer.MAX_VALUE),
                        taskExecutors);

        assertFalse(optionalMatchingSlot.isPresent());
    }
}
