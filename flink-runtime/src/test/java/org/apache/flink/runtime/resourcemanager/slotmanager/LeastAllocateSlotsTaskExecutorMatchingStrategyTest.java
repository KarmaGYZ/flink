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

import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertTrue;

/** Tests for the {@link LeastAllocateSlotsTaskExecutorMatchingStrategy}. */
public class LeastAllocateSlotsTaskExecutorMatchingStrategyTest extends TestLogger {
    @Test
    public void testReturnTaskExecutorWithLeastAllocatedSlots() {
        final ResourceProfile requestedResourceProfile = ResourceProfile.fromResources(2.0, 2);
        final Map<InstanceID, TaskExecutorInfo> taskExecutors = new HashMap<>();
        final InstanceID instanceId1 = new InstanceID();
        final InstanceID instanceId2 = new InstanceID();
        final InstanceID instanceId3 = new InstanceID();
        final Set<InstanceID> candidates = new HashSet<>();
        candidates.add(instanceId1);
        candidates.add(instanceId2);
        candidates.add(instanceId3);

        taskExecutors.put(
                instanceId1,
                TestingTaskExecutorInfo.newBuilder()
                        .withAvailableResource(ResourceProfile.fromResources(3.0, 5))
                        .withNumberAllocatedSlots(1)
                        .build());
        taskExecutors.put(
                instanceId2,
                TestingTaskExecutorInfo.newBuilder()
                        .withAvailableResource(ResourceProfile.fromResources(1.0, 10))
                        .withNumberAllocatedSlots(1)
                        .build());
        taskExecutors.put(
                instanceId3,
                TestingTaskExecutorInfo.newBuilder()
                        .withAvailableResource(ResourceProfile.fromResources(3.0, 5))
                        .withNumberAllocatedSlots(2)
                        .build());

        final Optional<InstanceID> matchingTaskExecutor =
                LeastAllocateSlotsTaskExecutorMatchingStrategy.INSTANCE.findMatchingTaskExecutor(
                        requestedResourceProfile, taskExecutors);

        assertTrue(matchingTaskExecutor.isPresent());
        assertThat(matchingTaskExecutor.get(), is(instanceId1));
    }
}
