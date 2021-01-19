/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.slots.ResourceCounter;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Collections;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/** Tests for the {@link DefaultTaskExecutorAllocationStrategy}. */
public class DefaultTaskExecutorAllocationStrategyTest extends TestLogger {
    private static final ResourceProfile DEFAULT_SLOT = ResourceProfile.fromResources(1, 100);
    private static final int NUM_SLOT = 2;
    private static final PendingTaskManager DEFAULT_TM =
            new PendingTaskManager(DEFAULT_SLOT.multiply(NUM_SLOT), DEFAULT_SLOT);
    private static final JobID JOB_ID = new JobID();
    private static final DefaultTaskExecutorAllocationStrategy STRATEGY =
            new DefaultTaskExecutorAllocationStrategy(DEFAULT_SLOT, NUM_SLOT);

    @Test
    public void testGetTaskExecutorsToFulfillWithUnfulfillableRequirement() {
        ResourceCounter unfulfilledRequirement =
                new ResourceCounter(
                        Collections.singletonMap(
                                ResourceProfile.fromResources(Double.MAX_VALUE, Integer.MAX_VALUE),
                                1));
        assertTrue(
                STRATEGY.getTaskExecutorsToFulfill(
                                Collections.singletonMap(JOB_ID, unfulfilledRequirement),
                                Collections.emptyMap())
                        .get(JOB_ID)
                        .f1);
    }

    @Test(expected = IllegalStateException.class)
    public void testGetTaskExecutorsToFulfillWithIllegalExistingPendingResources() {
        ResourceCounter requirement =
                new ResourceCounter(Collections.singletonMap(ResourceProfile.UNKNOWN, 1));
        STRATEGY.getTaskExecutorsToFulfill(
                Collections.singletonMap(JOB_ID, requirement),
                Collections.singletonMap(
                        new PendingTaskManager(
                                ResourceProfile.fromResources(1, 100),
                                ResourceProfile.fromResources(1, 100)),
                        1));
    }

    @Test
    public void testGetTaskExecutorsToFulfillWithEnoughExistingPendingResources() {
        ResourceCounter requirements = new ResourceCounter();
        requirements.incrementCount(ResourceProfile.UNKNOWN, 1);
        requirements.incrementCount(DEFAULT_SLOT, 3);
        assertFalse(
                STRATEGY.getTaskExecutorsToFulfill(
                                Collections.singletonMap(JOB_ID, requirements),
                                Collections.singletonMap(DEFAULT_TM, 2))
                        .get(JOB_ID)
                        .f1);
        assertTrue(
                STRATEGY.getTaskExecutorsToFulfill(
                                Collections.singletonMap(JOB_ID, requirements),
                                Collections.singletonMap(DEFAULT_TM, 2))
                        .get(JOB_ID)
                        .f0
                        .isEmpty());
    }

    @Test
    public void testGetTaskExecutorsToFulfillWithoutEnoughExistingPendingResources() {
        ResourceCounter requirements = new ResourceCounter();
        requirements.incrementCount(ResourceProfile.UNKNOWN, 2);
        requirements.incrementCount(DEFAULT_SLOT, 3);
        assertFalse(
                STRATEGY.getTaskExecutorsToFulfill(
                                Collections.singletonMap(JOB_ID, requirements),
                                Collections.singletonMap(DEFAULT_TM, 2))
                        .get(JOB_ID)
                        .f1);
        assertFalse(
                STRATEGY.getTaskExecutorsToFulfill(
                                Collections.singletonMap(JOB_ID, requirements),
                                Collections.singletonMap(DEFAULT_TM, 2))
                        .get(JOB_ID)
                        .f0
                        .isEmpty());
        assertThat(
                STRATEGY.getTaskExecutorsToFulfill(
                                Collections.singletonMap(JOB_ID, requirements),
                                Collections.singletonMap(DEFAULT_TM, 2))
                        .get(JOB_ID)
                        .f0
                        .get(DEFAULT_TM),
                is(1));
    }
}
