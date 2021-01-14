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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Queue;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/** Tests for the {@link FineGrainedTaskExecutorTracker}. */
public class FineGrainedTaskExecutorTrackerTest extends TestLogger {
    private static final TaskExecutorConnection TASK_EXECUTOR_CONNECTION =
            new TaskExecutorConnection(
                    ResourceID.generate(),
                    new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway());

    @Test
    public void testInitState() {
        final FineGrainedTaskExecutorTracker taskExecutorTracker =
                new FineGrainedTaskExecutorTracker();
        assertThat(taskExecutorTracker.getPendingTaskManager(), is(empty()));
        assertThat(taskExecutorTracker.getTaskExecutors(), is(empty()));
    }

    @Test
    public void testAddTaskExecutor() {
        final FineGrainedTaskExecutorTracker taskExecutorTracker =
                new FineGrainedTaskExecutorTracker();
        taskExecutorTracker.addTaskExecutor(
                TASK_EXECUTOR_CONNECTION,
                new SlotReport(),
                ResourceProfile.ANY,
                ResourceProfile.ANY);
        assertThat(taskExecutorTracker.getTaskExecutors().size(), is(1));
        assertThat(
                taskExecutorTracker.getTaskExecutors(),
                hasItem(equalTo(TASK_EXECUTOR_CONNECTION.getInstanceID())));
    }

    @Test
    public void testAddPendingTaskExecutor() {
        final FineGrainedTaskExecutorTracker taskExecutorTracker =
                new FineGrainedTaskExecutorTracker();
        taskExecutorTracker.addPendingTaskExecutor(ResourceProfile.ANY, ResourceProfile.ANY);
        assertThat(taskExecutorTracker.getPendingTaskManager().size(), is(1));
    }

    @Test
    public void testEmptyTaskExecutorCanFulfillPendingTaskExecutor() {
        final FineGrainedTaskExecutorTracker taskExecutorTracker =
                new FineGrainedTaskExecutorTracker();
        final ResourceProfile resourceProfile = ResourceProfile.fromResources(1, 10);
        final SlotReport slotReport =
                new SlotReport(
                        new SlotStatus(
                                new SlotID(TASK_EXECUTOR_CONNECTION.getResourceID(), 0),
                                ResourceProfile.ANY));
        taskExecutorTracker.addPendingTaskExecutor(resourceProfile, resourceProfile);
        taskExecutorTracker.addTaskExecutor(
                TASK_EXECUTOR_CONNECTION, slotReport, resourceProfile, resourceProfile);
        assertThat(taskExecutorTracker.getPendingTaskManager(), is(empty()));
        assertThat(taskExecutorTracker.getTaskExecutors().size(), is(1));
    }

    @Test
    public void testOccupiedTaskExecutorCannotFulfillPendingTaskExecutor() {
        final FineGrainedTaskExecutorTracker taskExecutorTracker =
                new FineGrainedTaskExecutorTracker();
        final ResourceProfile resourceProfile = ResourceProfile.fromResources(1, 10);
        final SlotReport slotReport =
                new SlotReport(
                        new SlotStatus(
                                new SlotID(TASK_EXECUTOR_CONNECTION.getResourceID(), 1),
                                resourceProfile,
                                new JobID(),
                                new AllocationID()));
        taskExecutorTracker.addPendingTaskExecutor(resourceProfile, resourceProfile);
        taskExecutorTracker.addTaskExecutor(
                TASK_EXECUTOR_CONNECTION, slotReport, resourceProfile, resourceProfile);
        assertThat(taskExecutorTracker.getPendingTaskManager().size(), is(1));
        assertThat(taskExecutorTracker.getTaskExecutors().size(), is(1));
    }

    @Test
    public void testMismatchTaskExecutorCannotFulfillPendingTaskExecutor() {
        final FineGrainedTaskExecutorTracker taskExecutorTracker =
                new FineGrainedTaskExecutorTracker();
        final ResourceProfile resourceProfile1 = ResourceProfile.fromResources(1, 10);
        final ResourceProfile resourceProfile2 = ResourceProfile.fromResources(1, 5);
        final SlotReport slotReport = new SlotReport();
        taskExecutorTracker.addPendingTaskExecutor(resourceProfile1, resourceProfile1);
        taskExecutorTracker.addTaskExecutor(
                TASK_EXECUTOR_CONNECTION, slotReport, resourceProfile2, resourceProfile2);
        assertThat(taskExecutorTracker.getPendingTaskManager().size(), is(1));
        assertThat(taskExecutorTracker.getTaskExecutors().size(), is(1));
    }

    @Test
    public void testTaskExecutorRemoval() {
        final Queue<Tuple3<AllocationID, SlotState, SlotState>> stateTransitions =
                new ArrayDeque<>();
        final FineGrainedTaskExecutorTracker taskExecutorTracker =
                new FineGrainedTaskExecutorTracker();
        taskExecutorTracker.registerSlotStatusUpdateListener(
                (slot, previous, current, jobId) ->
                        stateTransitions.add(Tuple3.of(slot.getAllocationId(), previous, current)));

        final JobID jobId = new JobID();
        final AllocationID allocationId1 = new AllocationID();
        final AllocationID allocationId2 = new AllocationID();
        final SlotID slotId1 = new SlotID(TASK_EXECUTOR_CONNECTION.getResourceID(), 0);
        final SlotID slotId2 = new SlotID(TASK_EXECUTOR_CONNECTION.getResourceID(), 1);
        final SlotID slotId3 = new SlotID(TASK_EXECUTOR_CONNECTION.getResourceID(), 2);
        final ResourceProfile totalResource = ResourceProfile.fromResources(5, 20);
        final ResourceProfile resource = ResourceProfile.fromResources(1, 4);
        final SlotReport slotReport =
                new SlotReport(
                        Arrays.asList(
                                new SlotStatus(slotId1, totalResource),
                                new SlotStatus(slotId2, resource, jobId, allocationId1),
                                new SlotStatus(slotId3, resource, jobId, allocationId2)));

        taskExecutorTracker.addTaskExecutor(
                TASK_EXECUTOR_CONNECTION, slotReport, totalResource, totalResource);
        taskExecutorTracker.removeTaskExecutor(TASK_EXECUTOR_CONNECTION.getInstanceID());

        assertThat(taskExecutorTracker.getTaskExecutors(), is(empty()));
        assertThat(stateTransitions.size(), is(4));
        assertThat(
                stateTransitions,
                containsInAnyOrder(
                        Tuple3.of(allocationId1, SlotState.ALLOCATED, SlotState.FREE),
                        Tuple3.of(allocationId2, SlotState.ALLOCATED, SlotState.FREE),
                        Tuple3.of(allocationId1, SlotState.FREE, SlotState.ALLOCATED),
                        Tuple3.of(allocationId2, SlotState.FREE, SlotState.ALLOCATED)));
    }

    @Test
    public void testAllocationStartAndCompletion() {
        final Queue<Tuple3<AllocationID, SlotState, SlotState>> stateTransitions =
                new ArrayDeque<>();
        final FineGrainedTaskExecutorTracker taskExecutorTracker =
                new FineGrainedTaskExecutorTracker();
        taskExecutorTracker.registerSlotStatusUpdateListener(
                (slot, previous, current, jobId) ->
                        stateTransitions.add(Tuple3.of(slot.getAllocationId(), previous, current)));
        final JobID jobId = new JobID();
        final AllocationID allocationId = new AllocationID();
        final ResourceProfile totalResource = ResourceProfile.fromResources(5, 20);
        final ResourceProfile resource = ResourceProfile.fromResources(1, 4);
        final SlotReport slotReport =
                new SlotReport(
                        new SlotStatus(
                                new SlotID(TASK_EXECUTOR_CONNECTION.getResourceID(), 0),
                                totalResource));

        taskExecutorTracker.addTaskExecutor(
                TASK_EXECUTOR_CONNECTION, slotReport, totalResource, totalResource);
        taskExecutorTracker.notifyAllocationStart(
                allocationId, jobId, TASK_EXECUTOR_CONNECTION.getInstanceID(), resource);
        assertThat(
                stateTransitions.remove(),
                is(Tuple3.of(allocationId, SlotState.FREE, SlotState.PENDING)));
        assertNotNull(taskExecutorTracker.getSlot(allocationId));
        assertThat(taskExecutorTracker.getSlot(allocationId).getState(), is(SlotState.PENDING));
        assertThat(taskExecutorTracker.getSlot(allocationId).getJobId(), is(jobId));
        assertThat(
                taskExecutorTracker.getSlot(allocationId).getResourceProfile(), equalTo(resource));

        taskExecutorTracker.notifyAllocationComplete(
                TASK_EXECUTOR_CONNECTION.getInstanceID(), allocationId);
        assertThat(
                stateTransitions.remove(),
                is(Tuple3.of(allocationId, SlotState.PENDING, SlotState.ALLOCATED)));
        assertNotNull(taskExecutorTracker.getSlot(allocationId));
        assertThat(taskExecutorTracker.getSlot(allocationId).getState(), is(SlotState.ALLOCATED));
        assertThat(taskExecutorTracker.getSlot(allocationId).getJobId(), is(jobId));
        assertThat(
                taskExecutorTracker.getSlot(allocationId).getResourceProfile(), equalTo(resource));
        assertThat(
                taskExecutorTracker.getTotalFreeResourcesOf(
                        TASK_EXECUTOR_CONNECTION.getInstanceID()),
                equalTo(totalResource.subtract(resource)));
    }

    @Test
    public void testFreeAllocatedSlot() {
        final Queue<Tuple3<AllocationID, SlotState, SlotState>> stateTransitions =
                new ArrayDeque<>();
        final FineGrainedTaskExecutorTracker taskExecutorTracker =
                new FineGrainedTaskExecutorTracker();
        taskExecutorTracker.registerSlotStatusUpdateListener(
                (slot, previous, current, jobId) ->
                        stateTransitions.add(Tuple3.of(slot.getAllocationId(), previous, current)));
        final JobID jobId = new JobID();
        final AllocationID allocationId = new AllocationID();
        final ResourceProfile resource = ResourceProfile.fromResources(1, 4);
        final SlotReport slotReport =
                new SlotReport(
                        new SlotStatus(
                                new SlotID(TASK_EXECUTOR_CONNECTION.getResourceID(), 0),
                                resource,
                                jobId,
                                allocationId));

        taskExecutorTracker.addTaskExecutor(
                TASK_EXECUTOR_CONNECTION, slotReport, resource, resource);
        assertThat(
                stateTransitions.remove(),
                is(Tuple3.of(allocationId, SlotState.FREE, SlotState.ALLOCATED)));
        assertNotNull(taskExecutorTracker.getSlot(allocationId));
        assertThat(taskExecutorTracker.getSlot(allocationId).getState(), is(SlotState.ALLOCATED));
        assertThat(taskExecutorTracker.getSlot(allocationId).getJobId(), is(jobId));
        assertThat(
                taskExecutorTracker.getSlot(allocationId).getResourceProfile(), equalTo(resource));
        assertThat(
                taskExecutorTracker.getTotalFreeResourcesOf(
                        TASK_EXECUTOR_CONNECTION.getInstanceID()),
                equalTo(ResourceProfile.ZERO));

        taskExecutorTracker.notifyFree(allocationId);
        assertThat(
                stateTransitions.remove(),
                is(Tuple3.of(allocationId, SlotState.ALLOCATED, SlotState.FREE)));
        assertNull(taskExecutorTracker.getSlot(allocationId));
        assertThat(
                taskExecutorTracker.getTotalFreeResourcesOf(
                        TASK_EXECUTOR_CONNECTION.getInstanceID()),
                equalTo(resource));
    }

    @Test
    public void testFreePendingSlot() {
        final Queue<Tuple3<AllocationID, SlotState, SlotState>> stateTransitions =
                new ArrayDeque<>();
        final FineGrainedTaskExecutorTracker taskExecutorTracker =
                new FineGrainedTaskExecutorTracker();
        taskExecutorTracker.registerSlotStatusUpdateListener(
                (slot, previous, current, jobId) ->
                        stateTransitions.add(Tuple3.of(slot.getAllocationId(), previous, current)));
        final JobID jobId = new JobID();
        final AllocationID allocationId = new AllocationID();
        final ResourceProfile totalResource = ResourceProfile.fromResources(5, 20);
        final ResourceProfile resource = ResourceProfile.fromResources(1, 4);
        final SlotReport slotReport = new SlotReport();

        taskExecutorTracker.addTaskExecutor(
                TASK_EXECUTOR_CONNECTION, slotReport, totalResource, totalResource);
        taskExecutorTracker.notifyAllocationStart(
                allocationId, jobId, TASK_EXECUTOR_CONNECTION.getInstanceID(), resource);
        assertThat(
                stateTransitions.remove(),
                is(Tuple3.of(allocationId, SlotState.FREE, SlotState.PENDING)));
        assertNotNull(taskExecutorTracker.getSlot(allocationId));

        taskExecutorTracker.notifyFree(allocationId);
        assertThat(
                stateTransitions.remove(),
                is(Tuple3.of(allocationId, SlotState.PENDING, SlotState.FREE)));
        assertNull(taskExecutorTracker.getSlot(allocationId));
        assertThat(
                taskExecutorTracker.getTotalFreeResourcesOf(
                        TASK_EXECUTOR_CONNECTION.getInstanceID()),
                equalTo(totalResource));
    }

    @Test
    public void testSlotStatusProcessing() {
        final FineGrainedTaskExecutorTracker taskExecutorTracker =
                new FineGrainedTaskExecutorTracker();

        final JobID jobId = new JobID();
        final AllocationID allocationId1 = new AllocationID();
        final AllocationID allocationId2 = new AllocationID();
        final AllocationID allocationId3 = new AllocationID();
        final SlotID slotId1 = new SlotID(TASK_EXECUTOR_CONNECTION.getResourceID(), 0);
        final SlotID slotId2 = new SlotID(TASK_EXECUTOR_CONNECTION.getResourceID(), 1);
        final SlotID slotId3 = new SlotID(TASK_EXECUTOR_CONNECTION.getResourceID(), 2);
        final ResourceProfile totalResource = ResourceProfile.fromResources(5, 20);
        final ResourceProfile resource = ResourceProfile.fromResources(1, 4);
        final SlotReport slotReport1 =
                new SlotReport(
                        Arrays.asList(
                                new SlotStatus(slotId1, totalResource),
                                new SlotStatus(slotId2, resource, jobId, allocationId1),
                                new SlotStatus(slotId3, resource, jobId, allocationId2)));
        final SlotReport slotReport2 =
                new SlotReport(
                        Arrays.asList(
                                new SlotStatus(slotId3, resource),
                                new SlotStatus(slotId2, resource, jobId, allocationId1)));

        taskExecutorTracker.addTaskExecutor(
                TASK_EXECUTOR_CONNECTION, slotReport1, totalResource, totalResource);
        assertThat(
                taskExecutorTracker.getTotalFreeResourcesOf(
                        TASK_EXECUTOR_CONNECTION.getInstanceID()),
                equalTo(ResourceProfile.fromResources(3, 12)));
        assertNotNull(taskExecutorTracker.getSlot(allocationId1));
        assertNotNull(taskExecutorTracker.getSlot(allocationId2));

        taskExecutorTracker.notifyAllocationStart(
                allocationId3, jobId, TASK_EXECUTOR_CONNECTION.getInstanceID(), resource);
        assertThat(
                taskExecutorTracker.getTotalFreeResourcesOf(
                        TASK_EXECUTOR_CONNECTION.getInstanceID()),
                equalTo(ResourceProfile.fromResources(2, 8)));
        assertNotNull(taskExecutorTracker.getSlot(allocationId3));

        // allocationId1 should still be allocated; allocationId3 should continue to be in a pending
        // state; allocationId2 should be freed
        taskExecutorTracker.notifySlotStatus(slotReport2, TASK_EXECUTOR_CONNECTION.getInstanceID());
        assertThat(
                taskExecutorTracker.getTotalFreeResourcesOf(
                        TASK_EXECUTOR_CONNECTION.getInstanceID()),
                equalTo(ResourceProfile.fromResources(3, 12)));
        assertNotNull(taskExecutorTracker.getSlot(allocationId1));
        assertNull(taskExecutorTracker.getSlot(allocationId2));
        assertNotNull(taskExecutorTracker.getSlot(allocationId3));
        assertThat(taskExecutorTracker.getSlot(allocationId1).getState(), is(SlotState.ALLOCATED));
        assertThat(taskExecutorTracker.getSlot(allocationId3).getState(), is(SlotState.PENDING));
    }

    @Test
    public void testCanFindTaskExecutorToFulfillWithEnoughResource() {
        final FineGrainedTaskExecutorTracker taskExecutorTracker =
                new FineGrainedTaskExecutorTracker();
        final ResourceProfile totalResource = ResourceProfile.fromResources(5, 20);
        final ResourceProfile resource = ResourceProfile.fromResources(1, 4);
        taskExecutorTracker.addTaskExecutor(
                TASK_EXECUTOR_CONNECTION, new SlotReport(), totalResource, totalResource);

        assertTrue(
                taskExecutorTracker
                        .findTaskExecutorToFulfill(
                                resource, AnyMatchingTaskExecutorMatchingStrategy.INSTANCE)
                        .isPresent());
        assertThat(
                taskExecutorTracker
                        .findTaskExecutorToFulfill(
                                resource, AnyMatchingTaskExecutorMatchingStrategy.INSTANCE)
                        .get(),
                is(TASK_EXECUTOR_CONNECTION));
    }

    @Test
    public void testCannotFindTaskExecutorToFulfillWithoutEnoughResource() {
        final FineGrainedTaskExecutorTracker taskExecutorTracker =
                new FineGrainedTaskExecutorTracker();
        final ResourceProfile totalResource = ResourceProfile.fromResources(5, 20);
        final ResourceProfile pendingResource = ResourceProfile.fromResources(1, 4);
        final ResourceProfile allocatedResource = ResourceProfile.fromResources(4, 16);
        final JobID jobId = new JobID();
        final AllocationID allocationId1 = new AllocationID();
        final AllocationID allocationId2 = new AllocationID();
        final SlotID slotId = new SlotID(TASK_EXECUTOR_CONNECTION.getResourceID(), 1);
        final SlotReport slotReport =
                new SlotReport(new SlotStatus(slotId, allocatedResource, jobId, allocationId1));

        taskExecutorTracker.addTaskExecutor(
                TASK_EXECUTOR_CONNECTION, slotReport, totalResource, totalResource);
        assertFalse(
                taskExecutorTracker
                        .findTaskExecutorToFulfill(
                                totalResource, AnyMatchingTaskExecutorMatchingStrategy.INSTANCE)
                        .isPresent());
        assertTrue(
                taskExecutorTracker
                        .findTaskExecutorToFulfill(
                                pendingResource, AnyMatchingTaskExecutorMatchingStrategy.INSTANCE)
                        .isPresent());

        // Add pending slot
        taskExecutorTracker.notifyAllocationStart(
                allocationId2, jobId, TASK_EXECUTOR_CONNECTION.getInstanceID(), pendingResource);
        assertFalse(
                taskExecutorTracker
                        .findTaskExecutorToFulfill(
                                pendingResource, AnyMatchingTaskExecutorMatchingStrategy.INSTANCE)
                        .isPresent());
    }

    @Test
    public void testGetTimeOutTaskExecutors() throws Exception {
        final FineGrainedTaskExecutorTracker taskExecutorTracker =
                new FineGrainedTaskExecutorTracker();
        final ResourceProfile resourceProfile = ResourceProfile.fromResources(1, 10);
        final JobID jobId = new JobID();
        final AllocationID allocationId = new AllocationID();
        final SlotID slotId = new SlotID(TASK_EXECUTOR_CONNECTION.getResourceID(), 1);
        final SlotReport slotReport =
                new SlotReport(new SlotStatus(slotId, resourceProfile, jobId, allocationId));
        final long timeoutMs = 25;
        taskExecutorTracker.addTaskExecutor(
                TASK_EXECUTOR_CONNECTION, slotReport, resourceProfile, resourceProfile);

        Thread.sleep(timeoutMs * 2);
        assertTrue(
                taskExecutorTracker
                        .getTimeOutTaskExecutors(Time.milliseconds(timeoutMs))
                        .isEmpty());

        taskExecutorTracker.notifyFree(allocationId);
        Thread.sleep(timeoutMs * 2);
        assertThat(
                taskExecutorTracker.getTimeOutTaskExecutors(Time.milliseconds(timeoutMs)).keySet(),
                contains(TASK_EXECUTOR_CONNECTION.getInstanceID()));
    }
}
