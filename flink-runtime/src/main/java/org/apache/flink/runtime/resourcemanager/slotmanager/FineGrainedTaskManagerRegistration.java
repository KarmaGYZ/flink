/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.util.Preconditions;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * A FineGrainedTaskManagerRegistration represents a TaskExecutor. It records the internal state of
 * the TaskExecutor, including allocated/pending slots, total/available resources.
 *
 * <p>This class is the fine-grained resource management version of the {@link
 * TaskManagerRegistration}.
 */
public class FineGrainedTaskManagerRegistration implements TaskExecutorInfo {
    private final TaskExecutorConnection taskManagerConnection;

    private final Map<AllocationID, FineGrainedTaskManagerSlot> slots;

    private final ResourceProfile defaultSlotResourceProfile;

    private final ResourceProfile totalResource;

    private final int defaultNumSlots;

    private ResourceProfile unusedResource;

    private ResourceProfile pendingResource = ResourceProfile.ZERO;

    /** Timestamp when the last time becoming idle. Otherwise Long.MAX_VALUE. */
    private long idleSince;

    public FineGrainedTaskManagerRegistration(
            TaskExecutorConnection taskManagerConnection,
            ResourceProfile totalResourceProfile,
            ResourceProfile defaultSlotResourceProfile) {
        this.taskManagerConnection = Preconditions.checkNotNull(taskManagerConnection);
        this.totalResource = Preconditions.checkNotNull(totalResourceProfile);
        this.defaultSlotResourceProfile = Preconditions.checkNotNull(defaultSlotResourceProfile);

        this.slots = new HashMap<>(16);

        this.defaultNumSlots =
                totalResourceProfile
                        .getCpuCores()
                        .getValue()
                        .divide(
                                defaultSlotResourceProfile.getCpuCores().getValue(),
                                0,
                                BigDecimal.ROUND_DOWN)
                        .intValue();

        this.unusedResource =
                ResourceProfile.newBuilder()
                        .setCpuCores(totalResourceProfile.getCpuCores())
                        .setTaskHeapMemory(totalResourceProfile.getTaskHeapMemory())
                        .setTaskOffHeapMemory(totalResourceProfile.getTaskOffHeapMemory())
                        .setNetworkMemory(totalResourceProfile.getNetworkMemory())
                        .setManagedMemory(totalResourceProfile.getManagedMemory())
                        .addExtendedResources(totalResourceProfile.getExtendedResources())
                        .build();

        idleSince = System.currentTimeMillis();
    }

    public TaskExecutorConnection getTaskManagerConnection() {
        return taskManagerConnection;
    }

    public InstanceID getInstanceId() {
        return taskManagerConnection.getInstanceID();
    }

    public Map<AllocationID, FineGrainedTaskManagerSlot> getSlots() {
        return Collections.unmodifiableMap(slots);
    }

    @Override
    public int getNumberAllocatedSlots() {
        return (int)
                slots.values().stream()
                        .filter(slot -> slot.getState() == SlotState.ALLOCATED)
                        .count();
    }

    @Override
    public ResourceProfile getAvailableResource() {
        if (!unusedResource.allFieldsNoLessThan(pendingResource)) {
            return ResourceProfile.ZERO;
        }
        return unusedResource.subtract(pendingResource);
    }

    @Override
    public ResourceProfile getDefaultSlotResourceProfile() {
        return defaultSlotResourceProfile;
    }

    @Override
    public ResourceProfile getTotalResource() {
        return totalResource;
    }

    public void freeSlot(AllocationID allocationId) {
        Preconditions.checkNotNull(allocationId);
        FineGrainedTaskManagerSlot taskManagerSlot =
                Preconditions.checkNotNull(slots.remove(allocationId));

        if (taskManagerSlot.getState() == SlotState.PENDING) {
            pendingResource = pendingResource.subtract(taskManagerSlot.getResourceProfile());
        } else {
            unusedResource = unusedResource.merge(taskManagerSlot.getResourceProfile());
        }

        if (slots.isEmpty()) {
            idleSince = System.currentTimeMillis();
        }
    }

    public void notifyAllocationComplete(AllocationID allocationId) {
        FineGrainedTaskManagerSlot slot = Preconditions.checkNotNull(slots.get(allocationId));
        slot.completeAllocation();
        pendingResource = pendingResource.subtract(slot.getResourceProfile());
        unusedResource = unusedResource.subtract(slot.getResourceProfile());
    }

    public void notifyAllocationStart(
            AllocationID allocationId, FineGrainedTaskManagerSlot taskManagerSlot) {
        Preconditions.checkNotNull(allocationId);
        Preconditions.checkNotNull(taskManagerSlot);
        slots.put(allocationId, taskManagerSlot);
        pendingResource = pendingResource.merge(taskManagerSlot.getResourceProfile());
        idleSince = Long.MAX_VALUE;
    }

    public void occupySlot(FineGrainedTaskManagerSlot taskManagerSlot, AllocationID allocationID) {
        Preconditions.checkNotNull(allocationID);
        Preconditions.checkNotNull(taskManagerSlot);

        unusedResource = unusedResource.subtract(taskManagerSlot.getResourceProfile());
        slots.put(allocationID, taskManagerSlot);

        idleSince = Long.MAX_VALUE;
    }

    public long getIdleSince() {
        return idleSince;
    }

    public boolean isIdle() {
        return idleSince != Long.MAX_VALUE;
    }

    public int getDefaultNumSlots() {
        return defaultNumSlots;
    }
}
