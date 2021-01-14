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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Implementation of {@link TaskExecutorTracker} supporting fine-grained resource management. */
public class FineGrainedTaskExecutorTracker implements TaskExecutorTracker {
    private static final Logger LOG = LoggerFactory.getLogger(FineGrainedTaskExecutorTracker.class);

    /** Map for allocated and pending slots. */
    private final Map<AllocationID, FineGrainedTaskManagerSlot> slots;

    /** All currently registered task managers. */
    private final Map<InstanceID, FineGrainedTaskManagerRegistration> taskManagerRegistrations;

    private final List<PendingTaskManager> pendingTaskManagers;

    private final SlotStatusUpdateListener.MultiSlotStatusUpdateListener slotStatusUpdateListeners =
            new SlotStatusUpdateListener.MultiSlotStatusUpdateListener();

    public FineGrainedTaskExecutorTracker() {
        slots = new HashMap<>(16);
        taskManagerRegistrations = new HashMap<>(4);
        pendingTaskManagers = new ArrayList<>(16);
    }

    @Override
    public void registerSlotStatusUpdateListener(
            SlotStatusUpdateListener slotStatusUpdateListener) {
        Preconditions.checkNotNull(slotStatusUpdateListener);
        this.slotStatusUpdateListeners.registerSlotStatusUpdateListener(slotStatusUpdateListener);
    }

    @Override
    public void addTaskExecutor(
            TaskExecutorConnection taskExecutorConnection,
            SlotReport initialSlotReport,
            ResourceProfile totalResourceProfile,
            ResourceProfile defaultSlotResourceProfile) {
        Preconditions.checkNotNull(taskExecutorConnection);
        Preconditions.checkNotNull(initialSlotReport);
        Preconditions.checkNotNull(totalResourceProfile);
        Preconditions.checkNotNull(defaultSlotResourceProfile);

        FineGrainedTaskManagerRegistration taskManagerRegistration =
                new FineGrainedTaskManagerRegistration(
                        taskExecutorConnection, totalResourceProfile, defaultSlotResourceProfile);

        taskManagerRegistrations.put(
                taskExecutorConnection.getInstanceID(), taskManagerRegistration);

        findMatchingPendingTaskManager(
                        initialSlotReport, totalResourceProfile, defaultSlotResourceProfile)
                .ifPresent(pendingTaskManagers::remove);

        notifySlotStatus(initialSlotReport, taskExecutorConnection.getInstanceID());
    }

    @Override
    public Optional<PendingTaskManager> findMatchingPendingTaskManager(
            SlotReport initialSlotReport,
            ResourceProfile totalResourceProfile,
            ResourceProfile defaultResourceProfile) {
        if (initialSlotReport.withAllocatedSlot()) {
            return Optional.empty();
        }
        for (PendingTaskManager pendingTaskManager : pendingTaskManagers) {
            if (pendingTaskManager.getTotalResourceProfile().equals(totalResourceProfile)
                    && pendingTaskManager
                            .getDefaultSlotResourceProfile()
                            .equals(defaultResourceProfile)) {
                return Optional.of(pendingTaskManager);
            }
        }
        return Optional.empty();
    }

    @Override
    public void addPendingTaskExecutor(
            ResourceProfile totalResourceProfile, ResourceProfile defaultSlotResourceProfile) {
        Preconditions.checkNotNull(totalResourceProfile);
        Preconditions.checkNotNull(defaultSlotResourceProfile);
        PendingTaskManager pendingTaskManager =
                new PendingTaskManager(totalResourceProfile, defaultSlotResourceProfile);
        pendingTaskManagers.add(pendingTaskManager);
    }

    @Override
    public void removeTaskExecutor(InstanceID instanceId) {
        final FineGrainedTaskManagerRegistration taskManagerRegistration =
                Preconditions.checkNotNull(taskManagerRegistrations.get(instanceId));
        for (AllocationID allocationId :
                new HashSet<>(taskManagerRegistration.getSlots().keySet())) {
            removeSlot(allocationId);
        }
        taskManagerRegistrations.remove(instanceId);
    }

    private void removeSlot(AllocationID allocationId) {
        FineGrainedTaskManagerSlot slot = slots.remove(allocationId);

        if (slot != null) {
            transitionSlotToFree(slot);
        } else {
            LOG.debug("There was no slot registered with allocation id {}.", allocationId);
        }
    }

    @Override
    public void notifyAllocationStart(
            AllocationID allocationId,
            JobID jobId,
            InstanceID instanceId,
            ResourceProfile requiredResource) {
        Preconditions.checkNotNull(allocationId);
        Preconditions.checkNotNull(jobId);
        Preconditions.checkNotNull(instanceId);
        FineGrainedTaskManagerRegistration taskManagerRegistration =
                taskManagerRegistrations.get(instanceId);
        final FineGrainedTaskManagerSlot slot =
                new FineGrainedTaskManagerSlot(
                        allocationId,
                        jobId,
                        requiredResource.equals(ResourceProfile.UNKNOWN)
                                ? taskManagerRegistration.getDefaultSlotResourceProfile()
                                : requiredResource,
                        taskManagerRegistration.getTaskManagerConnection(),
                        SlotState.PENDING);
        taskManagerRegistration.notifyAllocationStart(allocationId, slot);
        slots.put(allocationId, slot);

        slotStatusUpdateListeners.notifySlotStatusChange(
                slot, SlotState.FREE, SlotState.PENDING, slot.getJobId());
    }

    @Override
    public void notifyAllocationComplete(InstanceID instanceId, AllocationID allocationId) {
        Preconditions.checkNotNull(allocationId);
        Preconditions.checkNotNull(instanceId);
        FineGrainedTaskManagerRegistration taskManagerRegistration =
                Preconditions.checkNotNull(taskManagerRegistrations.get(instanceId));
        FineGrainedTaskManagerSlot slot = Preconditions.checkNotNull(slots.get(allocationId));

        taskManagerRegistration.notifyAllocationComplete(allocationId);
        slotStatusUpdateListeners.notifySlotStatusChange(
                slot, SlotState.PENDING, SlotState.ALLOCATED, slot.getJobId());
    }

    @Override
    public void notifyFree(AllocationID allocationId) {
        Preconditions.checkNotNull(allocationId);
        transitionSlotToFree(slots.get(allocationId));
    }

    @Override
    public void notifySlotStatus(SlotReport slotReport, InstanceID instanceId) {
        Preconditions.checkNotNull(slotReport);
        Preconditions.checkNotNull(instanceId);
        FineGrainedTaskManagerRegistration taskManagerRegistration =
                taskManagerRegistrations.get(instanceId);

        if (taskManagerRegistration == null) {
            LOG.debug(
                    "Received slot report for unknown task manager with instance id {}. Ignoring this report.",
                    instanceId);
            return;
        }

        LOG.debug("Received slot report from instance {}: {}.", instanceId, slotReport);

        Set<AllocationID> reportedAllocationIds = new HashSet<>();
        slotReport
                .iterator()
                .forEachRemaining(
                        slotStatus -> reportedAllocationIds.add(slotStatus.getAllocationID()));

        for (FineGrainedTaskManagerSlot slot :
                new HashSet<>(taskManagerRegistration.getSlots().values())) {
            // The slot was previous Allocated but is now freed.
            if (!reportedAllocationIds.contains(slot.getAllocationId())
                    && slot.getState() == SlotState.ALLOCATED) {
                transitionSlotToFree(slot);
            }
        }

        for (SlotStatus slotStatus : slotReport) {
            if (slotStatus.getAllocationID() == null) {
                continue;
            }
            notifyAllocatedSlotStatus(slotStatus, taskManagerRegistration);
        }
    }

    private void notifyAllocatedSlotStatus(
            SlotStatus slotStatus, FineGrainedTaskManagerRegistration taskManagerRegistration) {
        final AllocationID allocationId = Preconditions.checkNotNull(slotStatus.getAllocationID());
        final JobID jobId = Preconditions.checkNotNull(slotStatus.getJobID());
        final ResourceProfile resourceProfile =
                Preconditions.checkNotNull(slotStatus.getResourceProfile());

        if (taskManagerRegistration.getSlots().containsKey(allocationId)) {
            if (taskManagerRegistration.getSlots().get(allocationId).getState()
                    == SlotState.PENDING) {
                // Allocation Complete
                notifyAllocationComplete(taskManagerRegistration.getInstanceId(), allocationId);
            }
        } else {
            Preconditions.checkState(!slots.containsKey(allocationId));
            final FineGrainedTaskManagerSlot slot =
                    new FineGrainedTaskManagerSlot(
                            allocationId,
                            jobId,
                            resourceProfile,
                            taskManagerRegistration.getTaskManagerConnection(),
                            SlotState.ALLOCATED);
            taskManagerRegistration.occupySlot(slot, slot.getAllocationId());
            slots.put(allocationId, slot);
            slotStatusUpdateListeners.notifySlotStatusChange(
                    slot, SlotState.FREE, SlotState.ALLOCATED, jobId);
        }
    }

    @Override
    public Optional<TaskExecutorConnection> findTaskExecutorToFulfill(
            ResourceProfile requirement,
            TaskExecutorMatchingStrategy taskExecutorMatchingStrategy) {
        return taskExecutorMatchingStrategy
                .findMatchingTaskExecutor(
                        requirement, Collections.unmodifiableMap(taskManagerRegistrations))
                .map(
                        instanceId ->
                                taskManagerRegistrations
                                        .get(instanceId)
                                        .getTaskManagerConnection());
    }

    // ---------------------------------------------------------------------------------------------
    // Core state transitions
    // ---------------------------------------------------------------------------------------------

    private void transitionSlotToFree(FineGrainedTaskManagerSlot slot) {
        Preconditions.checkNotNull(slot);
        final JobID jobId = slot.getJobId();
        final SlotState state = slot.getState();

        taskManagerRegistrations.get(slot.getInstanceId()).freeSlot(slot.getAllocationId());
        slots.remove(slot.getAllocationId());

        slotStatusUpdateListeners.notifySlotStatusChange(slot, state, SlotState.FREE, jobId);
    }

    @Override
    public Set<InstanceID> getTaskExecutors() {
        return new HashSet<>(taskManagerRegistrations.keySet());
    }

    @Override
    public boolean isTaskExecutorRegistered(InstanceID instanceId) {
        return taskManagerRegistrations.containsKey(instanceId);
    }

    // ---------------------------------------------------------------------------------------------
    // TaskExecutor idleness / redundancy
    // ---------------------------------------------------------------------------------------------

    @Override
    public Map<InstanceID, TaskExecutorConnection> getTimeOutTaskExecutors(
            Time taskManagerTimeout) {
        Map<InstanceID, TaskExecutorConnection> timeOutTaskExecutors = new HashMap<>();
        long currentTime = System.currentTimeMillis();
        for (FineGrainedTaskManagerRegistration taskManagerRegistration :
                taskManagerRegistrations.values()) {
            if (taskManagerRegistration.isIdle()
                    && currentTime - taskManagerRegistration.getIdleSince()
                            >= taskManagerTimeout.toMilliseconds()) {
                timeOutTaskExecutors.put(
                        taskManagerRegistration.getInstanceId(),
                        taskManagerRegistration.getTaskManagerConnection());
            }
        }
        return timeOutTaskExecutors;
    }

    @Override
    public long getTaskExecutorIdleSince(InstanceID instanceId) {
        return taskManagerRegistrations.get(instanceId) == null
                ? 0
                : taskManagerRegistrations.get(instanceId).getIdleSince();
    }

    // ---------------------------------------------------------------------------------------------
    // slot / resource counts
    // ---------------------------------------------------------------------------------------------

    @Override
    public ResourceProfile getTotalRegisteredResources() {
        return taskManagerRegistrations.values().stream()
                .map(FineGrainedTaskManagerRegistration::getTotalResource)
                .reduce(ResourceProfile.ZERO, ResourceProfile::merge);
    }

    @Override
    public ResourceProfile getTotalRegisteredResourcesOf(InstanceID instanceId) {
        return taskManagerRegistrations.get(instanceId) == null
                ? ResourceProfile.ZERO
                : taskManagerRegistrations.get(instanceId).getTotalResource();
    }

    @Override
    public ResourceProfile getTotalFreeResources() {
        return taskManagerRegistrations.values().stream()
                .map(FineGrainedTaskManagerRegistration::getAvailableResource)
                .reduce(ResourceProfile.ZERO, ResourceProfile::merge);
    }

    @Override
    public ResourceProfile getTotalFreeResourcesOf(InstanceID instanceId) {
        return taskManagerRegistrations.get(instanceId) == null
                ? ResourceProfile.ZERO
                : taskManagerRegistrations.get(instanceId).getAvailableResource();
    }

    @Override
    public int getNumberRegisteredSlots() {
        return taskManagerRegistrations.values().stream()
                .mapToInt(FineGrainedTaskManagerRegistration::getDefaultNumSlots)
                .sum();
    }

    @Override
    public int getNumberRegisteredSlotsOf(InstanceID instanceId) {
        return taskManagerRegistrations.get(instanceId) != null
                ? taskManagerRegistrations.get(instanceId).getDefaultNumSlots()
                : 0;
    }

    @Override
    public int getNumberFreeSlots() {
        return taskManagerRegistrations.keySet().stream()
                .mapToInt(this::getNumberFreeSlotsOf)
                .sum();
    }

    @Override
    public int getNumberFreeTaskExecutors() {
        return (int)
                taskManagerRegistrations.values().stream()
                        .filter(
                                taskManagerRegistration ->
                                        taskManagerRegistration.getSlots().isEmpty())
                        .count();
    }

    @Override
    public int getNumberFreeSlotsOf(InstanceID instanceId) {
        return taskManagerRegistrations.get(instanceId) != null
                ? Math.max(
                        taskManagerRegistrations.get(instanceId).getDefaultNumSlots()
                                - taskManagerRegistrations
                                        .get(instanceId)
                                        .getNumberAllocatedSlots(),
                        0)
                : 0;
    }

    @Override
    public int getNumberRegisteredTaskExecutors() {
        return taskManagerRegistrations.size();
    }

    @Override
    public int getNumberPendingTaskExecutors() {
        return pendingTaskManagers.size();
    }

    @Override
    public List<PendingTaskManager> getPendingTaskManager() {
        return Collections.unmodifiableList(pendingTaskManagers);
    }

    @VisibleForTesting
    public FineGrainedTaskManagerSlot getSlot(AllocationID allocationId) {
        return slots.get(allocationId);
    }
}
