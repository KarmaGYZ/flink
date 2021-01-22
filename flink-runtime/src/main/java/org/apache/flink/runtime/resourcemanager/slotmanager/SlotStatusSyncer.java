package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.exceptions.SlotOccupiedException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public class SlotStatusSyncer {
    private static final Logger LOG = LoggerFactory.getLogger(FineGrainedSlotManager.class);
    NewTaskExecutorTracker taskExecutorTracker;
    ResourceTracker resourceTracker;
    private Set<AllocationID> pendingSlotAllocations;
    /** Timeout for slot requests to the task manager. */
    private Time taskManagerRequestTimeout;

    public CompletableFuture<Void> allocateSlot(
            InstanceID instanceId,
            JobID jobId,
            String targetAddress,
            ResourceProfile resourceProfile,
            ResourceManagerId resourceManagerId,
            Executor mainThreadExecutor) {
        final AllocationID allocationId = new AllocationID();
        final Optional<FineGrainedTaskManagerRegistration> taskManager =
                taskExecutorTracker.getRegisteredTaskExecutor(instanceId);
        if (!taskManager.isPresent()) {
            throw new IllegalStateException(
                    "Could not find a registered task manager for instance id " + instanceId + '.');
        }

        final TaskExecutorGateway gateway =
                taskManager.get().getTaskManagerConnection().getTaskExecutorGateway();

        taskExecutorTracker.notifySlotStatus(
                allocationId, jobId, instanceId, resourceProfile, SlotState.PENDING);
        resourceTracker.notifyAcquiredResource(jobId, resourceProfile);
        pendingSlotAllocations.add(allocationId);

        // RPC call to the task manager
        CompletableFuture<Acknowledge> requestFuture =
                gateway.requestSlot(
                        SlotID.getDynamicSlotID(
                                taskManager.get().getTaskManagerConnection().getResourceID()),
                        jobId,
                        allocationId,
                        resourceProfile,
                        targetAddress,
                        resourceManagerId,
                        taskManagerRequestTimeout);

        CompletableFuture<Void> returned = new CompletableFuture<>();

        FutureUtils.assertNoException(
                requestFuture.whenCompleteAsync(
                        (Acknowledge acknowledge, Throwable throwable) -> {
                            if (!pendingSlotAllocations.remove(allocationId)) {
                                LOG.debug(
                                        "Ignoring slot allocation update from task executor {} for allocation {} and job {}, because the allocation was already completed or cancelled.",
                                        instanceId,
                                        allocationId,
                                        jobId);
                                returned.complete(null);
                            }
                            if (acknowledge != null) {
                                LOG.trace(
                                        "Completed allocation of allocation {} for job {}.",
                                        allocationId,
                                        jobId);
                                taskExecutorTracker.notifySlotStatus(
                                        allocationId,
                                        jobId,
                                        instanceId,
                                        resourceProfile,
                                        SlotState.ALLOCATED);
                                returned.complete(null);
                            } else {
                                if (throwable instanceof SlotOccupiedException) {
                                    LOG.error("Should not get this exception.", throwable);
                                } else {
                                    // TODO If the taskExecutor does not have enough resource, we
                                    // may endlessly allocate slot on it until the next heartbeat.
                                    LOG.warn(
                                            "Slot allocation for allocation {} for job {} failed.",
                                            allocationId,
                                            jobId,
                                            throwable);
                                    taskExecutorTracker.notifySlotStatus(
                                            allocationId,
                                            jobId,
                                            instanceId,
                                            resourceProfile,
                                            SlotState.FREE);
                                }
                                returned.completeExceptionally(throwable);
                            }
                        },
                        mainThreadExecutor));
        return returned;
    }

    public void freeSlot(AllocationID allocationId) {
        LOG.debug("Freeing slot {}.", allocationId);

        taskExecutorTracker
                .getAllocatedSlot(allocationId)
                .ifPresent(
                        slot -> {
                            resourceTracker.notifyLostResource(
                                    slot.getJobId(), slot.getResourceProfile());
                            taskExecutorTracker.notifySlotStatus(
                                    allocationId,
                                    slot.getJobId(),
                                    slot.getInstanceId(),
                                    slot.getResourceProfile(),
                                    SlotState.FREE);
                        });
    }

    public void reportSlotStatus(InstanceID instanceId, SlotReport slotReport) {
        Preconditions.checkNotNull(slotReport);
        Preconditions.checkNotNull(instanceId);
        Optional<FineGrainedTaskManagerRegistration> taskManagerRegistration =
                taskExecutorTracker.getRegisteredTaskExecutor(instanceId);

        if (!taskManagerRegistration.isPresent()) {
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
                new HashSet<>(taskManagerRegistration.get().getSlots().values())) {
            // The slot was previous Allocated but is now freed.
            if (!reportedAllocationIds.contains(slot.getAllocationId())
                    && slot.getState() == SlotState.ALLOCATED) {
                taskExecutorTracker.notifySlotStatus(
                        slot.getAllocationId(),
                        slot.getJobId(),
                        slot.getInstanceId(),
                        slot.getResourceProfile(),
                        SlotState.FREE);
                resourceTracker.notifyLostResource(slot.getJobId(), slot.getResourceProfile());
            }
        }

        for (SlotStatus slotStatus : slotReport) {
            if (slotStatus.getAllocationID() == null) {
                continue;
            }
            syncAllocatedSlotStatus(slotStatus, taskManagerRegistration.get());
        }
    }

    private void syncAllocatedSlotStatus(
            SlotStatus slotStatus, FineGrainedTaskManagerRegistration taskManagerRegistration) {
        final AllocationID allocationId = Preconditions.checkNotNull(slotStatus.getAllocationID());
        final JobID jobId = Preconditions.checkNotNull(slotStatus.getJobID());
        final ResourceProfile resourceProfile =
                Preconditions.checkNotNull(slotStatus.getResourceProfile());

        if (taskManagerRegistration.getSlots().containsKey(allocationId)) {
            if (taskManagerRegistration.getSlots().get(allocationId).getState()
                    == SlotState.PENDING) {
                // Allocation Complete
                FineGrainedTaskManagerSlot slot =
                        taskManagerRegistration.getSlots().get(allocationId);
                pendingSlotAllocations.remove(slot.getAllocationId());
                taskExecutorTracker.notifySlotStatus(
                        slot.getAllocationId(),
                        slot.getJobId(),
                        slot.getInstanceId(),
                        slot.getResourceProfile(),
                        slot.getState());
            }
        } else {
            Preconditions.checkState(
                    !taskExecutorTracker.getAllocatedSlot(allocationId).isPresent());
            taskExecutorTracker.notifySlotStatus(
                    allocationId,
                    jobId,
                    taskManagerRegistration.getInstanceId(),
                    resourceProfile,
                    SlotState.ALLOCATED);
            resourceTracker.notifyAcquiredResource(jobId, resourceProfile);
        }
    }
}
