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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.SlotManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.slots.ResourceRequirements;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** Implementation of {@link SlotManager} supporting fine-grained resource management. */
public class FineGrainedSlotManager implements SlotManager {
    private static final Logger LOG = LoggerFactory.getLogger(FineGrainedSlotManager.class);

    private final NewTaskExecutorTracker taskExecutorTracker;
    private final ResourceTracker resourceTracker;
    ResourceAllocationStrategy resourceAllocationStrategy;
    SlotStatusSyncer slotStatusSyncer;

    /** Scheduled executor for timeouts. */
    private final ScheduledExecutor scheduledExecutor;

    /** Timeout after which an unused TaskManager is released. */
    private final Time taskManagerTimeout;

    private final SlotManagerMetricGroup slotManagerMetricGroup;

    private final Map<JobID, String> jobMasterTargetAddresses = new HashMap<>();

    /** Defines the max limitation of the total number of task executors. */
    private final int maxTaskExecutorNum;

    /** Defines the number of redundant task executors. */
    private final int redundantTaskExecutorNum;

    /**
     * Release task executor only when each produced result partition is either consumed or failed.
     */
    private final boolean waitResultConsumedBeforeRelease;

    /** The default resource spec of workers to request. */
    private final WorkerResourceSpec defaultWorkerResourceSpec;

    /** The resource profile of default slot. */
    private final ResourceProfile defaultSlotResourceProfile;

    private boolean sendNotEnoughResourceNotifications = true;

    /** ResourceManager's id. */
    @Nullable private ResourceManagerId resourceManagerId;

    /** Executor for future callbacks which have to be "synchronized". */
    @Nullable private Executor mainThreadExecutor;

    /** Callbacks for resource (de-)allocations. */
    @Nullable private ResourceActions resourceActions;

    private ScheduledFuture<?> taskManagerTimeoutsAndRedundancyCheck;

    /** True iff the component has been started. */
    private boolean started;

    public FineGrainedSlotManager(
            ScheduledExecutor scheduledExecutor,
            SlotManagerConfiguration slotManagerConfiguration,
            SlotManagerMetricGroup slotManagerMetricGroup,
            ResourceTracker resourceTracker,
            NewTaskExecutorTracker taskExecutorTracker) {

        this.scheduledExecutor = Preconditions.checkNotNull(scheduledExecutor);

        Preconditions.checkNotNull(slotManagerConfiguration);
        this.taskManagerTimeout = slotManagerConfiguration.getTaskManagerTimeout();
        this.redundantTaskExecutorNum = slotManagerConfiguration.getRedundantTaskManagerNum();
        this.waitResultConsumedBeforeRelease =
                slotManagerConfiguration.isWaitResultConsumedBeforeRelease();
        this.defaultWorkerResourceSpec = slotManagerConfiguration.getDefaultWorkerResourceSpec();
        int numSlotsPerWorker = slotManagerConfiguration.getNumSlotsPerWorker();
        this.maxTaskExecutorNum = slotManagerConfiguration.getMaxSlotNum() / numSlotsPerWorker;

        this.slotManagerMetricGroup = Preconditions.checkNotNull(slotManagerMetricGroup);
        this.defaultSlotResourceProfile =
                SlotManagerUtils.generateDefaultSlotResourceProfile(
                        defaultWorkerResourceSpec, numSlotsPerWorker);

        this.resourceTracker = Preconditions.checkNotNull(resourceTracker);
        this.taskExecutorTracker = Preconditions.checkNotNull(taskExecutorTracker);

        resourceManagerId = null;
        resourceActions = null;
        mainThreadExecutor = null;
        taskManagerTimeoutsAndRedundancyCheck = null;

        started = false;
    }

    private SlotStatusUpdateListener createSlotStatusUpdateListener() {
        return (taskManagerSlot, previous, current, jobId) -> {
            if (current == SlotState.ALLOCATED && previous == SlotState.FREE) {
                // Pending allocation complete or register task executor with allocated slots
                resourceTracker.notifyAcquiredResource(jobId, taskManagerSlot.getResourceProfile());
            }
        };
    }

    @Override
    public void setFailUnfulfillableRequest(boolean failUnfulfillableRequest) {
        // this sets up a grace period, e.g., when the cluster was started, to give task executors
        // time to connect
        sendNotEnoughResourceNotifications = failUnfulfillableRequest;

        if (failUnfulfillableRequest) {
            checkResourceRequirements();
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Component lifecycle methods
    // ---------------------------------------------------------------------------------------------

    /**
     * Starts the slot manager with the given leader id and resource manager actions.
     *
     * @param newResourceManagerId to use for communication with the task managers
     * @param newMainThreadExecutor to use to run code in the ResourceManager's main thread
     * @param newResourceActions to use for resource (de-)allocations
     */
    @Override
    public void start(
            ResourceManagerId newResourceManagerId,
            Executor newMainThreadExecutor,
            ResourceActions newResourceActions) {
        LOG.info("Starting the slot manager.");

        resourceManagerId = Preconditions.checkNotNull(newResourceManagerId);
        mainThreadExecutor = Preconditions.checkNotNull(newMainThreadExecutor);
        resourceActions = Preconditions.checkNotNull(newResourceActions);

        started = true;

        taskManagerTimeoutsAndRedundancyCheck =
                scheduledExecutor.scheduleWithFixedDelay(
                        () ->
                                mainThreadExecutor.execute(
                                        this::checkTaskManagerTimeoutsAndRedundancy),
                        0L,
                        taskManagerTimeout.toMilliseconds(),
                        TimeUnit.MILLISECONDS);

        registerSlotManagerMetrics();
    }

    private void registerSlotManagerMetrics() {
        slotManagerMetricGroup.gauge(
                MetricNames.TASK_SLOTS_AVAILABLE, () -> (long) getNumberFreeSlots());
        slotManagerMetricGroup.gauge(
                MetricNames.TASK_SLOTS_TOTAL, () -> (long) getNumberRegisteredSlots());
    }

    /** Suspends the component. This clears the internal state of the slot manager. */
    @Override
    public void suspend() {
        if (!started) {
            return;
        }

        LOG.info("Suspending the slot manager.");

        resourceTracker.clear();

        // stop the timeout checks for the TaskManagers
        if (taskManagerTimeoutsAndRedundancyCheck != null) {
            taskManagerTimeoutsAndRedundancyCheck.cancel(false);
            taskManagerTimeoutsAndRedundancyCheck = null;
        }

        for (FineGrainedTaskManagerRegistration registeredTaskManager :
                taskExecutorTracker.getRegisteredTaskExecutors()) {
            unregisterTaskManager(
                    registeredTaskManager.getInstanceId(),
                    new SlotManagerException("The slot manager is being suspended."));
        }

        resourceManagerId = null;
        resourceActions = null;
        started = false;
    }

    /**
     * Closes the slot manager.
     *
     * @throws Exception if the close operation fails
     */
    @Override
    public void close() throws Exception {
        LOG.info("Closing the slot manager.");

        suspend();
        slotManagerMetricGroup.close();
    }

    // ---------------------------------------------------------------------------------------------
    // Public API
    // ---------------------------------------------------------------------------------------------

    @Override
    public void processResourceRequirements(ResourceRequirements resourceRequirements) {
        checkInit();
        LOG.debug(
                "Received resource requirements from job {}: {}",
                resourceRequirements.getJobId(),
                resourceRequirements.getResourceRequirements());

        if (resourceRequirements.getResourceRequirements().isEmpty()) {
            jobMasterTargetAddresses.remove(resourceRequirements.getJobId());
        } else {
            jobMasterTargetAddresses.put(
                    resourceRequirements.getJobId(), resourceRequirements.getTargetAddress());
        }
        resourceTracker.notifyResourceRequirements(
                resourceRequirements.getJobId(), resourceRequirements.getResourceRequirements());
        checkResourceRequirements();
    }

    /**
     * Registers a new task manager at the slot manager. This will make the task managers slots
     * known and, thus, available for allocation.
     *
     * @param taskExecutorConnection for the new task manager
     * @param initialSlotReport for the new task manager
     * @param totalResourceProfile of the new task manager
     * @param defaultSlotResourceProfile of the new task manager
     * @return True if the task manager has not been registered before and is registered
     *     successfully; otherwise false
     */
    @Override
    public boolean registerTaskManager(
            final TaskExecutorConnection taskExecutorConnection,
            SlotReport initialSlotReport,
            ResourceProfile totalResourceProfile,
            ResourceProfile defaultSlotResourceProfile) {
        checkInit();
        LOG.debug(
                "Registering task executor {} under {} at the slot manager.",
                taskExecutorConnection.getResourceID(),
                taskExecutorConnection.getInstanceID());

        // we identify task managers by their instance id
        if (taskExecutorTracker
                .getRegisteredTaskExecutor(taskExecutorConnection.getInstanceID())
                .isPresent()) {
            LOG.debug(
                    "Task executor {} was already registered.",
                    taskExecutorConnection.getResourceID());
            reportSlotStatus(taskExecutorConnection.getInstanceID(), initialSlotReport);
            return false;
        } else {
            if (isMaxSlotNumExceededAfterRegistration(
                    initialSlotReport, totalResourceProfile, defaultSlotResourceProfile)) {
                LOG.info(
                        "The total number of slots exceeds the max limitation {}, releasing the excess task executor.",
                        maxTaskExecutorNum);
                resourceActions.releaseResource(
                        taskExecutorConnection.getInstanceID(),
                        new FlinkException(
                                "The total number of slots exceeds the max limitation."));
                return false;
            }
            taskExecutorTracker.addTaskExecutor(
                    taskExecutorConnection, totalResourceProfile, defaultSlotResourceProfile);
            slotStatusSyncer.reportSlotStatus(
                    taskExecutorConnection.getInstanceID(), initialSlotReport);

            checkResourceRequirements();
            return true;
        }
    }

    private boolean isMaxSlotNumExceededAfterRegistration(
            SlotReport initialSlotReport,
            ResourceProfile totalResourceProfile,
            ResourceProfile defaultSlotResourceProfile) {
        if (!isMaxSlotNumExceededAfterAdding(1)) {
            return false;
        }

        if (initialSlotReport.withAllocatedSlot()) {
            return isMaxSlotNumExceededAfterAdding(1);
        }

        return isMaxSlotNumExceededAfterAdding(
                taskExecutorTracker.getPendingTaskExecutors().stream()
                                .anyMatch(
                                        pendingTaskManager ->
                                                pendingTaskManager
                                                                .getTotalResourceProfile()
                                                                .equals(totalResourceProfile)
                                                        && pendingTaskManager
                                                                .getDefaultSlotResourceProfile()
                                                                .equals(defaultSlotResourceProfile))
                        ? 0
                        : 1);
    }

    private boolean isMaxSlotNumExceededAfterAdding(int numNewTaskExecutor) {
        return taskExecutorTracker.getPendingTaskExecutors().size()
                        + taskExecutorTracker.getRegisteredTaskExecutors().size()
                        + numNewTaskExecutor
                > maxTaskExecutorNum;
    }

    @Override
    public boolean unregisterTaskManager(InstanceID instanceId, Exception cause) {
        checkInit();

        LOG.debug("Unregistering task executor {} from the slot manager.", instanceId);

        if (taskExecutorTracker.getRegisteredTaskExecutor(instanceId).isPresent()) {
            FineGrainedTaskManagerRegistration taskManager =
                    taskExecutorTracker.getRegisteredTaskExecutor(instanceId).get();
            for (TaskManagerSlotInformation slot : new HashSet<>(taskManager.getSlots().values())) {
                slotStatusSyncer.freeSlot(slot.getAllocationId());
            }
            taskExecutorTracker.removeTaskExecutor(instanceId);
            checkResourceRequirements();

            return true;
        } else {
            LOG.debug(
                    "There is no task executor registered with instance ID {}. Ignoring this message.",
                    instanceId);

            return false;
        }
    }

    /**
     * Reports the current slot allocations for a task manager identified by the given instance id.
     *
     * @param instanceId identifying the task manager for which to report the slot status
     * @param slotReport containing the status for all of its slots
     * @return true if the slot status has been updated successfully, otherwise false
     */
    @Override
    public boolean reportSlotStatus(InstanceID instanceId, SlotReport slotReport) {
        checkInit();

        LOG.debug("Received slot report from instance {}: {}.", instanceId, slotReport);

        slotStatusSyncer.reportSlotStatus(instanceId, slotReport);

        if (taskExecutorTracker.getRegisteredTaskExecutor(instanceId).isPresent()) {
            slotStatusSyncer.reportSlotStatus(instanceId, slotReport);
            checkResourceRequirements();
            return true;
        } else {
            LOG.debug(
                    "Received slot report for unknown task manager with instance id {}. Ignoring this report.",
                    instanceId);

            return false;
        }
    }

    /**
     * Free the given slot from the given allocation. If the slot is still allocated by the given
     * allocation id, then the slot will be freed.
     *
     * @param slotId identifying the slot to free, will be ignored
     * @param allocationId with which the slot is presumably allocated
     */
    @Override
    public void freeSlot(SlotID slotId, AllocationID allocationId) {
        checkInit();
        LOG.debug("Freeing slot {}.", allocationId);

        slotStatusSyncer.freeSlot(allocationId);
        checkResourceRequirements();
    }

    // ---------------------------------------------------------------------------------------------
    // Requirement matching
    // ---------------------------------------------------------------------------------------------

    /**
     * Matches resource requirements against available resources. In a first round requirements are
     * matched against free slot, and any match results in a slot allocation. The remaining
     * unfulfilled requirements are matched against pending slots, allocating more workers if no
     * matching pending slot could be found. If the requirements for a job could not be fulfilled
     * then a notification is sent to the job master informing it as such.
     *
     * <p>Performance notes: At it's core this method loops, for each job, over all free/pending
     * slots for each required slot, trying to find a matching slot. One should generally go in with
     * the assumption that this runs in numberOfJobsRequiringResources * numberOfRequiredSlots *
     * numberOfFreeOrPendingTaskExecutors. This is especially important when dealing with pending
     * slots, as matches between requirements and pending slots are not persisted and recomputed on
     * each call. This may required further refinements in the future; e.g., persisting the matches
     * between requirements and pending slots, or not matching against pending slots at all.
     *
     * <p>When dealing with unspecific resource profiles (i.e., {@link ResourceProfile#UNKNOWN}),
     * then the number of free/pending task executors is not relevant because we only need exactly 1
     * comparison to determine whether a slot can be fulfilled or not, since they are all the same
     * anyway.
     *
     * <p>When dealing with specific resource profiles things can be a lot worse, with the classical
     * cases where either no matches are found, or only at the very end of the iteration. In the
     * absolute worst case, with J jobs, requiring R slots each with a unique resource profile such
     * each pair of these profiles is not matching, and T free/pending task executors that don't
     * fulfill any requirement, then this method does a total of J*R*T resource profile comparisons.
     */
    private void checkResourceRequirements() {
        final Map<JobID, Collection<ResourceRequirement>> missingResources =
                resourceTracker.getMissingResources();
        if (missingResources.isEmpty()) {
            return;
        }

        Map<InstanceID, Tuple2<ResourceProfile, ResourceProfile>> availableResources =
                taskExecutorTracker.getRegisteredTaskExecutors().stream()
                        .collect(
                                Collectors.toMap(
                                        FineGrainedTaskManagerRegistration::getInstanceId,
                                        taskManager ->
                                                Tuple2.of(
                                                        taskManager.getAvailableResource(),
                                                        taskManager.getAvailableResource())));

        ResourceAllocationStrategy.ResourceAllocationResult result =
                resourceAllocationStrategy.checkResourceRequirements(
                        missingResources,
                        availableResources,
                        taskExecutorTracker.getPendingTaskExecutors());

        // Allocate slots
        List<CompletableFuture<Void>> allocationFutures = new ArrayList<>();
        for (JobID jobID : missingResources.keySet()) {
            for (InstanceID instanceID : result.allocationResult.get(jobID).keySet()) {
                for (Map.Entry<ResourceProfile, Integer> resourceToBeAllocated :
                        result.allocationResult
                                .get(jobID)
                                .get(instanceID)
                                .getResourceProfilesWithCount()
                                .entrySet()) {
                    for (int i = 0; i < resourceToBeAllocated.getValue(); ++i) {
                        allocationFutures.add(
                                slotStatusSyncer.allocateSlot(
                                        instanceID,
                                        jobID,
                                        jobMasterTargetAddresses.get(jobID),
                                        resourceToBeAllocated.getKey(),
                                        resourceManagerId,
                                        mainThreadExecutor));
                    }
                }
            }
        }
        FutureUtils.combineAll(allocationFutures)
                .whenComplete(
                        (s, t) -> {
                            if (t != null) {
                                checkResourceRequirements();
                            }
                        });

        // Allocate TMs
        for (PendingTaskManager pendingTaskManager : result.pendingTaskManagersToBeAllocated) {
            if (!allocateResource(
                    pendingTaskManager.getTotalResourceProfile(),
                    pendingTaskManager.getDefaultSlotResourceProfile())) {
                for (JobID jobId :
                        result.pendingTaskManagerJobMap.get(pendingTaskManager.getId())) {
                    LOG.warn("Could not fulfill resource requirements of job {}.", jobId);
                    resourceActions.notifyNotEnoughResourcesAvailable(
                            jobId, resourceTracker.getAcquiredResources(jobId));
                    continue;
                }
            }
        }

        // Notify not enough
        for (JobID jobId : missingResources.keySet()) {
            if (!result.canJobFulfill.get(jobId) && sendNotEnoughResourceNotifications) {
                LOG.warn("Could not fulfill resource requirements of job {}.", jobId);
                resourceActions.notifyNotEnoughResourcesAvailable(
                        jobId, resourceTracker.getAcquiredResources(jobId));
                continue;
            }
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Legacy APIs
    // ---------------------------------------------------------------------------------------------

    @Override
    public int getNumberRegisteredSlots() {
        return taskExecutorTracker.getStatusOverview();
    }

    @Override
    public int getNumberRegisteredSlotsOf(InstanceID instanceId) {
        return taskExecutorTracker.getStatusOverview();
    }

    @Override
    public int getNumberFreeSlots() {
        return taskExecutorTracker.getStatusOverview();
    }

    @Override
    public int getNumberFreeSlotsOf(InstanceID instanceId) {
        return taskExecutorTracker.getStatusOverview();
    }

    @Override
    public Map<WorkerResourceSpec, Integer> getRequiredResources() {
        return taskExecutorTracker.getStatusOverview();
    }

    @Override
    public ResourceProfile getRegisteredResource() {
        return taskExecutorTracker.getStatusOverview();
    }

    @Override
    public ResourceProfile getRegisteredResourceOf(InstanceID instanceID) {
        return taskExecutorTracker.getStatusOverview();
    }

    @Override
    public ResourceProfile getFreeResource() {
        return taskExecutorTracker.getStatusOverview();
    }

    @Override
    public ResourceProfile getFreeResourceOf(InstanceID instanceID) {
        return taskExecutorTracker.getStatusOverview();
    }

    @Override
    public int getNumberPendingSlotRequests() {
        // only exists for testing purposes
        throw new UnsupportedOperationException();
    }

    // ---------------------------------------------------------------------------------------------
    // Internal periodic check methods
    // ---------------------------------------------------------------------------------------------

    private void checkTaskManagerTimeoutsAndRedundancy() {
        long currentTime = System.currentTimeMillis();
        Map<InstanceID, TaskExecutorConnection> timeoutTaskExecutors =
                taskExecutorTracker.getRegisteredTaskExecutors().stream()
                        .filter(
                                taskManager ->
                                        taskManager.isIdle()
                                                && currentTime - taskManager.getIdleSince()
                                                        >= taskManagerTimeout.toMilliseconds())
                        .collect(
                                Collectors.toMap(
                                        FineGrainedTaskManagerRegistration::getInstanceId,
                                        FineGrainedTaskManagerRegistration
                                                ::getTaskManagerConnection));
        if (!timeoutTaskExecutors.isEmpty()) {
            int freeTaskManagersNum =
                    (int)
                            taskExecutorTracker.getRegisteredTaskExecutors().stream()
                                    .filter(taskManager -> taskManager.getSlots().isEmpty())
                                    .count();
            int taskManagersDiff = redundantTaskExecutorNum - freeTaskManagersNum;
            if (taskManagersDiff > 0) {
                // Keep enough redundant taskManagers from time to time.
                allocateRedundantTaskExecutors(taskManagersDiff);
            } else {
                // second we trigger the release resource callback which can decide upon the
                // resource release
                releaseIdleTaskExecutors(
                        timeoutTaskExecutors,
                        Math.min(-taskManagersDiff, timeoutTaskExecutors.size()));
            }
        }
    }

    private void releaseIdleTaskExecutors(
            Map<InstanceID, TaskExecutorConnection> timeoutTaskExecutors, int releaseNum) {
        for (Map.Entry<InstanceID, TaskExecutorConnection> timeoutTaskExecutor :
                timeoutTaskExecutors.entrySet()) {
            if (releaseNum == 0) {
                break;
            }
            releaseNum -= 1;
            if (waitResultConsumedBeforeRelease) {
                releaseIdleTaskExecutorIfPossible(
                        timeoutTaskExecutor.getKey(), timeoutTaskExecutor.getValue());
            } else {
                releaseIdleTaskExecutor(timeoutTaskExecutor.getKey());
            }
        }
    }

    private void releaseIdleTaskExecutorIfPossible(
            InstanceID instanceId, TaskExecutorConnection taskExecutorConnection) {
        long idleSince =
                taskExecutorTracker
                        .getRegisteredTaskExecutor(instanceId)
                        .map(FineGrainedTaskManagerRegistration::getIdleSince)
                        .orElse(0L);
        taskExecutorConnection
                .getTaskExecutorGateway()
                .canBeReleased()
                .thenAcceptAsync(
                        canBeReleased -> {
                            boolean stillIdle =
                                    idleSince
                                            == taskExecutorTracker
                                                    .getRegisteredTaskExecutor(instanceId)
                                                    .map(
                                                            FineGrainedTaskManagerRegistration
                                                                    ::getIdleSince)
                                                    .orElse(0L);
                            if (stillIdle && canBeReleased) {
                                releaseIdleTaskExecutor(instanceId);
                            }
                        },
                        mainThreadExecutor);
    }

    private void releaseIdleTaskExecutor(InstanceID timedOutTaskManagerId) {
        final FlinkException cause = new FlinkException("TaskExecutor exceeded the idle timeout.");
        LOG.debug(
                "Release TaskExecutor {} because it exceeded the idle timeout.",
                timedOutTaskManagerId);
        resourceActions.releaseResource(timedOutTaskManagerId, cause);
    }

    private void allocateRedundantTaskExecutors(int number) {
        LOG.debug("Allocating {} task executors for redundancy.", number);
        int allocatedNumber = allocateResources(number);
        if (number != allocatedNumber) {
            LOG.warn(
                    "Expect to allocate {} taskManagers. Actually allocate {} taskManagers.",
                    number,
                    allocatedNumber);
        }
    }

    /**
     * Allocate a number of workers based on the input param.
     *
     * @param workerNum the number of workers to allocate.
     * @return the number of allocated workers successfully.
     */
    private int allocateResources(int workerNum) {
        int allocatedWorkerNum = 0;
        for (int i = 0; i < workerNum; ++i) {
            if (allocateResource(
                    SlotManagerUtils.generateDefaultTaskManagerResourceProfile(
                            defaultWorkerResourceSpec),
                    defaultSlotResourceProfile)) {
                ++allocatedWorkerNum;
            } else {
                break;
            }
        }
        return allocatedWorkerNum;
    }

    private boolean allocateResource(
            ResourceProfile taskExecutorProfile, ResourceProfile defaultSlotResourceProfile) {
        if (isMaxSlotNumExceededAfterAdding(1)) {
            LOG.warn(
                    "Could not allocate more task executors. The number of registered and pending task executors are {} and {}, while the maximum is {}.",
                    taskExecutorTracker.getRegisteredTaskExecutors().size(),
                    taskExecutorTracker.getPendingTaskExecutors().size(),
                    maxTaskExecutorNum);
            return false;
        }

        if (!resourceActions.allocateResource(
                WorkerResourceSpec.fromResourceProfile(taskExecutorProfile))) {
            // resource cannot be allocated
            return false;
        }

        taskExecutorTracker.addPendingTaskExecutor(taskExecutorProfile, defaultSlotResourceProfile);
        return true;
    }

    // ---------------------------------------------------------------------------------------------
    // Internal utility methods
    // ---------------------------------------------------------------------------------------------

    private void checkInit() {
        Preconditions.checkState(started, "The slot manager has not been started.");
    }
}
