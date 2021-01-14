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
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.SlotManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.slots.ResourceCounter;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.slots.ResourceRequirements;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.exceptions.SlotOccupiedException;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Implementation of {@link SlotManager} supporting fine-grained resource management. */
public class FineGrainedSlotManager implements SlotManager {
    private static final Logger LOG = LoggerFactory.getLogger(FineGrainedSlotManager.class);

    private final TaskExecutorTracker taskExecutorTracker;
    private final ResourceTracker resourceTracker;

    /** Scheduled executor for timeouts. */
    private final ScheduledExecutor scheduledExecutor;

    /** Timeout for slot requests to the task manager. */
    private final Time taskManagerRequestTimeout;

    /** Timeout after which an unused TaskManager is released. */
    private final Time taskManagerTimeout;

    private final TaskExecutorMatchingStrategy taskExecutorMatchingStrategy;

    private final TaskExecutorAllocationStrategy taskExecutorAllocationStrategy;

    private final SlotManagerMetricGroup slotManagerMetricGroup;

    private final Map<JobID, String> jobMasterTargetAddresses = new HashMap<>();
    private final Set<AllocationID> pendingSlotAllocations;

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
            TaskExecutorTracker taskExecutorTracker) {

        this.scheduledExecutor = Preconditions.checkNotNull(scheduledExecutor);

        Preconditions.checkNotNull(slotManagerConfiguration);
        this.taskManagerRequestTimeout = slotManagerConfiguration.getTaskManagerRequestTimeout();
        this.taskManagerTimeout = slotManagerConfiguration.getTaskManagerTimeout();
        this.redundantTaskExecutorNum = slotManagerConfiguration.getRedundantTaskManagerNum();
        this.waitResultConsumedBeforeRelease =
                slotManagerConfiguration.isWaitResultConsumedBeforeRelease();
        this.defaultWorkerResourceSpec = slotManagerConfiguration.getDefaultWorkerResourceSpec();
        this.taskExecutorMatchingStrategy =
                slotManagerConfiguration.getTaskExecutorMatchingStrategy();
        this.taskExecutorAllocationStrategy =
                slotManagerConfiguration.getTaskExecutorAllocationStrategy();
        int numSlotsPerWorker = slotManagerConfiguration.getNumSlotsPerWorker();
        this.maxTaskExecutorNum = slotManagerConfiguration.getMaxSlotNum() / numSlotsPerWorker;

        this.slotManagerMetricGroup = Preconditions.checkNotNull(slotManagerMetricGroup);
        this.defaultSlotResourceProfile =
                SlotManagerUtils.generateDefaultSlotResourceProfile(
                        defaultWorkerResourceSpec, numSlotsPerWorker);

        pendingSlotAllocations = new HashSet<>(16);

        this.resourceTracker = Preconditions.checkNotNull(resourceTracker);
        this.taskExecutorTracker = Preconditions.checkNotNull(taskExecutorTracker);
        taskExecutorTracker.registerSlotStatusUpdateListener(createSlotStatusUpdateListener());

        resourceManagerId = null;
        resourceActions = null;
        mainThreadExecutor = null;
        taskManagerTimeoutsAndRedundancyCheck = null;

        started = false;
    }

    private SlotStatusUpdateListener createSlotStatusUpdateListener() {
        return (taskManagerSlot, previous, current, jobId) -> {
            if (previous == SlotState.PENDING) {
                pendingSlotAllocations.remove(taskManagerSlot.getAllocationId());
            }

            if (current == SlotState.PENDING
                    || (current == SlotState.ALLOCATED && previous == SlotState.FREE)) {
                // Pending allocation complete or register task executor with allocated slots
                resourceTracker.notifyAcquiredResource(jobId, taskManagerSlot.getResourceProfile());
            }
            if (current == SlotState.FREE) {
                resourceTracker.notifyLostResource(jobId, taskManagerSlot.getResourceProfile());
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

        for (InstanceID registeredTaskManager : taskExecutorTracker.getTaskExecutors()) {
            unregisterTaskManager(
                    registeredTaskManager,
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
        if (taskExecutorTracker.isTaskExecutorRegistered(taskExecutorConnection.getInstanceID())) {
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
                    taskExecutorConnection,
                    initialSlotReport,
                    totalResourceProfile,
                    defaultSlotResourceProfile);

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

        return isMaxSlotNumExceededAfterAdding(
                taskExecutorTracker
                                .findMatchingPendingTaskManager(
                                        initialSlotReport,
                                        totalResourceProfile,
                                        defaultSlotResourceProfile)
                                .isPresent()
                        ? 0
                        : 1);
    }

    private boolean isMaxSlotNumExceededAfterAdding(int numNewTaskExecutor) {
        return taskExecutorTracker.getNumberPendingTaskExecutors()
                        + taskExecutorTracker.getNumberRegisteredTaskExecutors()
                        + numNewTaskExecutor
                > maxTaskExecutorNum;
    }

    @Override
    public boolean unregisterTaskManager(InstanceID instanceId, Exception cause) {
        checkInit();

        LOG.debug("Unregistering task executor {} from the slot manager.", instanceId);

        if (taskExecutorTracker.isTaskExecutorRegistered(instanceId)) {
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

        if (taskExecutorTracker.isTaskExecutorRegistered(instanceId)) {
            taskExecutorTracker.notifySlotStatus(slotReport, instanceId);
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

        taskExecutorTracker.notifyFree(allocationId);
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

        final Map<JobID, ResourceCounter> unfulfilledRequirements = new LinkedHashMap<>();
        for (Map.Entry<JobID, Collection<ResourceRequirement>> resourceRequirements :
                missingResources.entrySet()) {
            final JobID jobId = resourceRequirements.getKey();

            final ResourceCounter unfulfilledJobRequirements =
                    tryAllocateSlotsForJob(jobId, resourceRequirements.getValue());
            if (!unfulfilledJobRequirements.isEmpty()) {
                unfulfilledRequirements.put(jobId, unfulfilledJobRequirements);
            }
        }
        if (unfulfilledRequirements.isEmpty()) {
            return;
        }

        final Map<PendingTaskManager, Integer> pendingResources =
                taskExecutorTracker.getPendingTaskManager().stream()
                        .collect(
                                Collectors.groupingBy(
                                        Function.identity(), Collectors.summingInt(x -> 1)));

        tryFulfillRequirementsWithPendingResources(unfulfilledRequirements, pendingResources);
    }

    private ResourceCounter tryAllocateSlotsForJob(
            JobID jobId, Collection<ResourceRequirement> missingResources) {
        final ResourceCounter outstandingRequirements = new ResourceCounter();

        for (ResourceRequirement resourceRequirement : missingResources) {
            int numMissingRequirements =
                    internalTryAllocateSlots(
                            jobId, jobMasterTargetAddresses.get(jobId), resourceRequirement);
            if (numMissingRequirements > 0) {
                outstandingRequirements.incrementCount(
                        resourceRequirement.getResourceProfile(), numMissingRequirements);
            }
        }
        return outstandingRequirements;
    }

    /**
     * Tries to allocate slots for the given requirement. If there are not enough resources
     * available, the resource manager is informed to allocate more resources.
     *
     * @param jobId job to allocate slots for
     * @param targetAddress address of the jobmaster
     * @param resourceRequirement required slots
     * @return the number of unfulfilled requirements
     */
    private int internalTryAllocateSlots(
            JobID jobId, String targetAddress, ResourceRequirement resourceRequirement) {
        final ResourceProfile requiredResource = resourceRequirement.getResourceProfile();

        int numUnfulfilled = 0;
        for (int x = 0; x < resourceRequirement.getNumberOfRequiredSlots(); x++) {
            final Optional<TaskExecutorConnection> matchedTaskExecutor =
                    taskExecutorTracker.findTaskExecutorToFulfill(
                            requiredResource, taskExecutorMatchingStrategy);
            if (matchedTaskExecutor.isPresent()) {
                allocateSlot(matchedTaskExecutor.get(), jobId, targetAddress, requiredResource);
            } else {
                // exit loop early; we won't find a matching slot for this requirement
                int numRemaining = resourceRequirement.getNumberOfRequiredSlots() - x;
                numUnfulfilled += numRemaining;
                break;
            }
        }
        return numUnfulfilled;
    }

    /**
     * Allocates the resource from the given task executor. This entails sending a registration
     * message to the task manager and treating failures.
     *
     * @param taskExecutorConnection of the task executor
     * @param jobId job for which the slot should be allocated for
     * @param targetAddress address of the job master
     * @param resourceProfile resource profile for the requirement for which the slot is used
     */
    private void allocateSlot(
            TaskExecutorConnection taskExecutorConnection,
            JobID jobId,
            String targetAddress,
            ResourceProfile resourceProfile) {
        LOG.debug(
                "Starting allocation for job {} with resource profile {}.", jobId, resourceProfile);

        final InstanceID instanceId = taskExecutorConnection.getInstanceID();
        final AllocationID allocationId = new AllocationID();
        if (!taskExecutorTracker.isTaskExecutorRegistered(instanceId)) {
            throw new IllegalStateException(
                    "Could not find a registered task manager for instance id " + instanceId + '.');
        }

        final TaskExecutorGateway gateway = taskExecutorConnection.getTaskExecutorGateway();

        taskExecutorTracker.notifyAllocationStart(
                allocationId, jobId, taskExecutorConnection.getInstanceID(), resourceProfile);
        pendingSlotAllocations.add(allocationId);

        // RPC call to the task manager
        CompletableFuture<Acknowledge> requestFuture =
                gateway.requestSlot(
                        SlotID.getDynamicSlotID(taskExecutorConnection.getResourceID()),
                        jobId,
                        allocationId,
                        resourceProfile,
                        targetAddress,
                        resourceManagerId,
                        taskManagerRequestTimeout);

        CompletableFuture<Void> slotAllocationResponseProcessingFuture =
                requestFuture.handleAsync(
                        (Acknowledge acknowledge, Throwable throwable) -> {
                            if (!pendingSlotAllocations.contains(allocationId)) {
                                LOG.debug(
                                        "Ignoring slot allocation update from task executor {} for allocation {} and job {}, because the allocation was already completed or cancelled.",
                                        instanceId,
                                        allocationId,
                                        jobId);
                                return null;
                            }
                            if (acknowledge != null) {
                                LOG.trace(
                                        "Completed allocation of allocation {} for job {}.",
                                        allocationId,
                                        jobId);
                                taskExecutorTracker.notifyAllocationComplete(
                                        instanceId, allocationId);
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
                                    taskExecutorTracker.notifyFree(allocationId);
                                }
                                checkResourceRequirements();
                            }
                            return null;
                        },
                        mainThreadExecutor);
        FutureUtils.assertNoException(slotAllocationResponseProcessingFuture);
    }

    private void tryFulfillRequirementsWithPendingResources(
            Map<JobID, ResourceCounter> unfulfilledRequirements,
            Map<PendingTaskManager, Integer> pendingResources) {
        final Map<JobID, Tuple2<Map<PendingTaskManager, Integer>, Boolean>> allocationResults =
                taskExecutorAllocationStrategy.getTaskExecutorsToFulfill(
                        unfulfilledRequirements, pendingResources);
        for (JobID jobId : allocationResults.keySet()) {
            Tuple2<Map<PendingTaskManager, Integer>, Boolean> allocationResult =
                    allocationResults.get(jobId);
            final boolean hasUnfulfilled = allocationResult.f1;
            final Map<PendingTaskManager, Integer> taskExecutorsToBeAllocated = allocationResult.f0;

            if (hasUnfulfilled && sendNotEnoughResourceNotifications) {
                LOG.warn("Could not fulfill resource requirements of job {}.", jobId);
                resourceActions.notifyNotEnoughResourcesAvailable(
                        jobId, resourceTracker.getAcquiredResources(jobId));
                return;
            }

            for (PendingTaskManager pendingTaskManager : taskExecutorsToBeAllocated.keySet()) {
                for (int i = 0; i < taskExecutorsToBeAllocated.get(pendingTaskManager); ++i) {
                    if (!allocateResource(
                                    pendingTaskManager.getTotalResourceProfile(),
                                    pendingTaskManager.getDefaultSlotResourceProfile())
                            && sendNotEnoughResourceNotifications) {
                        LOG.warn("Could not fulfill resource requirements of job {}.", jobId);
                        resourceActions.notifyNotEnoughResourcesAvailable(
                                jobId, resourceTracker.getAcquiredResources(jobId));
                        return;
                    }
                }
            }
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Legacy APIs
    // ---------------------------------------------------------------------------------------------

    @Override
    public int getNumberRegisteredSlots() {
        return taskExecutorTracker.getNumberRegisteredSlots();
    }

    @Override
    public int getNumberRegisteredSlotsOf(InstanceID instanceId) {
        return taskExecutorTracker.getNumberRegisteredSlotsOf(instanceId);
    }

    @Override
    public int getNumberFreeSlots() {
        return taskExecutorTracker.getNumberFreeSlots();
    }

    @Override
    public int getNumberFreeSlotsOf(InstanceID instanceId) {
        return taskExecutorTracker.getNumberFreeSlotsOf(instanceId);
    }

    @Override
    public Map<WorkerResourceSpec, Integer> getRequiredResources() {
        return taskExecutorTracker.getPendingTaskManager().stream()
                .map(PendingTaskManager::getTotalResourceProfile)
                .map(WorkerResourceSpec::fromResourceProfile)
                .collect(Collectors.groupingBy(Function.identity(), Collectors.summingInt(e -> 1)));
    }

    @Override
    public ResourceProfile getRegisteredResource() {
        return taskExecutorTracker.getTotalRegisteredResources();
    }

    @Override
    public ResourceProfile getRegisteredResourceOf(InstanceID instanceID) {
        return taskExecutorTracker.getTotalRegisteredResourcesOf(instanceID);
    }

    @Override
    public ResourceProfile getFreeResource() {
        return taskExecutorTracker.getTotalFreeResources();
    }

    @Override
    public ResourceProfile getFreeResourceOf(InstanceID instanceID) {
        return taskExecutorTracker.getTotalFreeResourcesOf(instanceID);
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
        Map<InstanceID, TaskExecutorConnection> timeoutTaskExecutors =
                taskExecutorTracker.getTimeOutTaskExecutors(taskManagerTimeout);
        if (!timeoutTaskExecutors.isEmpty()) {
            int freeTaskManagersNum = taskExecutorTracker.getNumberFreeTaskExecutors();
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
        long idleSince = taskExecutorTracker.getTaskExecutorIdleSince(instanceId);
        taskExecutorConnection
                .getTaskExecutorGateway()
                .canBeReleased()
                .thenAcceptAsync(
                        canBeReleased -> {
                            boolean stillIdle =
                                    idleSince
                                            == taskExecutorTracker.getTaskExecutorIdleSince(
                                                    instanceId);
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
                    taskExecutorTracker.getNumberRegisteredTaskExecutors(),
                    taskExecutorTracker.getNumberPendingTaskExecutors(),
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
