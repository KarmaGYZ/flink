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
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.taskexecutor.SlotReport;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Tracks TaskExecutor's resource and slot status. */
interface TaskExecutorTracker {

    /**
     * Registers the given listener with this tracker.
     *
     * @param slotStatusUpdateListener listener to register
     */
    void registerSlotStatusUpdateListener(SlotStatusUpdateListener slotStatusUpdateListener);

    // ---------------------------------------------------------------------------------------------
    // Add / Remove (pending) Resource
    // ---------------------------------------------------------------------------------------------

    /**
     * Register a new task executor with its initial slot report.
     *
     * @param taskExecutorConnection of the new task executor
     * @param initialSlotReport of the new task executor
     * @param totalResourceProfile of the new task executor
     * @param defaultSlotResourceProfile of the new task executor
     */
    void addTaskExecutor(
            TaskExecutorConnection taskExecutorConnection,
            SlotReport initialSlotReport,
            ResourceProfile totalResourceProfile,
            ResourceProfile defaultSlotResourceProfile);

    /**
     * Add a new pending task executor with its resource profile.
     *
     * @param totalResourceProfile of the pending task executor
     * @param defaultSlotResourceProfile of the pending task executor
     */
    void addPendingTaskExecutor(
            ResourceProfile totalResourceProfile, ResourceProfile defaultSlotResourceProfile);

    /**
     * Unregister a task executor with the given instance id.
     *
     * @param instanceId of the task executor
     */
    void removeTaskExecutor(InstanceID instanceId);

    // ---------------------------------------------------------------------------------------------
    // Slot status updates
    // ---------------------------------------------------------------------------------------------

    /**
     * Find a task executor to fulfill the given resource requirement.
     *
     * @param resourceProfile of the given requirement
     * @param taskExecutorMatchingStrategy to use
     * @return An Optional of {@link TaskExecutorConnection}, if find, of the matching task
     *     executor.
     */
    Optional<TaskExecutorConnection> findTaskExecutorToFulfill(
            ResourceProfile resourceProfile,
            TaskExecutorMatchingStrategy taskExecutorMatchingStrategy);

    /**
     * Notify the allocation of the given slot is started.
     *
     * @param allocationId of the allocated slot
     * @param jobId of the allocated slot
     * @param instanceId of the located task executor
     * @param requiredResource of the allocated slot
     */
    void notifyAllocationStart(
            AllocationID allocationId,
            JobID jobId,
            InstanceID instanceId,
            ResourceProfile requiredResource);

    /**
     * Notify the allocation of the given slot is completed.
     *
     * @param instanceId of the located task executor
     * @param allocationId of the allocated slot
     */
    void notifyAllocationComplete(InstanceID instanceId, AllocationID allocationId);

    /**
     * Notify the given allocated slot is free.
     *
     * @param allocationId of the allocated slot
     */
    void notifyFree(AllocationID allocationId);

    /**
     * Notifies the tracker about the slot statuses.
     *
     * @param slotReport slot report
     * @param instanceId of the task executor
     */
    void notifySlotStatus(SlotReport slotReport, InstanceID instanceId);

    // ---------------------------------------------------------------------------------------------
    // Utility method
    // ---------------------------------------------------------------------------------------------

    /**
     * Get the instance ids of all registered task executors.
     *
     * @return a set of instance ids of all registered task executors.
     */
    Set<InstanceID> getTaskExecutors();

    /**
     * Check if there is a registered task executor with the given instance id.
     *
     * @param instanceId of the task executor
     * @return whether there is a registered task executor with the given instance id
     */
    boolean isTaskExecutorRegistered(InstanceID instanceId);

    /**
     * Find an exactly matching pending task executor with the given resource profile.
     *
     * @param initialSlotReport of the task executor
     * @param totalResourceProfile of the task executor
     * @param defaultResourceProfile of the task executor
     * @return An Optional of {@link PendingTaskManager}, if find, of the matching pending task
     *     executor.
     */
    Optional<PendingTaskManager> findMatchingPendingTaskManager(
            SlotReport initialSlotReport,
            ResourceProfile totalResourceProfile,
            ResourceProfile defaultResourceProfile);

    // ---------------------------------------------------------------------------------------------
    // TaskExecutor idleness / redundancy
    // ---------------------------------------------------------------------------------------------

    /**
     * Get all task executors which idle exceed the given timeout period
     *
     * @param taskManagerTimeout timeout period
     * @return a map of timeout task executors' connection index by the instance id
     */
    Map<InstanceID, TaskExecutorConnection> getTimeOutTaskExecutors(Time taskManagerTimeout);

    /**
     * Get the start time of idleness of the task executor with the given instance id.
     *
     * @param instanceId of the task executor
     * @return the start time of idleness
     */
    long getTaskExecutorIdleSince(InstanceID instanceId);

    // ---------------------------------------------------------------------------------------------
    // slot / resource counts
    // ---------------------------------------------------------------------------------------------

    /**
     * Get the total registered resources.
     *
     * @return the total registered resources
     */
    ResourceProfile getTotalRegisteredResources();

    /**
     * Get the total registered resources of the given task executor
     *
     * @param instanceId of the task executor
     * @return the total registered resources of the given task executor
     */
    ResourceProfile getTotalRegisteredResourcesOf(InstanceID instanceId);

    /**
     * Get the total free resources.
     *
     * @return the total free resources
     */
    ResourceProfile getTotalFreeResources();

    /**
     * Get the total free resources of the given task executor
     *
     * @param instanceId of the task executor
     * @return the total free resources of the given task executor
     */
    ResourceProfile getTotalFreeResourcesOf(InstanceID instanceId);

    /**
     * Get the number of registered slots.
     *
     * @return the number of registered slots
     */
    int getNumberRegisteredSlots();

    /**
     * Get the number of registered slots of the given task executor.
     *
     * @param instanceId of the task executor
     * @return the number of registered slots of the given task executor
     */
    int getNumberRegisteredSlotsOf(InstanceID instanceId);

    /**
     * Get the number of free slots.
     *
     * @return the number of free slots.
     */
    int getNumberFreeSlots();

    /**
     * Get the number of free task executors.
     *
     * @return the number of free task executors
     */
    int getNumberFreeTaskExecutors();

    /**
     * Get the number of the free slots of the given task executor.
     *
     * @param instanceId of the task executor
     * @return the number of the free slots of the given task executor
     */
    int getNumberFreeSlotsOf(InstanceID instanceId);

    /**
     * Get the number of registered task executors.
     *
     * @return the number of registered task executors
     */
    int getNumberRegisteredTaskExecutors();

    /**
     * Get the number of pending task executors.
     *
     * @return the number of pending task executors
     */
    int getNumberPendingTaskExecutors();

    /**
     * Get all pending task executors.
     *
     * @return all pending task executors
     */
    List<PendingTaskManager> getPendingTaskManager();
}
