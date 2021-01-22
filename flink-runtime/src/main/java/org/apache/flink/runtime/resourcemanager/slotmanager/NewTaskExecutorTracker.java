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
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;

import java.util.List;
import java.util.Optional;

/** Tracks TaskExecutor's resource and slot status. */
interface NewTaskExecutorTracker {

    // ---------------------------------------------------------------------------------------------
    // Add / Remove (pending) Resource
    // ---------------------------------------------------------------------------------------------

    /**
     * Register a new task executor with its initial slot report.
     *
     * @param taskExecutorConnection of the new task executor
     * @param totalResourceProfile of the new task executor
     * @param defaultSlotResourceProfile of the new task executor
     */
    void addTaskExecutor(
            TaskExecutorConnection taskExecutorConnection,
            ResourceProfile totalResourceProfile,
            ResourceProfile defaultSlotResourceProfile);

    /**
     * Unregister a task executor with the given instance id.
     *
     * @param instanceId of the task executor
     */
    void removeTaskExecutor(InstanceID instanceId);

    /**
     * Add a new pending task executor with its resource profile.
     *
     * @param totalResourceProfile of the pending task executor
     * @param defaultSlotResourceProfile of the pending task executor
     */
    void addPendingTaskExecutor(
            ResourceProfile totalResourceProfile, ResourceProfile defaultSlotResourceProfile);

    /**
     * Remove a pending task executor with its resource profile.
     *
     * @param totalResourceProfile of the pending task executor
     * @param defaultSlotResourceProfile of the pending task executor
     */
    void removePendingTaskExecutor(
            ResourceProfile totalResourceProfile, ResourceProfile defaultSlotResourceProfile);

    // ---------------------------------------------------------------------------------------------
    // Slot status updates
    // ---------------------------------------------------------------------------------------------

    /**
     * Notifies the tracker about the slot statuses.
     *
     * @param instanceId of the task executor
     */
    void notifySlotStatus(
            AllocationID allocationId,
            JobID jobId,
            InstanceID instanceId,
            ResourceProfile resource,
            SlotState slotState);

    // ---------------------------------------------------------------------------------------------
    // Utility method
    // ---------------------------------------------------------------------------------------------

    /**
     * Get the instance ids of all registered task executors.
     *
     * @return a set of instance ids of all registered task executors.
     */
    List<FineGrainedTaskManagerRegistration> getRegisteredTaskExecutors();

    Optional<FineGrainedTaskManagerRegistration> getRegisteredTaskExecutor(InstanceID instanceID);

    /**
     * Get the instance ids of all registered task executors.
     *
     * @return a set of instance ids of all registered task executors.
     */
    List<PendingTaskManager> getPendingTaskExecutors();

    Optional<TaskManagerSlotInformation> getAllocatedSlot(AllocationID allocationID);

    // ---------------------------------------------------------------------------------------------
    // slot / resource counts
    // ---------------------------------------------------------------------------------------------

    StatusOverview getStatusOverview();
}
