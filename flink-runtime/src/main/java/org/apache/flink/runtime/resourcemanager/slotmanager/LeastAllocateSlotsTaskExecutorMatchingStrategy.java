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

import java.util.Comparator;
import java.util.Map;
import java.util.Optional;

/**
 * {@link TaskExecutorMatchingStrategy} which picks a matching TaskExecutor with the least number of
 * allocated slots.
 */
public enum LeastAllocateSlotsTaskExecutorMatchingStrategy implements TaskExecutorMatchingStrategy {
    INSTANCE;

    @Override
    public Optional<InstanceID> findMatchingTaskExecutor(
            ResourceProfile requirement,
            Map<InstanceID, ? extends TaskExecutorInfo> taskExecutors) {
        return taskExecutors.entrySet().stream()
                .filter(taskExecutor -> canFulfillRequirement(requirement, taskExecutor.getValue()))
                .min(
                        Comparator.comparingInt(
                                taskExecutor -> taskExecutor.getValue().getNumberAllocatedSlots()))
                .map(Map.Entry::getKey);
    }

    private boolean canFulfillRequirement(
            ResourceProfile requirement, TaskExecutorInfo taskExecutorInfo) {
        return taskExecutorInfo
                .getAvailableResource()
                .allFieldsNoLessThan(
                        requirement.equals(ResourceProfile.UNKNOWN)
                                ? taskExecutorInfo.getDefaultSlotResourceProfile()
                                : requirement);
    }
}
