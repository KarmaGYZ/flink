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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.slots.ResourceCounter;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * The default implementation of {@link TaskExecutorAllocationStrategy}, which always allocate
 * pending task executors with the fixed profile.
 */
public class DefaultTaskExecutorAllocationStrategy implements TaskExecutorAllocationStrategy {
    private final ResourceProfile defaultSlotResourceProfile;
    private final ResourceProfile totalResourceProfile;
    private final PendingTaskManager defaultPendingTaskManager;

    public DefaultTaskExecutorAllocationStrategy(
            ResourceProfile defaultSlotResourceProfile, int numSlotsPerWorker) {
        this.defaultSlotResourceProfile = defaultSlotResourceProfile;
        this.totalResourceProfile = defaultSlotResourceProfile.multiply(numSlotsPerWorker);
        this.defaultPendingTaskManager =
                new PendingTaskManager(totalResourceProfile, defaultSlotResourceProfile);
    }

    @Override
    public Map<JobID, Tuple2<Map<PendingTaskManager, Integer>, Boolean>> getTaskExecutorsToFulfill(
            Map<JobID, ResourceCounter> requirements,
            Map<PendingTaskManager, Integer> existingPendingResources) {
        Preconditions.checkState(
                existingPendingResources.isEmpty()
                        || (existingPendingResources.containsKey(defaultPendingTaskManager)
                                && existingPendingResources.size() == 1),
                "The DefaultTaskExecutorAllocationStrategy should only allocate pending task executors with the fixed profile.");
        ResourceCounter availableResources = new ResourceCounter();
        if (existingPendingResources.containsKey(defaultPendingTaskManager)) {
            availableResources.incrementCount(
                    totalResourceProfile, existingPendingResources.get(defaultPendingTaskManager));
        }
        Map<JobID, Tuple2<Map<PendingTaskManager, Integer>, Boolean>> allocationResults =
                new HashMap<>();
        for (JobID jobId : requirements.keySet()) {
            int numTaskExecutors = 0;
            boolean hasUnfulfilled = false;
            for (Map.Entry<ResourceProfile, Integer> missingResource :
                    requirements.get(jobId).getResourceProfilesWithCount().entrySet()) {
                ResourceProfile effectiveResourceProfile =
                        missingResource.getKey().equals(ResourceProfile.UNKNOWN)
                                ? defaultSlotResourceProfile
                                : missingResource.getKey();
                for (int i = 0; i < missingResource.getValue(); i++) {
                    if (!tryFulfillWithPendingResources(
                            effectiveResourceProfile, availableResources)) {
                        if (totalResourceProfile.allFieldsNoLessThan(effectiveResourceProfile)) {
                            numTaskExecutors += 1;
                            availableResources.incrementCount(
                                    totalResourceProfile.subtract(effectiveResourceProfile), 1);
                        } else {
                            hasUnfulfilled = true;
                            break;
                        }
                    }
                }
            }
            if (numTaskExecutors == 0) {
                allocationResults.put(jobId, Tuple2.of(Collections.emptyMap(), hasUnfulfilled));
            } else {
                allocationResults.put(
                        jobId,
                        Tuple2.of(
                                Collections.singletonMap(
                                        new PendingTaskManager(
                                                totalResourceProfile, defaultSlotResourceProfile),
                                        numTaskExecutors),
                                hasUnfulfilled));
            }
        }
        return allocationResults;
    }

    private boolean tryFulfillWithPendingResources(
            ResourceProfile resourceProfile, ResourceCounter availableResources) {
        Set<ResourceProfile> pendingTaskExecutorProfiles = availableResources.getResourceProfiles();

        // short-cut, pretty much only applicable to fine-grained resource management
        if (pendingTaskExecutorProfiles.contains(resourceProfile)) {
            availableResources.decrementCount(resourceProfile, 1);
            return true;
        }

        for (ResourceProfile pendingTaskExecutorProfile : pendingTaskExecutorProfiles) {
            if (pendingTaskExecutorProfile.allFieldsNoLessThan(resourceProfile)) {
                availableResources.decrementCount(pendingTaskExecutorProfile, 1);
                availableResources.incrementCount(
                        pendingTaskExecutorProfile.subtract(resourceProfile), 1);
                return true;
            }
        }

        return false;
    }
}
