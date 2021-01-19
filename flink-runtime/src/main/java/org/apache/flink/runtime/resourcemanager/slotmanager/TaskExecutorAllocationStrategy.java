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
import org.apache.flink.runtime.slots.ResourceCounter;

import java.util.Collections;
import java.util.Map;

/** Strategy how to allocate new task executors to fulfill the unfulfilled requirements. */
public interface TaskExecutorAllocationStrategy {
    TaskExecutorAllocationStrategy NO_OP_STRATEGY =
            (requirements, existingPendingResources) -> Collections.emptyMap();

    /**
     * Calculate {@link PendingTaskManager}s needed to fulfill the given requirements.
     *
     * @param requirements requirements indexed by jobId
     * @param existingPendingResources existing pending resources can be used to fulfill requirement
     * @return {@link PendingTaskManager}s needed and whether all the requirements can be fulfilled
     *     after allocation of pending task executors, indexed by jobId
     */
    Map<JobID, Tuple2<Map<PendingTaskManager, Integer>, Boolean>> getTaskExecutorsToFulfill(
            Map<JobID, ResourceCounter> requirements,
            Map<PendingTaskManager, Integer> existingPendingResources);
}
