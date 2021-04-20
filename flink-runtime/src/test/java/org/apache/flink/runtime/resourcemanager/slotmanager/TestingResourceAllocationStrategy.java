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
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.Map;
import java.util.function.BiFunction;

/** Implementation of {@link ResourceAllocationStrategy} for testing purpose. */
public class TestingResourceAllocationStrategy implements ResourceAllocationStrategy {
    private final BiFunction<
                    Map<JobID, Collection<ResourceRequirement>>,
                    TaskManagerResourceInfoProvider,
                    ResourceAllocationResult>
            tryFulfillRequirementsFunction;
    private final BiFunction<Collection<TaskManagerInfo>, ResourceProfile, ResourceAllocationResult>
            checkIdleAndRedundantTaskManagersFunction;

    private TestingResourceAllocationStrategy(
            BiFunction<
                            Map<JobID, Collection<ResourceRequirement>>,
                            TaskManagerResourceInfoProvider,
                            ResourceAllocationResult>
                    tryFulfillRequirementsFunction,
            BiFunction<Collection<TaskManagerInfo>, ResourceProfile, ResourceAllocationResult>
                    checkIdleAndRedundantTaskManagersFunction) {
        this.tryFulfillRequirementsFunction =
                Preconditions.checkNotNull(tryFulfillRequirementsFunction);
        this.checkIdleAndRedundantTaskManagersFunction =
                Preconditions.checkNotNull(checkIdleAndRedundantTaskManagersFunction);
    }

    @Override
    public ResourceAllocationResult tryFulfillRequirements(
            Map<JobID, Collection<ResourceRequirement>> missingResources,
            TaskManagerResourceInfoProvider taskManagerResourceInfoProvider) {
        return tryFulfillRequirementsFunction.apply(
                missingResources, taskManagerResourceInfoProvider);
    }

    @Override
    public ResourceAllocationResult checkIdleAndRedundantTaskManagers(
            Collection<TaskManagerInfo> idleTaskManagers, ResourceProfile totalFreeResource) {
        return checkIdleAndRedundantTaskManagersFunction.apply(idleTaskManagers, totalFreeResource);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private BiFunction<
                        Map<JobID, Collection<ResourceRequirement>>,
                        TaskManagerResourceInfoProvider,
                        ResourceAllocationResult>
                tryFulfillRequirementsFunction =
                        (ignored0, ignored1) -> ResourceAllocationResult.builder().build();
        private BiFunction<Collection<TaskManagerInfo>, ResourceProfile, ResourceAllocationResult>
                checkIdleAndRedundantTaskManagersFunction =
                        (ignored0, ignored1) -> ResourceAllocationResult.builder().build();

        public Builder setTryFulfillRequirementsFunction(
                BiFunction<
                                Map<JobID, Collection<ResourceRequirement>>,
                                TaskManagerResourceInfoProvider,
                                ResourceAllocationResult>
                        tryFulfillRequirementsFunction) {
            this.tryFulfillRequirementsFunction = tryFulfillRequirementsFunction;
            return this;
        }

        public Builder setCheckIdleAndRedundantTaskManagersFunction(
                BiFunction<Collection<TaskManagerInfo>, ResourceProfile, ResourceAllocationResult>
                        checkIdleAndRedundantTaskManagersFunction) {
            this.checkIdleAndRedundantTaskManagersFunction =
                    checkIdleAndRedundantTaskManagersFunction;
            return this;
        }

        public TestingResourceAllocationStrategy build() {
            return new TestingResourceAllocationStrategy(
                    tryFulfillRequirementsFunction, checkIdleAndRedundantTaskManagersFunction);
        }
    }
}
