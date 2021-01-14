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

public class TestingTaskExecutorInfo implements TaskExecutorInfo {
    private final ResourceProfile totalResource;
    private final ResourceProfile availableResource;
    private final ResourceProfile defaultSlotResourceProfile;
    private final int numberAllocatedSlots;

    private TestingTaskExecutorInfo(
            ResourceProfile totalResource,
            ResourceProfile availableResource,
            ResourceProfile defaultSlotResourceProfile,
            int numberAllocatedSlots) {
        this.totalResource = totalResource;
        this.availableResource = availableResource;
        this.defaultSlotResourceProfile = defaultSlotResourceProfile;
        this.numberAllocatedSlots = numberAllocatedSlots;
    }

    @Override
    public ResourceProfile getTotalResource() {
        return totalResource;
    }

    @Override
    public ResourceProfile getDefaultSlotResourceProfile() {
        return defaultSlotResourceProfile;
    }

    @Override
    public ResourceProfile getAvailableResource() {
        return availableResource;
    }

    @Override
    public int getNumberAllocatedSlots() {
        return numberAllocatedSlots;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private ResourceProfile totalResource = ResourceProfile.ANY;
        private ResourceProfile availableResource = ResourceProfile.ANY;
        private ResourceProfile defaultSlotResourceProfile = ResourceProfile.ANY;
        private int numberAllocatedSlots = 0;

        private Builder() {};

        public Builder withTotalResource(ResourceProfile totalResource) {
            this.totalResource = totalResource;
            return this;
        }

        public Builder withAvailableResource(ResourceProfile availableResource) {
            this.availableResource = availableResource;
            return this;
        }

        public Builder withDefaultSlotResourceProfile(ResourceProfile defaultSlotResourceProfile) {
            this.defaultSlotResourceProfile = defaultSlotResourceProfile;
            return this;
        }

        public Builder withNumberAllocatedSlots(int numberAllocatedSlots) {
            this.numberAllocatedSlots = numberAllocatedSlots;
            return this;
        }

        public TestingTaskExecutorInfo build() {
            return new TestingTaskExecutorInfo(
                    totalResource,
                    availableResource,
                    defaultSlotResourceProfile,
                    numberAllocatedSlots);
        }
    }
}
