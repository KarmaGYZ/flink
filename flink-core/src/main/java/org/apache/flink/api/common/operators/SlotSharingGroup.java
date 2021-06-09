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

package org.apache.flink.api.common.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.resources.ExternalResource;
import org.apache.flink.configuration.MemorySize;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Describe the name and the the different resource factors of a slot sharing group. */
@PublicEvolving
public class SlotSharingGroup implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String name;

    private final ResourceSpec resourceSpec;

    private SlotSharingGroup(String name, ResourceSpec resourceSpec) {
        this.name = checkNotNull(name);
        this.resourceSpec = checkNotNull(resourceSpec);
    }

    @Internal
    public SlotSharingGroup(String name) {
        this.name = checkNotNull(name);
        this.resourceSpec = ResourceSpec.UNKNOWN;
    }

    @Internal
    public String getName() {
        return name;
    }

    @Internal
    public ResourceSpec getResourceSpec() {
        return resourceSpec;
    }

    public static Builder newBuilder(String name) {
        return new Builder(name);
    }

    /** Builder for the {@link ResourceSpec}. */
    public static class Builder {

        private String name;
        private final ResourceSpec.Builder resourceSpecBuilder = ResourceSpec.newBuilder(0, 0);

        private Builder(String name) {
            this.name = name;
        }

        public Builder setCpuCores(double cpuCores) {
            resourceSpecBuilder.setCpuCores(cpuCores);
            return this;
        }

        public Builder setTaskHeapMemory(MemorySize taskHeapMemory) {
            resourceSpecBuilder.setTaskHeapMemory(taskHeapMemory);
            return this;
        }

        public Builder setTaskHeapMemoryMB(int taskHeapMemoryMB) {
            resourceSpecBuilder.setTaskHeapMemoryMB(taskHeapMemoryMB);
            return this;
        }

        public Builder setTaskOffHeapMemory(MemorySize taskOffHeapMemory) {
            resourceSpecBuilder.setTaskOffHeapMemory(taskOffHeapMemory);
            return this;
        }

        public Builder setOffTaskHeapMemoryMB(int taskOffHeapMemoryMB) {
            resourceSpecBuilder.setOffTaskHeapMemoryMB(taskOffHeapMemoryMB);
            return this;
        }

        public Builder setManagedMemory(MemorySize managedMemory) {
            resourceSpecBuilder.setManagedMemory(managedMemory);
            return this;
        }

        public Builder setManagedMemoryMB(int managedMemoryMB) {
            resourceSpecBuilder.setManagedMemoryMB(managedMemoryMB);
            return this;
        }

        /**
         * Add the given extended resource. The old value with the same resource name will be
         * replaced if present.
         */
        public Builder setExtendedResource(String name, double value) {
            resourceSpecBuilder.setExtendedResource(new ExternalResource(name, value));
            return this;
        }

        public SlotSharingGroup build() {
            return new SlotSharingGroup(name, resourceSpecBuilder.build());
        }
    }
}
