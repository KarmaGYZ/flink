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

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;

/** Basic resource information of a TaskExecutor. */
public interface TaskExecutorInfo {

    /**
     * Get the available resource.
     *
     * @return the available resource
     */
    ResourceProfile getAvailableResource();

    /**
     * Get the total resource.
     *
     * @return the total resource
     */
    ResourceProfile getTotalResource();

    /**
     * Get the default slot resource profile.
     *
     * @return the default slot resource profile
     */
    ResourceProfile getDefaultSlotResourceProfile();

    /**
     * Get the number allocated slots.
     *
     * @return the number allocated slots
     */
    int getNumberAllocatedSlots();
}
