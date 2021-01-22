package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.slots.ResourceCounter;
import org.apache.flink.runtime.slots.ResourceRequirement;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface ResourceAllocationStrategy {
    ResourceAllocationResult checkResourceRequirements(
            Map<JobID, Collection<ResourceRequirement>> missingResources,
            Map<InstanceID, Tuple2<ResourceProfile, ResourceProfile>> registeredResources,
            List<PendingTaskManager> pendingTaskManagers);

    class ResourceAllocationResult {
        Map<JobID, Boolean> canJobFulfill;
        Map<JobID, Map<InstanceID, ResourceCounter>> allocationResult;
        List<PendingTaskManager> pendingTaskManagersToBeAllocated;
        Map<PendingTaskManagerId, Set<JobID>> pendingTaskManagerJobMap;

        public List<PendingTaskManager> getPendingTaskManagersToBeAllocated() {
            return pendingTaskManagersToBeAllocated;
        }

        public Map<JobID, Boolean> getCanJobFulfill() {
            return canJobFulfill;
        }

        public Map<JobID, Map<InstanceID, ResourceCounter>> getAllocationResult() {
            return allocationResult;
        }

        public Map<PendingTaskManagerId, Set<JobID>> getPendingTaskManagerJobMap() {
            return pendingTaskManagerJobMap;
        }
    }
}
