package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.slots.ResourceCounter;
import org.apache.flink.runtime.slots.ResourceRequirement;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class DefaultResourceAllocationStrategy implements ResourceAllocationStrategy {
    private ResourceProfile defaultSlotResourceProfile;
    private ResourceProfile totalResourceProfile;
    private PendingTaskManager defaultPendingTaskManager;

    @Override
    public ResourceAllocationResult checkResourceRequirements(
            Map<JobID, Collection<ResourceRequirement>> missingResources,
            Map<InstanceID, Tuple2<ResourceProfile, ResourceProfile>> registeredResources,
            List<PendingTaskManager> pendingTaskManagers) {

        final ResourceAllocationResult result = new ResourceAllocationResult();
        final Map<JobID, ResourceCounter> unfulfilledRequirements = new LinkedHashMap<>();
        for (Map.Entry<JobID, Collection<ResourceRequirement>> resourceRequirements :
                missingResources.entrySet()) {
            final JobID jobId = resourceRequirements.getKey();

            final ResourceCounter unfulfilledJobRequirements =
                    tryAllocateSlotsForJob(
                            jobId, resourceRequirements.getValue(), registeredResources, result);
            if (!unfulfilledJobRequirements.isEmpty()) {
                unfulfilledRequirements.put(jobId, unfulfilledJobRequirements);
            }
        }
        if (unfulfilledRequirements.isEmpty()) {
            return result;
        }

        tryFulfillRequirementsWithPendingResources(
                unfulfilledRequirements, pendingTaskManagers, result);
    }

    private ResourceCounter tryAllocateSlotsForJob(
            JobID jobId,
            Collection<ResourceRequirement> missingResources,
            Map<InstanceID, Tuple2<ResourceProfile, ResourceProfile>> registeredResources,
            ResourceAllocationResult result) {
        final ResourceCounter outstandingRequirements = new ResourceCounter();

        for (ResourceRequirement resourceRequirement : missingResources) {
            int numMissingRequirements =
                    internalTryAllocateSlots(
                            jobId, resourceRequirement, registeredResources, result);
            if (numMissingRequirements > 0) {
                outstandingRequirements.incrementCount(
                        resourceRequirement.getResourceProfile(), numMissingRequirements);
            }
        }
        return outstandingRequirements;
    }

    private int internalTryAllocateSlots(
            JobID jobId,
            ResourceRequirement resourceRequirement,
            Map<InstanceID, Tuple2<ResourceProfile, ResourceProfile>> registeredResources,
            ResourceAllocationResult result) {
        final ResourceProfile requiredResource = resourceRequirement.getResourceProfile();

        int numUnfulfilled = 0;
        for (int x = 0; x < resourceRequirement.getNumberOfRequiredSlots(); x++) {
            final Optional<InstanceID> matchedTaskExecutor =
                    findMatchingTaskExecutor(requiredResource, registeredResources);
            if (matchedTaskExecutor.isPresent()) {
                ResourceProfile effective =
                        requiredResource.equals(ResourceProfile.UNKNOWN)
                                ? registeredResources.get(matchedTaskExecutor.get()).f1
                                : requiredResource;
                result.allocationResult
                        .computeIfAbsent(jobId, jobID -> new HashMap<>())
                        .computeIfAbsent(matchedTaskExecutor.get(), id -> new ResourceCounter())
                        .incrementCount(effective, 1);
                registeredResources.compute(
                        matchedTaskExecutor.get(),
                        (id, tuple2) -> Tuple2.of(tuple2.f0.subtract(effective), tuple2.f1));
            } else {
                // exit loop early; we won't find a matching slot for this requirement
                int numRemaining = resourceRequirement.getNumberOfRequiredSlots() - x;
                numUnfulfilled += numRemaining;
                break;
            }
        }
        return numUnfulfilled;
    }

    public Optional<InstanceID> findMatchingTaskExecutor(
            ResourceProfile requirement,
            Map<InstanceID, Tuple2<ResourceProfile, ResourceProfile>> registeredResources) {
        return registeredResources.entrySet().stream()
                .filter(
                        taskExecutor ->
                                canFulfillRequirement(
                                        requirement.equals(ResourceProfile.UNKNOWN)
                                                ? taskExecutor.getValue().f1
                                                : requirement,
                                        taskExecutor.getValue().f0))
                .findFirst()
                .map(Map.Entry::getKey);
    }

    private boolean canFulfillRequirement(
            ResourceProfile requirement, ResourceProfile resourceProfile) {
        return resourceProfile.allFieldsNoLessThan(requirement);
    }

    private void tryFulfillRequirementsWithPendingResources(
            Map<JobID, ResourceCounter> unfulfilledRequirements,
            List<PendingTaskManager> pendingResources,
            ResourceAllocationResult result) {
        ResourceCounter availableResources = new ResourceCounter();
        availableResources.incrementCount(
                defaultPendingTaskManager.getTotalResourceProfile(), pendingResources.size());
        for (JobID jobId : unfulfilledRequirements.keySet()) {
            for (Map.Entry<ResourceProfile, Integer> missingResource :
                    unfulfilledRequirements.get(jobId).getResourceProfilesWithCount().entrySet()) {
                ResourceProfile effectiveResourceProfile =
                        missingResource.getKey().equals(ResourceProfile.UNKNOWN)
                                ? defaultSlotResourceProfile
                                : missingResource.getKey();
                for (int i = 0; i < missingResource.getValue(); i++) {
                    if (!tryFulfillWithPendingResources(
                            effectiveResourceProfile, availableResources)) {
                        if (totalResourceProfile.allFieldsNoLessThan(effectiveResourceProfile)) {
                            result.pendingTaskManagersToBeAllocated.add(defaultPendingTaskManager);
                            result.pendingTaskManagerJobMap.computeIfAbsent().add(jobId);
                            availableResources.incrementCount(
                                    totalResourceProfile.subtract(effectiveResourceProfile), 1);
                        } else {
                            result.canJobFulfill.put(jobId, false);
                            break;
                        }
                    }
                }
            }
        }
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
