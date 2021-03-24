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

package org.apache.flink.externalresource.gpu;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.externalresource.ExternalResourceDriver;
import org.apache.flink.api.common.externalresource.ExternalResourceDriverConfiguration;
import org.apache.flink.api.common.externalresource.ExternalResourceInfo;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ExternalResourceOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * Driver takes the responsibility to discover GPU resources and provide the GPU resource
 * information. It retrieves the GPU information by executing a user-defined discovery script.
 */
class GPUDriver implements ExternalResourceDriver {

    private static final Logger LOG = LoggerFactory.getLogger(GPUDriver.class);

    private static final long DISCOVERY_SCRIPT_TIMEOUT_MS = 10000;

    @VisibleForTesting
    static final ConfigOption<String> DISCOVERY_SCRIPT_PATH =
            key("discovery-script.path")
                    .stringType()
                    .defaultValue(
                            String.format(
                                    "%s/external-resource-gpu/nvidia-gpu-discovery.sh",
                                    ConfigConstants.DEFAULT_FLINK_PLUGINS_DIRS));

    @VisibleForTesting
    static final ConfigOption<String> DISCOVERY_SCRIPT_ARG =
            key("discovery-script.args").stringType().noDefaultValue();

    /**
     * The mode which external resource is allocated between slots of the same TaskManager. Valid
     * options are SHARE and EXCLUSIVE.
     */
    @VisibleForTesting
    static final ConfigOption<AllocationMode> ALLOCATION_MODE =
            key("allocation-mode")
                    .enumType(AllocationMode.class)
                    .defaultValue(AllocationMode.SHARE);

    private final File discoveryScriptFile;
    private final String args;
    private final long totalAmount;
    private final AllocationMode allocationMode;
    private final Set<GPUInfo> gpuResources;
    private boolean initialized;

    GPUDriver(ExternalResourceDriverConfiguration config) throws Exception {
        Preconditions.checkArgument(
                config.getTotalAmount() > 0,
                "The gpuAmount should be positive when retrieving the GPU resource information.");
        this.totalAmount = config.getTotalAmount();
        this.allocationMode =
                Preconditions.checkNotNull(config.getParamConfigs().get(ALLOCATION_MODE));
        this.gpuResources = new HashSet<>();
        this.initialized = false;

        final String discoveryScriptPathStr =
                config.getParamConfigs().getString(DISCOVERY_SCRIPT_PATH);
        if (StringUtils.isNullOrWhitespaceOnly(discoveryScriptPathStr)) {
            throw new IllegalConfigurationException(
                    String.format(
                            "GPU discovery script ('%s') is not configured.",
                            ExternalResourceOptions.genericKeyWithSuffix(
                                    DISCOVERY_SCRIPT_PATH.key())));
        }

        Path discoveryScriptPath = Paths.get(discoveryScriptPathStr);
        if (!discoveryScriptPath.isAbsolute()) {
            discoveryScriptPath =
                    Paths.get(
                            System.getenv().getOrDefault(ConfigConstants.ENV_FLINK_HOME_DIR, "."),
                            discoveryScriptPathStr);
        }
        discoveryScriptFile = discoveryScriptPath.toFile();

        if (!discoveryScriptFile.exists()) {
            throw new FileNotFoundException(
                    String.format(
                            "The gpu discovery script does not exist in path %s.",
                            discoveryScriptFile.getAbsolutePath()));
        }
        if (!discoveryScriptFile.canExecute()) {
            throw new FlinkException(
                    String.format(
                            "The discovery script %s is not executable.",
                            discoveryScriptFile.getAbsolutePath()));
        }

        args = config.getParamConfigs().getString(DISCOVERY_SCRIPT_ARG);
    }

    @Override
    public Set<GPUInfo> retrieveResourceInfo(long gpuAmount) throws Exception {
        initialize();
        switch (allocationMode) {
            case SHARE:
                return Collections.unmodifiableSet(gpuResources);
            case EXCLUSIVE:
                Preconditions.checkArgument(
                        gpuAmount <= gpuResources.size(),
                        "Could not retrieve {} GPU devices, only {} available.",
                        gpuAmount,
                        gpuResources.size());

                final Set<GPUInfo> gpuInfos = new HashSet<>();
                final Iterator<GPUInfo> gpuResourcesItr = gpuResources.iterator();
                for (int i = 0; i < gpuAmount; ++i) {
                    gpuInfos.add(gpuResourcesItr.next());
                    gpuResourcesItr.remove();
                }
                LOG.info("Retrieve GPU resources: {}.", gpuInfos);
                return Collections.unmodifiableSet(gpuInfos);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported allocation mode " + allocationMode);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void releaseResources(Set<? extends ExternalResourceInfo> releasedResources) {
        Preconditions.checkState(initialized);
        switch (allocationMode) {
            case EXCLUSIVE:
                Preconditions.checkArgument(
                        releasedResources.size() + gpuResources.size() <= totalAmount);
                gpuResources.addAll((Collection<? extends GPUInfo>) releasedResources);
                LOG.info("Release GPU resources: {}.", releasedResources);
                break;
            case SHARE:
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported allocation mode " + allocationMode);
        }
    }

    private void initialize() throws Exception {
        if (initialized) {
            return;
        }

        String output = executeDiscoveryScript(discoveryScriptFile, totalAmount, args);
        if (!output.isEmpty()) {
            String[] indexes = output.split(",");
            for (String index : indexes) {
                if (!StringUtils.isNullOrWhitespaceOnly(index)) {
                    gpuResources.add(new GPUInfo(index.trim()));
                }
            }
        }
        LOG.info("Discover GPU resources: {}.", gpuResources);

        initialized = true;
    }

    private String executeDiscoveryScript(File discoveryScript, long gpuAmount, String args)
            throws Exception {
        final String cmd = discoveryScript.getAbsolutePath() + " " + gpuAmount + " " + args;
        final Process process = Runtime.getRuntime().exec(cmd);
        try (final BufferedReader stdoutReader =
                        new BufferedReader(
                                new InputStreamReader(
                                        process.getInputStream(), StandardCharsets.UTF_8));
                final BufferedReader stderrReader =
                        new BufferedReader(
                                new InputStreamReader(
                                        process.getErrorStream(), StandardCharsets.UTF_8))) {
            final boolean hasProcessTerminated =
                    process.waitFor(DISCOVERY_SCRIPT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            if (!hasProcessTerminated) {
                throw new TimeoutException(
                        String.format(
                                "The discovery script executed for over %d ms.",
                                DISCOVERY_SCRIPT_TIMEOUT_MS));
            }

            final int exitVal = process.exitValue();
            if (exitVal != 0) {
                final String stdout =
                        stdoutReader
                                .lines()
                                .collect(
                                        StringBuilder::new,
                                        StringBuilder::append,
                                        StringBuilder::append)
                                .toString();
                final String stderr =
                        stderrReader
                                .lines()
                                .collect(
                                        StringBuilder::new,
                                        StringBuilder::append,
                                        StringBuilder::append)
                                .toString();
                LOG.warn(
                        "Discovery script exit with {}.\nSTDOUT: {}\nSTDERR: {}",
                        exitVal,
                        stdout,
                        stderr);
                throw new FlinkException(
                        String.format(
                                "Discovery script exit with non-zero return code: %s.", exitVal));
            }
            Object[] stdout = stdoutReader.lines().toArray();
            if (stdout.length > 1) {
                LOG.warn(
                        "The output of the discovery script should only contain one single line. Finding {} lines with content: {}. Will only keep the first line.",
                        stdout.length,
                        Arrays.toString(stdout));
            }
            if (stdout.length == 0) {
                return "";
            }
            return (String) stdout[0];
        } finally {
            process.destroyForcibly();
        }
    }

    /** Allocation mode of external resources. */
    public enum AllocationMode {
        SHARE,
        EXCLUSIVE
    }
}
