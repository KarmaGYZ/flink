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

package org.apache.flink.streaming.connectors.elasticsearch;

import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.InstantiationUtil;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.client.Client;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class for all Flink Elasticsearch Sinks.
 *
 * <p>This class implements the common behaviour across Elasticsearch versions, such as the use of
 * an internal {@link BulkProcessor} to buffer multiple {@link ActionRequest}s before sending the
 * requests to the cluster, as well as passing input records to the user provided {@link
 * ElasticsearchSinkFunction} for processing.
 *
 * <p>The version specific API calls for different Elasticsearch versions should be defined by a
 * concrete implementation of a {@link ElasticsearchApiCallBridge}, which is provided to the
 * constructor of this class. This call bridge is used, for example, to create a Elasticsearch
 * {@link Client}, handle failed item responses, etc.
 *
 * @param <IN> Type of the elements handled by this sink
 * @param <C> Type of the Elasticsearch client, which implements {@link AutoCloseable}
 */
public class ElasticsearchSinkBase<IN, C extends AutoCloseable>
        implements Sink<IN, Void, Void, Void> {

    // ------------------------------------------------------------------------
    //  User-facing API and configuration
    // ------------------------------------------------------------------------

    /**
     * The config map that contains configuration for the bulk flushing behaviours.
     *
     * <p>For {@link org.elasticsearch.client.transport.TransportClient} based implementations, this
     * config map would also contain Elasticsearch-shipped configuration, and therefore this config
     * map would also be forwarded when creating the Elasticsearch client.
     */
    private final Map<String, String> userConfig;

    /**
     * The function that is used to construct multiple {@link ActionRequest ActionRequests} from
     * each incoming element.
     */
    private final ElasticsearchSinkFunction<IN> elasticsearchSinkFunction;

    /** User-provided handler for failed {@link ActionRequest ActionRequests}. */
    private final ActionRequestFailureHandler failureHandler;

    /**
     * If true, the producer will wait until all outstanding action requests have been sent to
     * Elasticsearch.
     */
    private boolean flushOnCheckpoint = true;

    // ------------------------------------------------------------------------
    //  Internals for the Flink Elasticsearch Sink
    // ------------------------------------------------------------------------

    /** Call bridge for different version-specific. */
    private final ElasticsearchApiCallBridge<C> callBridge;

    /**
     * This is set from inside the {@link BulkProcessor.Listener} if a {@link Throwable} was thrown
     * in callbacks and the user considered it should fail the sink via the {@link
     * ActionRequestFailureHandler#onFailure(ActionRequest, Throwable, int, RequestIndexer)} method.
     *
     * <p>Errors will be checked and rethrown before processing each input element, and when the
     * sink is closed.
     */
    private final AtomicReference<Throwable> failureThrowable = new AtomicReference<>();

    public ElasticsearchSinkBase(
            ElasticsearchApiCallBridge<C> callBridge,
            Map<String, String> userConfig,
            ElasticsearchSinkFunction<IN> elasticsearchSinkFunction,
            ActionRequestFailureHandler failureHandler) {

        this.callBridge = checkNotNull(callBridge);
        this.elasticsearchSinkFunction = checkNotNull(elasticsearchSinkFunction);
        this.failureHandler = checkNotNull(failureHandler);
        // we eagerly check if the user-provided sink function and failure handler is serializable;
        // otherwise, if they aren't serializable, users will merely get a non-informative error
        // message
        // "ElasticsearchSinkBase is not serializable"

        checkArgument(
                InstantiationUtil.isSerializable(elasticsearchSinkFunction),
                "The implementation of the provided ElasticsearchSinkFunction is not serializable. "
                        + "The object probably contains or references non-serializable fields.");

        checkArgument(
                InstantiationUtil.isSerializable(failureHandler),
                "The implementation of the provided ActionRequestFailureHandler is not serializable. "
                        + "The object probably contains or references non-serializable fields.");

        // extract and remove bulk processor related configuration from the user-provided config,
        // so that the resulting user config only contains configuration related to the
        // Elasticsearch client.

        checkNotNull(userConfig);

        this.userConfig = userConfig;
    }

    /**
     * Disable flushing on checkpoint. When disabled, the sink will not wait for all pending action
     * requests to be acknowledged by Elasticsearch on checkpoints.
     *
     * <p>NOTE: If flushing on checkpoint is disabled, the Flink Elasticsearch Sink does NOT provide
     * any strong guarantees for at-least-once delivery of action requests.
     */
    public void disableFlushOnCheckpoint() {
        this.flushOnCheckpoint = false;
    }

    @Override
    public SinkWriter<IN, Void, Void> createWriter(InitContext context, List<Void> states)
            throws IOException {
        C client = callBridge.createClient(userConfig);
        callBridge.verifyClientConnection(client);
        return new ElasticsearchWriter<>(
                callBridge,
                context,
                elasticsearchSinkFunction,
                failureHandler,
                flushOnCheckpoint,
                client,
                userConfig);
    }

    @Override
    public Optional<Committer<Void>> createCommitter() throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalCommitter<Void, Void>> createGlobalCommitter() throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getCommittableSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getGlobalCommittableSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getWriterStateSerializer() {
        return Optional.empty();
    }
}
