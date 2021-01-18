package org.apache.flink.streaming.connectors.elasticsearch;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.api.java.utils.ParameterTool;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class ElasticsearchWriter<IN, C extends AutoCloseable>
        implements SinkWriter<IN, Void, Void> {

    // ------------------------------------------------------------------------
    //  Internal bulk processor configuration
    // ------------------------------------------------------------------------

    public static final String CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS = "bulk.flush.max.actions";
    public static final String CONFIG_KEY_BULK_FLUSH_MAX_SIZE_MB = "bulk.flush.max.size.mb";
    public static final String CONFIG_KEY_BULK_FLUSH_INTERVAL_MS = "bulk.flush.interval.ms";
    public static final String CONFIG_KEY_BULK_FLUSH_BACKOFF_ENABLE = "bulk.flush.backoff.enable";
    public static final String CONFIG_KEY_BULK_FLUSH_BACKOFF_TYPE = "bulk.flush.backoff.type";
    public static final String CONFIG_KEY_BULK_FLUSH_BACKOFF_RETRIES = "bulk.flush.backoff.retries";
    public static final String CONFIG_KEY_BULK_FLUSH_BACKOFF_DELAY = "bulk.flush.backoff.delay";

    /** Used to control whether the retry delay should increase exponentially or remain constant. */
    @PublicEvolving
    public enum FlushBackoffType {
        CONSTANT,
        EXPONENTIAL
    }

    /**
     * Provides a backoff policy for bulk requests. Whenever a bulk request is rejected due to
     * resource constraints (i.e. the client's internal thread pool is full), the backoff policy
     * decides how long the bulk processor will wait before the operation is retried internally.
     *
     * <p>This is a proxy for version specific backoff policies.
     */
    public static class BulkFlushBackoffPolicy implements Serializable {

        private static final long serialVersionUID = -6022851996101826049L;

        // the default values follow the Elasticsearch default settings for BulkProcessor
        private FlushBackoffType backoffType = FlushBackoffType.EXPONENTIAL;
        private int maxRetryCount = 8;
        private long delayMillis = 50;

        public FlushBackoffType getBackoffType() {
            return backoffType;
        }

        public int getMaxRetryCount() {
            return maxRetryCount;
        }

        public long getDelayMillis() {
            return delayMillis;
        }

        public void setBackoffType(FlushBackoffType backoffType) {
            this.backoffType = checkNotNull(backoffType);
        }

        public void setMaxRetryCount(int maxRetryCount) {
            checkArgument(maxRetryCount >= 0);
            this.maxRetryCount = maxRetryCount;
        }

        public void setDelayMillis(long delayMillis) {
            checkArgument(delayMillis >= 0);
            this.delayMillis = delayMillis;
        }
    }

    // ------------------------------------------------------------------------
    //  User-facing API and configuration
    // ------------------------------------------------------------------------

    /**
     * The function that is used to construct multiple {@link ActionRequest ActionRequests} from
     * each incoming element.
     */
    private final ElasticsearchSinkFunction<IN> elasticsearchSinkFunction;

    /** User-provided handler for failed {@link ActionRequest ActionRequests}. */
    private final ActionRequestFailureHandler failureHandler;

    private final Sink.InitContext sinkContext;

    /** Call bridge for different version-specific. */
    private final ElasticsearchApiCallBridge<C> callBridge;

    /**
     * Provided to the user via the {@link ElasticsearchSinkFunction} to add {@link ActionRequest
     * ActionRequests}.
     */
    private final RequestIndexer requestIndexer;

    /**
     * Provided to the {@link ActionRequestFailureHandler} to allow users to re-index failed
     * requests.
     */
    private final BufferingNoOpRequestIndexer failureRequestIndexer;

    /**
     * Number of pending action requests not yet acknowledged by Elasticsearch. This value is
     * maintained only if flushOnCheckpoint is {@code true}.
     *
     * <p>This is incremented whenever the user adds (or re-adds through the {@link
     * ActionRequestFailureHandler}) requests to the {@link RequestIndexer}. It is decremented for
     * each completed request of a bulk request, in {@link BulkProcessor.Listener#afterBulk(long,
     * BulkRequest, BulkResponse)} and {@link BulkProcessor.Listener#afterBulk(long, BulkRequest,
     * Throwable)}.
     */
    private AtomicLong numPendingRequests = new AtomicLong(0);

    /** Elasticsearch client created using the call bridge. */
    private transient C client;

    /** Bulk processor to buffer and send requests to Elasticsearch, created using the client. */
    private transient BulkProcessor bulkProcessor;

    /**
     * If true, the producer will wait until all outstanding action requests have been sent to
     * Elasticsearch.
     */
    private final boolean flushOnCheckpoint;

    /**
     * This is set from inside the {@link BulkProcessor.Listener} if a {@link Throwable} was thrown
     * in callbacks and the user considered it should fail the sink via the {@link
     * ActionRequestFailureHandler#onFailure(ActionRequest, Throwable, int, RequestIndexer)} method.
     *
     * <p>Errors will be checked and rethrown before processing each input element, and when the
     * sink is closed.
     */
    private final AtomicReference<Throwable> failureThrowable = new AtomicReference<>();

    public ElasticsearchWriter(
            ElasticsearchApiCallBridge<C> callBridge,
            Sink.InitContext sinkContext,
            ElasticsearchSinkFunction<IN> elasticsearchSinkFunction,
            ActionRequestFailureHandler failureHandler,
            boolean flushOnCheckpoint,
            C client,
            Map<String, String> userConfig)
            throws IOException {
        this.elasticsearchSinkFunction = elasticsearchSinkFunction;
        this.failureHandler = failureHandler;
        this.sinkContext = sinkContext;
        this.callBridge = callBridge;

        this.bulkProcessor = buildBulkProcessor(new BulkProcessorListener(), client, userConfig);

        this.client = client;
        this.flushOnCheckpoint = flushOnCheckpoint;
        this.requestIndexer =
                callBridge.createBulkProcessorIndexer(
                        bulkProcessor, flushOnCheckpoint, numPendingRequests);
        this.failureRequestIndexer = new BufferingNoOpRequestIndexer();

        elasticsearchSinkFunction.open();
    }

    @Override
    public void write(IN element, Context context) throws IOException {
        checkAsyncErrorsAndRequests();
        elasticsearchSinkFunction.process(element, sinkContext, requestIndexer);
    }

    @Override
    public List<Void> prepareCommit(boolean flush) throws IOException {
        checkAsyncErrorsAndRequests();

        if (flushOnCheckpoint) {
            while (numPendingRequests.get() != 0) {
                bulkProcessor.flush();
                checkAsyncErrorsAndRequests();
            }
        }
        return Collections.emptyList();
    }

    @Override
    public List<Void> snapshotState() throws IOException {
        checkAsyncErrorsAndRequests();

        if (flushOnCheckpoint) {
            while (numPendingRequests.get() != 0) {
                bulkProcessor.flush();
                checkAsyncErrorsAndRequests();
            }
        }
        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {
        elasticsearchSinkFunction.close();
        if (bulkProcessor != null) {
            bulkProcessor.close();
            bulkProcessor = null;
        }

        if (client != null) {
            client.close();
            client = null;
        }

        callBridge.cleanup();

        // make sure any errors from callbacks are rethrown
        checkErrorAndRethrow();
    }

    private void setBulkConfigs(Map<String, String> userConfig) {}

    private void checkErrorAndRethrow() {
        Throwable cause = failureThrowable.get();
        if (cause != null) {
            throw new RuntimeException("An error occurred in ElasticsearchSink.", cause);
        }
    }

    private void checkAsyncErrorsAndRequests() {
        checkErrorAndRethrow();
        failureRequestIndexer.processBufferedRequests(requestIndexer);
    }

    @VisibleForTesting
    protected BulkProcessor buildBulkProcessor(
            BulkProcessor.Listener listener, C client, Map<String, String> userConfig) {
        checkNotNull(listener);
        checkNotNull(client);
        checkNotNull(userConfig);

        ParameterTool params = ParameterTool.fromMap(userConfig);

        Integer bulkProcessorFlushMaxActions;
        Integer bulkProcessorFlushMaxSizeMb;
        Long bulkProcessorFlushIntervalMillis;
        BulkFlushBackoffPolicy bulkProcessorFlushBackoffPolicy;

        if (params.has(CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS)) {
            bulkProcessorFlushMaxActions = params.getInt(CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS);
            userConfig.remove(CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS);
        } else {
            bulkProcessorFlushMaxActions = null;
        }

        if (params.has(CONFIG_KEY_BULK_FLUSH_MAX_SIZE_MB)) {
            bulkProcessorFlushMaxSizeMb = params.getInt(CONFIG_KEY_BULK_FLUSH_MAX_SIZE_MB);
            userConfig.remove(CONFIG_KEY_BULK_FLUSH_MAX_SIZE_MB);
        } else {
            bulkProcessorFlushMaxSizeMb = null;
        }

        if (params.has(CONFIG_KEY_BULK_FLUSH_INTERVAL_MS)) {
            bulkProcessorFlushIntervalMillis = params.getLong(CONFIG_KEY_BULK_FLUSH_INTERVAL_MS);
            userConfig.remove(CONFIG_KEY_BULK_FLUSH_INTERVAL_MS);
        } else {
            bulkProcessorFlushIntervalMillis = null;
        }

        boolean bulkProcessorFlushBackoffEnable =
                params.getBoolean(CONFIG_KEY_BULK_FLUSH_BACKOFF_ENABLE, true);
        userConfig.remove(CONFIG_KEY_BULK_FLUSH_BACKOFF_ENABLE);

        if (bulkProcessorFlushBackoffEnable) {
            bulkProcessorFlushBackoffPolicy = new BulkFlushBackoffPolicy();

            if (params.has(CONFIG_KEY_BULK_FLUSH_BACKOFF_TYPE)) {
                bulkProcessorFlushBackoffPolicy.setBackoffType(
                        FlushBackoffType.valueOf(params.get(CONFIG_KEY_BULK_FLUSH_BACKOFF_TYPE)));
                userConfig.remove(CONFIG_KEY_BULK_FLUSH_BACKOFF_TYPE);
            }

            if (params.has(CONFIG_KEY_BULK_FLUSH_BACKOFF_RETRIES)) {
                bulkProcessorFlushBackoffPolicy.setMaxRetryCount(
                        params.getInt(CONFIG_KEY_BULK_FLUSH_BACKOFF_RETRIES));
                userConfig.remove(CONFIG_KEY_BULK_FLUSH_BACKOFF_RETRIES);
            }

            if (params.has(CONFIG_KEY_BULK_FLUSH_BACKOFF_DELAY)) {
                bulkProcessorFlushBackoffPolicy.setDelayMillis(
                        params.getLong(CONFIG_KEY_BULK_FLUSH_BACKOFF_DELAY));
                userConfig.remove(CONFIG_KEY_BULK_FLUSH_BACKOFF_DELAY);
            }

        } else {
            bulkProcessorFlushBackoffPolicy = null;
        }

        BulkProcessor.Builder bulkProcessorBuilder =
                callBridge.createBulkProcessorBuilder(client, listener);

        // This makes flush() blocking
        bulkProcessorBuilder.setConcurrentRequests(0);

        if (bulkProcessorFlushMaxActions != null) {
            bulkProcessorBuilder.setBulkActions(bulkProcessorFlushMaxActions);
        }

        if (bulkProcessorFlushMaxSizeMb != null) {
            bulkProcessorBuilder.setBulkSize(
                    new ByteSizeValue(bulkProcessorFlushMaxSizeMb, ByteSizeUnit.MB));
        }

        if (bulkProcessorFlushIntervalMillis != null) {
            bulkProcessorBuilder.setFlushInterval(
                    TimeValue.timeValueMillis(bulkProcessorFlushIntervalMillis));
        }

        // if backoff retrying is disabled, bulkProcessorFlushBackoffPolicy will be null
        callBridge.configureBulkProcessorBackoff(
                bulkProcessorBuilder, bulkProcessorFlushBackoffPolicy);

        return bulkProcessorBuilder.build();
    }

    class BulkProcessorListener implements BulkProcessor.Listener {

        @Override
        public void beforeBulk(long executionId, BulkRequest request) {}

        @Override
        public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
            if (response.hasFailures()) {
                BulkItemResponse itemResponse;
                Throwable failure;
                RestStatus restStatus;
                DocWriteRequest actionRequest;

                try {
                    for (int i = 0; i < response.getItems().length; i++) {
                        itemResponse = response.getItems()[i];
                        failure = callBridge.extractFailureCauseFromBulkItemResponse(itemResponse);
                        if (failure != null) {
                            restStatus = itemResponse.getFailure().getStatus();
                            actionRequest = request.requests().get(i);
                            if (restStatus == null) {
                                if (actionRequest instanceof ActionRequest) {
                                    failureHandler.onFailure(
                                            (ActionRequest) actionRequest,
                                            failure,
                                            -1,
                                            failureRequestIndexer);
                                } else {
                                    throw new UnsupportedOperationException(
                                            "The sink currently only supports ActionRequests");
                                }
                            } else {
                                if (actionRequest instanceof ActionRequest) {
                                    failureHandler.onFailure(
                                            (ActionRequest) actionRequest,
                                            failure,
                                            restStatus.getStatus(),
                                            failureRequestIndexer);
                                } else {
                                    throw new UnsupportedOperationException(
                                            "The sink currently only supports ActionRequests");
                                }
                            }
                        }
                    }
                } catch (Throwable t) {
                    // fail the sink and skip the rest of the items
                    // if the failure handler decides to throw an exception
                    failureThrowable.compareAndSet(null, t);
                }
            }

            if (flushOnCheckpoint) {
                numPendingRequests.getAndAdd(-request.numberOfActions());
            }
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
            try {
                for (DocWriteRequest writeRequest : request.requests()) {
                    if (writeRequest instanceof ActionRequest) {
                        failureHandler.onFailure(
                                (ActionRequest) writeRequest, failure, -1, failureRequestIndexer);
                    } else {
                        throw new UnsupportedOperationException(
                                "The sink currently only supports ActionRequests");
                    }
                }
            } catch (Throwable t) {
                // fail the sink and skip the rest of the items
                // if the failure handler decides to throw an exception
                failureThrowable.compareAndSet(null, t);
            }

            if (flushOnCheckpoint) {
                numPendingRequests.getAndAdd(-request.numberOfActions());
            }
        }
    }

    @VisibleForTesting
    long getNumPendingRequests() {
        if (flushOnCheckpoint) {
            return numPendingRequests.get();
        } else {
            throw new UnsupportedOperationException(
                    "The number of pending requests is not maintained when flushing on checkpoint is disabled.");
        }
    }
}
