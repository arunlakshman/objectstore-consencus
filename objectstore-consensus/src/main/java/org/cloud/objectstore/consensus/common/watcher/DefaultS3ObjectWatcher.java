package org.cloud.objectstore.consensus.common.watcher;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.cloud.objectstore.consensus.api.data.LeaderElectionConfig;
import org.cloud.objectstore.consensus.api.data.ObjectState;
import org.cloud.objectstore.consensus.api.data.ObjectWatcherConfig;
import org.cloud.objectstore.consensus.api.watcher.ObjectStoreWatcher;
import org.cloud.objectstore.consensus.api.watcher.WatchCallbackHandler;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

import static org.cloud.objectstore.consensus.common.utils.SchedulerUtils.scheduleWithVariableRate;

@Slf4j
@RequiredArgsConstructor
public class DefaultS3ObjectWatcher implements ObjectStoreWatcher {

    private final ObjectWatcherConfig watcherConfig;
    private final S3Client s3Client;
    private final String bucketName;
    private final String keyName;
    private final Executor executor;
    private final AtomicReference<ObjectState> observedObjectState = new AtomicReference<>();
    private WatchCallbackHandler handler;

    @Override
    public CompletableFuture<Void> startWatch(WatchCallbackHandler handler) {
        this.handler = handler;
        return watchS3ObjectV2(handler, executor);
    }

    private CompletableFuture<Void> watchS3ObjectV2(WatchCallbackHandler watchCallbackHandler, Executor executor) {

        return loop(completion -> {

            if (watchCallbackHandler == null) {
                log.error("WatchCallbackHandler is null");
                completion.completeExceptionally(new IllegalArgumentException("WatchCallbackHandler is null"));
                return;
            }
            ObjectState currentState = observedObjectState.get();
            log.info("Current state: {}", currentState);
            log.info("Etag : {}", Optional.ofNullable(currentState).map(ObjectState::getETag));
            try {
                ObjectState newState = getLatestObjectState(Optional.ofNullable(currentState).map(ObjectState::getETag));
                log.info("New Etag : {}", newState.getETag());
                if (currentState == null) {
                    // Object was created for the first time
                    observedObjectState.set(newState);
                    watchCallbackHandler.onAdded(newState);
                } else {
                    boolean etagMismatch = !currentState.getETag().equals(newState.getETag());
                    log.info("Etag mismatch: {}", etagMismatch);
                    if (etagMismatch) {
                        // Object already exists and it was modified
                        observedObjectState.set(newState);
                        watchCallbackHandler.onModified(newState);
                    } else {
                        // Object was not modified
                        log.info("Object not modified");
                    }
                }
            } catch (NoSuchKeyException e) {
                if (currentState == null) {
                    log.warn("Object is yet to be created");
                } else {
                    log.warn("Object deleted");
                    observedObjectState.set(null);
                    watchCallbackHandler.onDeleted(observedObjectState.get());
                }
            } catch (S3Exception e) {
                if (e.statusCode() != 412) {
                    //Object was not modified
                    log.info("Precondition failed. Object Etag was not modified");
                }
            } catch (Exception e) {
                log.warn("Exception", e);
                watchCallbackHandler.handleError(e);
            }
        }, () -> watcherConfig.getRetryPeriod().toMillis(), executor);
    }

    private ObjectState getLatestObjectState(Optional<String> oldEtag) throws IOException {
        GetObjectRequest.Builder requestBuilder = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(keyName);
        requestBuilder.ifNoneMatch(oldEtag.orElse(null));
        ResponseInputStream<GetObjectResponse> object = s3Client.getObject(requestBuilder.build());
        String content = new String(object.readAllBytes());
        return new ObjectState(object.response().metadata(),
                content,
                object.response().eTag());
    }

    protected static CompletableFuture<Void> loop(
            Consumer<CompletableFuture<?>> consumer,
            LongSupplier delaySupplier,
            Executor executor) {
        CompletableFuture<Void> completion = new CompletableFuture<>();
        scheduleWithVariableRate(completion, executor,
                () -> consumer.accept(completion), 0,
                delaySupplier,
                TimeUnit.MILLISECONDS);
        return completion;
    }
}
