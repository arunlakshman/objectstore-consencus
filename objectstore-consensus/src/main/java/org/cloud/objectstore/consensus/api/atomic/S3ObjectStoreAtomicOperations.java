package org.cloud.objectstore.consensus.api.atomic;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.cloud.objectstore.consensus.api.data.ObjectState;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.IOException;
import java.util.function.Function;

/**
 * Implementations of ObjectStoreAtomicOperations  on S3 object store.
 */
@RequiredArgsConstructor
@Slf4j
public class S3ObjectStoreAtomicOperations implements ObjectStoreAtomicOperations {

    private final S3Client s3Client;
    private final String bucketName;
    private final String keyName;

    @Override
    public boolean update(Function<ObjectState, ObjectState> updateFunction) {
        try {
            ObjectState objectState = getObjectState();
            ObjectState updatedObjectState = updateFunction.apply(objectState);

            PutObjectRequest.Builder requestBuilder = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(keyName)
                    .ifMatch(objectState.getETag())
                    .metadata(updatedObjectState.getMetadata());
            s3Client.putObject(requestBuilder.build(), RequestBody.fromString(updatedObjectState.getContent()));
        } catch (S3Exception e) {
            if (e.statusCode() == 412) {
                log.warn("Precondition failed for update operation");
                return false;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    private ObjectState getObjectState() throws IOException {
        GetObjectRequest.Builder requestBuilder = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(keyName);
        ResponseInputStream<GetObjectResponse> object = s3Client.getObject(requestBuilder.build());
        String content = new String(object.readAllBytes());
        return new ObjectState(object.response().metadata(),
                content,
                object.response().eTag());
    }
}
