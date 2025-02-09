package org.cloud.objectstore.consensus.atomic;

import org.cloud.objectstore.consensus.api.data.ObjectState;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class S3ObjectStoreAtomicOperationsTest {

    private S3Client s3Client;
    private S3ObjectStoreAtomicOperations operations;
    private final String bucketName = "test-bucket";
    private final String keyName = "test-key";

    @BeforeEach
    void setUp() {
        s3Client = mock(S3Client.class);
        operations = new S3ObjectStoreAtomicOperations(s3Client, bucketName, keyName);
    }

    @Test
    void updateSuccessfully() throws IOException {
        ObjectState initialState = new ObjectState(new HashMap<>(), "initial content", "etag1");
        ObjectState updatedState = new ObjectState(new HashMap<>(), "updated content", "etag2");

        when(s3Client.getObject(any(GetObjectRequest.class)))
                .thenReturn(new ResponseInputStream<>(mock(GetObjectResponse.class), new ByteArrayInputStream("initial content".getBytes())));
        when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenReturn(null);

        boolean result = operations.update(state -> updatedState);

        Assertions.assertTrue(result);
        verify(s3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    }

    @Test
    void updateFailsDueToPreconditionFailed() throws IOException {
        ObjectState initialState = new ObjectState(new HashMap<>(), "initial content", "etag1");

        when(s3Client.getObject(any(GetObjectRequest.class)))
                .thenReturn(new ResponseInputStream<>(mock(GetObjectResponse.class), new ByteArrayInputStream("initial content".getBytes())));
        doThrow(S3Exception.builder().statusCode(412).build())
                .when(s3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));

        boolean result = operations.update(state -> initialState);

        Assertions.assertFalse(result);
        verify(s3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    }

    @Test
    void updateThrowsRuntimeExceptionOnOtherErrors() throws IOException {
        ObjectState initialState = new ObjectState(new HashMap<>(), "initial content", "etag1");

        when(s3Client.getObject(any(GetObjectRequest.class)))
                .thenReturn(new ResponseInputStream<>(mock(GetObjectResponse.class), new ByteArrayInputStream("initial content".getBytes())));
        doThrow(S3Exception.builder().statusCode(500).build())
                .when(s3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));

        Assertions.assertThrows(RuntimeException.class, () -> operations.update(state -> initialState));
    }

    @Test
    void getObjectStateSuccessfully() throws IOException {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("key", "value");
        GetObjectResponse response = mock(GetObjectResponse.class);
        when(response.metadata()).thenReturn(metadata);
        when(response.eTag()).thenReturn("etag1");

        when(s3Client.getObject(any(GetObjectRequest.class)))
                .thenReturn(new ResponseInputStream<>(response, new ByteArrayInputStream("content".getBytes())));

        ObjectState state = operations.getObjectState();

        Assertions.assertEquals("content", state.getContent());
        Assertions.assertEquals("etag1", state.getETag());
        Assertions.assertEquals(metadata, state.getMetadata());
    }
}