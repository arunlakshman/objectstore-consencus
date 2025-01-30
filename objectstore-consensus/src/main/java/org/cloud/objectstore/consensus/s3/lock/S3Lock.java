package org.cloud.objectstore.consensus.s3.lock;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.cloud.objectstore.consensus.common.leaderelection.LeaderElectionRecord;
import org.cloud.objectstore.consensus.common.lock.Lock;
import org.cloud.objectstore.consensus.exceptions.LeaderConflictWriteException;
import org.cloud.objectstore.consensus.exceptions.LeaderElectionException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.MetadataDirective;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.util.Map;
import java.util.Optional;

import static org.cloud.objectstore.consensus.common.leaderelection.LeaderElectionRecord.metadataFromRecord;
import static org.cloud.objectstore.consensus.common.leaderelection.LeaderElectionRecord.recordFromMetadata;

@Slf4j
@RequiredArgsConstructor
public class S3Lock implements Lock {

    private final S3Client s3Client;
    private final String bucketName;
    private final String leaderKey;
    private final String holderIdentity;

    @Override
    public Optional<LeaderElectionRecord> get() throws LeaderElectionException {
        try {
            log.info("Getting leader election record from bucket: {}, key: {}", bucketName, leaderKey);
            HeadObjectResponse headObjectResponse = s3Client.headObject(request -> request.bucket(bucketName)
                    .key(leaderKey)
                    .build());
            Map<String, String> metadata = headObjectResponse.metadata();
            log.info("Leader election record metadata: {}", metadata);
            return Optional.of(recordFromMetadata(metadata, headObjectResponse.eTag()));
        } catch (NoSuchKeyException e) {
            log.warn("No leader election record found", e);
            return Optional.empty();
        } catch (Exception e) {
            log.error("Error getting leader election record", e);
            throw new LeaderElectionException("Error getting leader election record", e);
        }
    }

    @Override
    public void create(LeaderElectionRecord leaderElectionRecord) throws LeaderConflictWriteException {
        try {
            log.info("Creating leader election record in bucket: {}, key: {}", bucketName, leaderKey);
            s3Client.putObject(request -> request.bucket(bucketName)
                    .key(leaderKey)
                    .ifNoneMatch("*")
                    .metadata(metadataFromRecord(leaderElectionRecord))
                    .build(), RequestBody.empty());
            log.info("Leader election record created successfully");
        } catch (S3Exception e) {
            if (e.statusCode() == 412) {
                log.error("Error creating leader election record: {}", e.getMessage());
                throw new LeaderConflictWriteException("Error creating leader election record", e);
            }
            log.error("S3Exception while creating leader election record", e);
            throw e;
        }
    }

    @Override
    public void update(String oldEtag, LeaderElectionRecord leaderElectionRecord) throws LeaderConflictWriteException {
        try {
            log.info("Updating leader election record in bucket: {}, key: {}", bucketName, leaderKey);
            s3Client.copyObject(request -> request
                    .sourceBucket(bucketName)
                    .sourceKey(leaderKey)
                    .destinationBucket(bucketName)
                    .destinationKey(leaderKey)
                    .metadata(metadataFromRecord(leaderElectionRecord))
                    .metadataDirective(MetadataDirective.REPLACE)
                    .copySourceIfMatch(oldEtag)
                    .build());
            log.info("Leader election record updated successfully");
        } catch (S3Exception e) {
            if (e.statusCode() == 412) {
                log.error("Error updating leader election record: {}", e.getMessage());
                throw new LeaderConflictWriteException("Error updating leader election record", e);
            }
            log.error("S3Exception while updating leader election record", e);
            throw e;
        }
    }

    @Override
    public String identity() {
        log.debug("Returning holder identity: {}", holderIdentity);
        return holderIdentity;
    }

    @Override
    public String describe() {
        String description = String.format("Lock: %s :: %s (%s)", bucketName, leaderKey, holderIdentity);
        log.info("Lock description: {}", description);
        return description;
    }
}