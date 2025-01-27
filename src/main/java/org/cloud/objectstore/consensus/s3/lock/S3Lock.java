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
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.util.Map;

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
    public LeaderElectionRecord get() throws LeaderElectionException {
        try {

            HeadObjectResponse headObjectResponse = s3Client.headObject(request -> request.bucket(bucketName)
                    .key(leaderKey)
                    .build());
            Map<String, String> metadata = headObjectResponse.metadata();
            return recordFromMetadata(metadata, headObjectResponse.eTag());
        } catch (Exception e) {
            throw new LeaderElectionException("Error getting leader election record", e);
        }
    }

    @Override
    public void create(LeaderElectionRecord leaderElectionRecord) throws LeaderConflictWriteException {
        try {
            // S3 PutObject empty file
            s3Client.putObject(request -> request.bucket(bucketName)
                    .key(leaderKey)
                    // If none match * then create
                    .ifNoneMatch("*")
                    .metadata(metadataFromRecord(leaderElectionRecord))
                    .build(), RequestBody.empty());
        } catch (S3Exception e) {
            // handle 412 PreconditionFailed, someone else updated the record
            if (e.statusCode() == 412) {
                log.error("Error updating leader election record: {}", e.getMessage());
                throw new LeaderConflictWriteException("Error updating leader election record", e);
            }
            throw e;
        }
    }

    @Override
    public void update(String oldEtag, LeaderElectionRecord leaderElectionRecord) throws LeaderConflictWriteException {
        try {
            // Use copy operation to update metadata while preserving the object
            s3Client.copyObject(request -> request
                    .sourceBucket(bucketName)
                    .sourceKey(leaderKey)
                    .destinationBucket(bucketName)
                    .destinationKey(leaderKey)
                    .metadata(metadataFromRecord(leaderElectionRecord))
                    .metadataDirective(MetadataDirective.REPLACE)
                    .copySourceIfMatch(oldEtag)
                    .build());

        } catch (S3Exception e) {
            // handle 412 PreconditionFailed, someone else updated the record
            if (e.statusCode() == 412) {
                log.error("Error updating leader election record: {}", e.getMessage());
                throw new LeaderConflictWriteException("Error updating leader election record", e);
            }
            throw e;
        }
    }

    @Override
    public String identity() {
        return holderIdentity;
    }

    @Override
    public String describe() {
        return String.format("Lock: %s :: %s (%s)", bucketName, leaderKey, holderIdentity);
    }
}
