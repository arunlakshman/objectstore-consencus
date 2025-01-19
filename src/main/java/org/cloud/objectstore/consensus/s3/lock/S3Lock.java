package org.cloud.objectstore.consensus.s3.lock;

import lombok.RequiredArgsConstructor;
import org.cloud.objectstore.consensus.api.Lock;
import org.cloud.objectstore.consensus.common.leaderelection.LeaderElectionRecord;
import software.amazon.awssdk.services.s3.S3Client;

@RequiredArgsConstructor
public class S3Lock implements Lock {

    private final S3Client s3Client;

    @Override
    public LeaderElectionRecord get() {
        return null;
    }

    @Override
    public void create(LeaderElectionRecord leaderElectionRecord) {

    }

    @Override
    public void update(LeaderElectionRecord leaderElectionRecord) {

    }

    @Override
    public String identity() {
        return null;
    }

    @Override
    public String describe() {
        return null;
    }

    /*
    private void updateLeader(LeaderElectionRecord newRecord, String expectedEtag) {
        // Use copy operation to update metadata while preserving the object
        s3Client.copyObject(req -> req
                .copySource(bucketName + "/" + leaderKey)
                .destinationBucket(bucketName)
                .destinationKey(leaderKey)
                .metadata(metadataFromRecord(newRecord))
                .metadataDirective(MetadataDirective.REPLACE)
                .copySourceIfMatch(expectedEtag)
                .build());
    }
     */
}
