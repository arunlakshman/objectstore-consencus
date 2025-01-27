package org.cloud.objectstore.consensus.s3;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class S3AccessConfig {

    private final String jitter;

}
