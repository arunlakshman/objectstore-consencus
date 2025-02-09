package org.cloud.objectstore.consensus.api.data;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.util.Map;

/**
 * Class containing both metadata and content of an S3/GCS object
 */
@Getter
@RequiredArgsConstructor
@ToString
public class ObjectState {
    private final Map<String, String> metadata;
    private final String content;
    private final String eTag;
}
