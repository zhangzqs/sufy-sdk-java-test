package com.sufy.sdktest;

import com.google.gson.Gson;

import java.io.IOException;
import java.io.InputStream;

public class TestConfig {
    private String bucketName;
    private String accessKey;
    private String secretKey;
    private String region;
    private String endpoint;
    private boolean forcePathStyle;


    public String getBucketName() {
        return bucketName;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public String getRegion() {
        return region;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public boolean isForcePathStyle() {
        return forcePathStyle;
    }


    public static TestConfig load() throws IOException {
        try (InputStream is = TestConfig.class.getResourceAsStream("/test-config.json")) {
            Gson gson = new Gson();
            assert is != null;
            return gson.fromJson(new String(is.readAllBytes()), TestConfig.class);
        }
    }
}
