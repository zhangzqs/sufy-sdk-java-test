package sufy.config;


public class ObjectConfig {
    public String bucketName;
    public String accessKey;
    public String secretKey;
    public String region;
    public String endpoint;
    public boolean forcePathStyle;


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
}