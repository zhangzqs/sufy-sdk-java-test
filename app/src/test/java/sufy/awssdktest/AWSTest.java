package sufy.awssdktest;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import sufy.config.ObjectConfig;
import sufy.config.ProxyConfig;
import sufy.config.TestConfig;

import java.net.URI;
import java.time.Duration;
import java.util.Map;

public class AWSTest {
    ObjectConfig config;
    ProxyConfig proxyConfig;
    S3Client s3;

    @BeforeEach
    public void setup() throws Exception {
        proxyConfig = TestConfig.load().proxy;
        config = TestConfig.load().object;
        s3 = S3Client.builder()
                .region(Region.of(config.getRegion()))
                .endpointOverride(URI.create(config.getEndpoint()))
                .forcePathStyle(config.isForcePathStyle())
                .credentialsProvider(
                        StaticCredentialsProvider.create(
                                AwsBasicCredentials.create(
                                        config.getAccessKey(), config.getSecretKey()
                                )
                        )
                )
                .httpClient(ApacheHttpClient.builder()
                        .maxConnections(100)
                        .connectionTimeout(Duration.ofSeconds(5))
                        .proxyConfiguration(ProxyConfiguration.builder()
                                .endpoint(
                                        URI.create(
                                                proxyConfig.getType() + "://" + proxyConfig.getHost() + ":" + proxyConfig.getPort()
                                        )
                                )
                                .build())
                        .build())
                .build();
    }

    @Test
    public void testListObject() {
        String prefix = "test-list-objects-";

        ListObjectsResponse listObjectsResponse1 = s3.listObjects(ListObjectsRequest.builder()
                .bucket(config.getBucketName())
                .prefix(prefix)
                .maxKeys(7)
                .build()
        );
        System.out.println(listObjectsResponse1.contents().get(0).lastModified());
    }

    @Test
    public void testHeadObject() {
        String key = "test-key1";
        Map<String, String> metadata = Map.of(
                "testKey1", "testValue1",
                "testKey2", "testValue2"
        );
        s3.putObject(
                PutObjectRequest.builder()
                        .bucket(config.getBucketName())
                        .key(key)
                        .metadata(metadata)
                        .build(),
                RequestBody.fromString("test")
        );

        HeadObjectResponse headObjectResponse = s3.headObject(
                HeadObjectRequest.builder()
                        .bucket(config.getBucketName())
                        .key(key)
                        .build()
        );
        headObjectResponse.metadata().forEach((k, v) -> System.out.println(k + " " + v));
        ResponseInputStream<GetObjectResponse> ri = s3.getObject(
                GetObjectRequest.builder()
                        .bucket(config.getBucketName())
                        .key(key)
                        .build()
        );
        ri.response().metadata().forEach((k, v) -> System.out.println(k + " " + v));
    }
}
