package com.sufy.sdktest.object;

import com.sufy.sdk.auth.credentials.StaticCredentialsProvider;
import com.sufy.sdk.auth.credentials.SufyBasicCredentials;
import com.sufy.sdk.services.object.ObjectClient;
import com.sufy.sdk.services.object.model.DeleteObjectRequest;
import com.sufy.sdk.services.object.model.PutObjectRequest;
import com.sufy.sdktest.HttpClientRecorder;
import com.sufy.sdktest.TestConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class ObjectTestBase {
    protected TestConfig config;
    protected ObjectClient object;
    protected HttpClientRecorder recorder;

    @BeforeEach
    public void setup() throws IOException {
        this.recorder = new HttpClientRecorder(ApacheHttpClient.builder()
                .maxConnections(100)
                .connectionTimeout(Duration.ofSeconds(5))
                .build()
        );

        this.config = TestConfig.load();
        this.object = ObjectClient.builder()
                .region(Region.of(config.getRegion())) // 华东区 region id
                .endpointOverride(URI.create(config.getEndpoint()))
                .forcePathStyle(config.isForcePathStyle())
                .credentialsProvider(
                        StaticCredentialsProvider.create(
                                SufyBasicCredentials.create(
                                        config.getAccessKey(), config.getSecretKey()
                                )
                        )
                )
                .httpClient(recorder)
                .build();
    }

    @AfterEach
    public void teardown() {
        this.object.close();
    }

    protected void checkPublicRequestHeader(SdkHttpRequest request) {
        {
            assertTrue(request.headers().containsKey("Host"));
            String host = request.headers().get("Host").get(0);
            if (!config.isForcePathStyle()) {
                assertTrue(host.startsWith(config.getBucketName() + "."));
            }
        }
        {
            assertTrue(request.headers().containsKey("Authorization"));
            String auth = request.headers().get("Authorization").get(0);
            assertTrue(auth.startsWith("Sufy "));
        }
        {
            assertTrue(request.headers().containsKey("X-Sufy-Date"));
            String date = request.headers().get("X-Sufy-Date").get(0);
            // 判断时间格式是否为ISO8601格式
            // TODO: 20230413T012347Z 是ISO8601格式吗?
//            assertTrue(date.matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z"), date);
        }

        assertTrue(request.headers().get("User-Agent").get(0).startsWith("sufy-sdk-java/"));

        {
            // TODO: 这两个请求头是否需要改为sufy开头？没有在sufy sdk代码与服务定义文件中找到，
            //  猜测可能是aws sdk的http client依赖，或许需要使用拦截器修改这两个字段名
            //  amz-sdk-invocation-id: 28560793-74f0-7394-1fd3-addafac3045c
            //  amz-sdk-request: attempt=1; max=4
//            assertTrue(request.headers().containsKey("sufy-sdk-invocation-id"));
//            assertTrue(request.headers().containsKey("sufy-sdk-request"));
        }
    }

    protected void checkPublicResponseHeader(SdkHttpResponse response) {
        assertTrue(response.headers().containsKey("X-Sufy-Request-Id"));
        assertTrue(response.headers().containsKey("X-Reqid"));
        // TODO: 响应体缺少该字段
//        assertTrue(response.headers().containsKey("X-Sufy-Id"));
        {
            assertTrue(response.headers().containsKey("Date"));
            // 判断时间格式是否形如 Tue, 10 Jan 2023 16:02:15 GMT
            String date = response.headers().get("Date").get(0);
            assertTrue(date.matches("\\w{3}, \\d{2} \\w{3} \\d{4} \\d{2}:\\d{2}:\\d{2} GMT"));
        }
    }

    protected String getBucketName() {
        return config.getBucketName();
    }


    protected void forceDeleteBucket(String bucketName) {
        try {
            // TODO： 先删除所有对象
            object.deleteBucket(req -> req.bucket(bucketName).build());
        } catch (Exception e) {
            // ignore
        }
    }


    // 准备一个测试文件
    protected void prepareTestFile(String key, String content) {
        object.putObject(PutObjectRequest.builder()
                        .bucket(getBucketName())
                        .key(key)
                        .build(),
                RequestBody.fromString(content)
        );
    }

    // 删除一个测试文件
    protected void deleteTestFile(String key) {
        object.deleteObject(DeleteObjectRequest.builder()
                .bucket(getBucketName())
                .key(key)
                .build()
        );
    }


}
