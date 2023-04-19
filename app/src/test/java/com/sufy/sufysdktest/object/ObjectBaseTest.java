package com.sufy.sufysdktest.object;

import com.sufy.config.ObjectConfig;
import com.sufy.config.ProxyConfig;
import com.sufy.config.TestConfig;
import com.sufy.sdk.auth.credentials.StaticCredentialsProvider;
import com.sufy.sdk.auth.credentials.SufyBasicCredentials;
import com.sufy.sdk.services.object.ObjectClient;
import com.sufy.sdk.services.object.model.*;
import com.sufy.util.HttpClientRecorder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.regions.Region;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class ObjectBaseTest {
    private ExecutorService threadPool;
    protected ObjectConfig config;
    protected ProxyConfig proxyConfig;
    protected ObjectClient object;
    protected HttpClientRecorder recorder;

    @BeforeEach
    public void setup() throws IOException {
        this.config = TestConfig.load().object;
        this.proxyConfig = TestConfig.load().proxy;

        ApacheHttpClient.Builder apacheHttpClientBuilder = ApacheHttpClient.builder()
                .maxConnections(100)
                .connectionTimeout(Duration.ofSeconds(5));
        if (proxyConfig != null) {
            apacheHttpClientBuilder.proxyConfiguration(ProxyConfiguration.builder()
                    .endpoint(
                            URI.create(
                                    proxyConfig.getType() + "://" + proxyConfig.getHost() + ":" + proxyConfig.getPort()
                            )
                    ).build()
            );
        }
        this.recorder = new HttpClientRecorder(apacheHttpClientBuilder.build());

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


    // 准备一个测试文件
    protected void prepareTestFile(String key, String content) {
        object.putObject(PutObjectRequest.builder()
                        .bucket(getBucketName())
                        .key(key)
                        .build(),
                RequestBody.fromString(content)
        );
    }

    protected void prepareAsyncEnv(int nThreads) {
        threadPool = Executors.newFixedThreadPool(nThreads);
    }

    protected void prepareAsyncEnv() {
        prepareAsyncEnv(10);
    }

    protected void submitAsyncTask(Runnable task) {
        threadPool.submit(task);
    }

    protected void awaitAllAsyncTasks() {
        threadPool.shutdown();
        try {
            boolean terminated;
            do {
                terminated = threadPool.awaitTermination(1, TimeUnit.SECONDS);
            } while (!terminated);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    // 删除一个测试文件
    protected void deleteTestFile(String key) {
        threadPool.submit(() -> {
            object.deleteObject(DeleteObjectRequest.builder()
                    .bucket(getBucketName())
                    .key(key)
                    .build()
            );
        });
    }

    // 清理测试环境所有文件
    protected void cleanAllFiles() {
        // 定义批量删除请求
        String bucketName = getBucketName();
        ListObjectsV2Request listReq = ListObjectsV2Request.builder().bucket(bucketName).build();
        ListObjectsV2Response listRes;
        prepareAsyncEnv();
        do {
            // 列出存储桶中的对象
            listRes = object.listObjectsV2(listReq);
            // 构建批量删除请求

            // 删除对象
            final ListObjectsV2Response finalListRes = listRes;
            submitAsyncTask(() -> object.deleteObjects(DeleteObjectsRequest.builder()
                    .bucket(bucketName)
                    .delete(Delete.builder()
                            .objects(finalListRes.contents().stream()
                                    .map(o -> ObjectIdentifier.builder().key(o.key()).build())
                                    .collect(Collectors.toList()))
                            .build()
                    )
                    .build()
            ));
            listReq = ListObjectsV2Request.builder()
                    .bucket(bucketName)
                    .continuationToken(listRes.nextContinuationToken())
                    .build();
        } while (listRes.isTruncated());
        awaitAllAsyncTasks();
    }

    protected void forceDeleteBucket(String bucketName) {
        try {
            cleanAllFiles();
            object.deleteBucket(req -> req.bucket(bucketName).build());
        } catch (Exception e) {
            // ignore
        }
    }

    protected void makeSureBucketExists() {
        try {
            object.createBucket(CreateBucketRequest.builder()
                    .bucket(getBucketName())
                    // aws要求LocationConstraint和Host里的Region信息一致，我们不要求，这样方便创建任意区域的空间
                    .createBucketConfiguration(CreateBucketConfiguration.builder()
                            .locationConstraint(config.getRegion())
                            .build()
                    )
                    .build()
            );
        } catch (Exception e) {
            // ignore
        }
    }

    @Test
    public void testClearAllFiles() {
        // 创建10个文件
        for (int i = 0; i < 10; i++) {
            prepareTestFile("test" + i, "test" + i);
        }

        cleanAllFiles();

        // 列举文件
        ListObjectsV2Response listRes = object.listObjectsV2(ListObjectsV2Request.builder()
                .bucket(getBucketName())
                .build()
        );

        // 判断是否为空
        assertTrue(listRes.contents().isEmpty());
    }
}
