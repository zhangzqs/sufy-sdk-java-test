package com.sufy.sdktest;

import com.sufy.sdk.auth.credentials.StaticCredentialsProvider;
import com.sufy.sdk.auth.credentials.SufyBasicCredentials;
import com.sufy.sdk.services.object.ObjectClient;
import com.sufy.sdk.services.object.model.*;
import org.apache.http.entity.ContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public class ObjectTest {
    private TestConfig config;
    private ObjectClient object;
    private HttpClientRecorder recorder;

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

    private void checkPublicRequestHeader(SdkHttpRequest request) {
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
            // TODO: 猜测可能是aws sdk的http client依赖，或许需要使用拦截器修改这两个字段名
            // TODO: amz-sdk-invocation-id: 28560793-74f0-7394-1fd3-addafac3045c
            // TODO: amz-sdk-request: attempt=1; max=4
//            assertTrue(request.headers().containsKey("sufy-sdk-invocation-id"));
//            assertTrue(request.headers().containsKey("sufy-sdk-request"));
        }
    }

    private void checkPublicResponseHeader(SdkHttpResponse response) {
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

    private String getBucketName() {
        return config.getBucketName();
    }

    @Nested
    class ServiceTest {
        @Test
        public void testListBuckets() throws Exception {
            recorder.startRecording();
            {
                ListBucketsResponse listBucketsResponse = object.listBuckets(ListBucketsRequest.builder().build());
                if (listBucketsResponse.buckets().size() > 0) {
                    Bucket bucket = listBucketsResponse.buckets().get(0);
                    assertNotNull(bucket.name());
                    assertNotNull(bucket.creationDate());

                    // TODO: SDK 缺少该扩展字段定义
//                assertNotNull(bucket.locationConstraint());
                }
                assertNotNull(listBucketsResponse.owner().id());
                assertNotNull(listBucketsResponse.owner().displayName());
            }
            HttpClientRecorder.HttpRecord record = recorder.stopAndGetRecords().get(0);

            // 请求头与响应头判定
            SdkHttpRequest req = record.request.httpRequest();
            SdkHttpResponse resp = record.response.httpResponse();

            checkPublicRequestHeader(req);
            req.encodedPath();
            assertEquals(SdkHttpMethod.GET, req.method());
            assertEquals("/", req.encodedPath());
            assertTrue(req.headers().containsKey("Content-Length"));
            assertTrue(req.headers().containsKey("Content-Type"));


            checkPublicResponseHeader(resp);
            assertEquals(200, resp.statusCode());
            assertEquals("OK", resp.statusText().orElseThrow());
        }
    }


    private void forceDeleteBucket(String bucketName) {
        try {
            // TODO： 先删除所有对象
            object.deleteBucket(req -> req.bucket(bucketName).build());
        } catch (Exception e) {
            // ignore
        }
    }

    @Nested
    class BucketManageTest {

        @Test
        public void testCreateBucket() {
            forceDeleteBucket(getBucketName());

            recorder.startRecording();
            {
                final CreateBucketResponse response = object.createBucket(CreateBucketRequest.builder()
                        .bucket(getBucketName())
                        // aws要求LocationConstraint和Host里的Region信息一致，我们不要求，这样方便创建任意区域的空间
                        .createBucketConfiguration(CreateBucketConfiguration.builder()
                                .locationConstraint(config.getRegion())
                                .build())
                        .build()
                );
                assertNotNull(response.location());
            }

            HttpClientRecorder.HttpRecord record = recorder.stopAndGetRecords().get(0);

            // 请求头与响应头判定
            SdkHttpRequest req = record.request.httpRequest();
            SdkHttpResponse resp = record.response.httpResponse();

            checkPublicRequestHeader(req);
            assertEquals(SdkHttpMethod.PUT, req.method());
            assertTrue(req.headers().containsKey("Content-Length"));
            assertTrue(req.headers().containsKey("Content-Type"));


            // 使用了ForcePathStyle的情况下，请求路径为 /{bucketName}
            assertEquals("/" + getBucketName(), req.encodedPath());

            checkPublicResponseHeader(resp);
            assertEquals(200, resp.statusCode());
            assertEquals("OK", resp.statusText().orElseThrow());
            assertTrue(resp.headers().containsKey("Location"));
        }


        @Test
        public void testHeadBucket() {
            recorder.startRecording();
            {
                HeadBucketResponse headBucketResponse = object.headBucket(HeadBucketRequest.builder()
                        .bucket(getBucketName())
                        .build()
                );
                // TODO: SDK 缺少该字段获取器
//            assertNotNull(headBucketResponse.region());
            }
            HttpClientRecorder.HttpRecord record = recorder.stopAndGetRecords().get(0);

            // 请求头与响应头判定
            SdkHttpRequest req = record.request.httpRequest();
            SdkHttpResponse resp = record.response.httpResponse();

            checkPublicRequestHeader(req);
            assertEquals(SdkHttpMethod.HEAD, req.method());
            assertEquals("/" + getBucketName(), req.encodedPath());

            checkPublicResponseHeader(resp);
            assertEquals(200, resp.statusCode());
            assertEquals("OK", resp.statusText().orElseThrow());
            assertTrue(resp.headers().containsKey("X-Sufy-Bucket-Region"));
            assertEquals(config.getRegion(), resp.headers().get("X-Sufy-Bucket-Region").get(0));
        }

        @Test
        public void testGetBucketLocation() {
            recorder.startRecording();
            {
                GetBucketLocationResponse response = object.getBucketLocation(GetBucketLocationRequest.builder()
                        .bucket(getBucketName())
                        .build()
                );
                // TODO: 返回null
                // TODO: 需要将区域内置到SDK中，否则不存在的regionId将返回null
//            BucketLocationConstraint blc = response.locationConstraint();
//            assertNotNull(blc);
//            assertEquals(config.getRegion(), response.locationConstraint().toString());
            }
            HttpClientRecorder.HttpRecord record = recorder.stopAndGetRecords().get(0);

            // 请求头与响应头判定
            SdkHttpRequest req = record.request.httpRequest();
            SdkHttpResponse resp = record.response.httpResponse();

            checkPublicRequestHeader(req);
            assertEquals(SdkHttpMethod.GET, req.method());
            assertEquals("/" + getBucketName(), req.encodedPath());
            assertEquals("location", req.encodedQueryParameters().orElseThrow());

            checkPublicResponseHeader(resp);
            assertEquals(200, resp.statusCode());
            assertEquals("OK", resp.statusText().orElseThrow());
        }

        @Test
        public void testDeleteBucket() {

            object.deleteBucket(DeleteBucketRequest.builder()
                    .bucket(getBucketName())
                    .build()
            );
        }
    }

    @Nested
    class ListObjectTest {
        @Test
        public void testListObjects() {
            String prefix = "test-list-objects-";
            List<String> keys = new ArrayList<>();
            // 准备 10 个文件
            for (int i = 0; i < 10; i++) {
                prepareTestFile(prefix + i, prefix + i);
                keys.add(prefix + i);
            }

            /*
             * TODO：此处报错如下
             *  Unable to unmarshall response (Unable to parse date : 2023-04-13T09:16:56.000Z).
             *  Response Code: 200, Response Text: OK
             *  这个时间来自于文件列表中文件的lastModified字段
             *  https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
             * */
            ListObjectsResponse listObjectsResponse1 = object.listObjects(ListObjectsRequest.builder()
                    .bucket(getBucketName())
                    .prefix(prefix)
                    .maxKeys(7)
                    .build()
            );
            assertEquals(7, listObjectsResponse1.contents().size());

            ListObjectsResponse listObjectsResponse2 = object.listObjects(ListObjectsRequest.builder()
                    .bucket(getBucketName())
                    .prefix(prefix)
                    .marker(listObjectsResponse1.nextMarker())
                    .maxKeys(7)
                    .build()
            );
            assertEquals(3, listObjectsResponse2.contents().size());

            // 删除所有文件
            keys.forEach(key -> {
                object.deleteObject(DeleteObjectRequest.builder()
                        .bucket(getBucketName())
                        .key(key)
                        .build()
                );
            });
        }

        @Test
        public void testListObjectsV2() {
            String prefix = "test-list-objects-v2-";
            List<String> keys = new ArrayList<>();
            // 准备 10 个文件
            for (int i = 0; i < 10; i++) {
                prepareTestFile(prefix + i, prefix + i);
                keys.add(prefix + i);
            }

            // TODO: Unable to unmarshall response (Unable to parse date : 2023-04-13T09:28:21.000Z).
            // TODO: Response Code: 200, Response Text: OK
            ListObjectsV2Response listObjectsV2Response1 = object.listObjectsV2(ListObjectsV2Request.builder()
                    .bucket(getBucketName())
                    .prefix(prefix)
                    .maxKeys(7)
                    .build()
            );
            assertEquals(7, listObjectsV2Response1.contents().size());

            ListObjectsV2Response listObjectsResponse2 = object.listObjectsV2(ListObjectsV2Request.builder()
                    .bucket(getBucketName())
                    .prefix(prefix)
                    .continuationToken(listObjectsV2Response1.nextContinuationToken())
                    .maxKeys(7)
                    .build()
            );
            assertEquals(3, listObjectsResponse2.contents().size());

            // 删除所有文件
            keys.forEach(key -> {
                object.deleteObject(DeleteObjectRequest.builder()
                        .bucket(getBucketName())
                        .key(key)
                        .build()
                );
            });
        }

    }


    @Nested
    class WebsiteTest {
        @Test
        public void testPutBucketWebsite() {
            // TODO: 服务端未实现
//            object.putBucketWebsite(req -> req.bucket(getBucketName()).build());
        }

        @Test
        public void testGetBucketWebsite() {
            // TODO: 服务端未实现
//            object.getBucketWebsite(req -> req.bucket(getBucketName()).build());
        }

        @Test
        public void testDeleteBucketWebsite() {
            // TODO: 服务端未实现
//            object.deleteBucketWebsite(req -> req.bucket(getBucketName()).build());
        }

    }

    @Nested
    class LifecycleTest {
        @Test
        public void testPutGetDeleteLifecycle() {
            object.putBucketLifecycleConfiguration(PutBucketLifecycleConfigurationRequest.builder()
                    .bucket(getBucketName())
                    .checksumAlgorithm("MD5")
                    .lifecycleConfiguration(BucketLifecycleConfiguration.builder()
                            .rules(List.of(
                                    LifecycleRule.builder()
                                            .id("test")
                                            .filter(LifecycleRuleFilter.builder()
                                                    .prefix("test")
                                                    .build()
                                            )
                                            .transitions(List.of(
                                                    Transition.builder()
                                                            .days(1)
                                                            .storageClass(TransitionStorageClass.STANDARD_IA)
                                                            .build()
                                            ))
                                            .expiration(LifecycleExpiration.builder()
                                                    .days(1)
                                                    .build()
                                            )
                                            .build()
                            ))
                            .build()
                    )
                    .build()
            );

            GetBucketLifecycleConfigurationResponse getBucketLifecycleConfigurationResponse = object.getBucketLifecycleConfiguration(
                    GetBucketLifecycleConfigurationRequest.builder()
                            .bucket(getBucketName())
                            .build()
            );
            LifecycleRule rule = getBucketLifecycleConfigurationResponse.rules().get(0);
            // TODO: SDK缺少 rule.creationDate() 扩展字段的获取方法

            // 删除
            object.deleteBucketLifecycle(DeleteBucketLifecycleRequest.builder()
                    .bucket(getBucketName())
                    .build()
            );
        }

    }


    @Nested
    class CorsTest {
        @Test
        public void testPutBucketCors() {
            object.putBucketCors(req -> req.bucket(getBucketName()).build());
        }

        @Test
        public void testGutBucketCors() {
            object.getBucketCors(req -> req.bucket(getBucketName()).build());
        }

        @Test
        public void testDeleteBucketCors() {
            object.deleteBucketCors(req -> req.bucket(getBucketName()).build());
        }

    }


    @Nested
    class PolicyTest {
        /**
         * 测试查看空间是否公开
         */
        @Test
        public void testGetBucketPolicyStatus() {
            // aws在没配置BucketPolicy时会报NoSuchBucketPolicy,对客户端不友好，我们返回200(isPublic值为false)
            recorder.startRecording();
            {
                GetBucketPolicyStatusResponse getBucketLocationResponse = object.getBucketPolicyStatus(GetBucketPolicyStatusRequest.builder()
                        .bucket(getBucketName())
                        .build()
                );
                assertNotNull(getBucketLocationResponse.policyStatus());
                // TODO: SDK 中 isPublic() 返回值类型为 Boolean 得到了一个null
                // TODO: 但是服务器端正常返回了isPublic值为false
//            assertFalse(getBucketLocationResponse.policyStatus().isPublic());
            }
            HttpClientRecorder.HttpRecord httpRecord = recorder.stopAndGetRecords().get(0);
            SdkHttpRequest request = httpRecord.request.httpRequest();
            SdkHttpResponse response = httpRecord.response.httpResponse();

            checkPublicRequestHeader(request);
            assertEquals("GET", request.method().name());
            assertEquals("/" + getBucketName(), request.encodedPath());
            assertEquals("policyStatus", request.encodedQueryParameters().orElseThrow());

            checkPublicResponseHeader(response);
            assertEquals(200, response.statusCode());
            assertEquals("OK", response.statusText().orElseThrow());
        }

        @Test
        public void testPutBucketPolicy() {
            object.putBucketPolicy(req -> req.bucket(getBucketName()).build());
        }

        @Test
        public void testGetBucketPolicy() {
            object.getBucketPolicy(req -> req.bucket(getBucketName()).build());
        }

        @Test
        public void testDeleteBucketPolicy() {
            object.deleteBucketPolicy(req -> req.bucket(getBucketName()).build());
        }

    }


    @Nested
    class TagTest {
        @Test
        public void testPutBucketTagging() {
            object.putBucketTagging(req -> req.bucket(getBucketName()).build());
        }

        @Test
        public void testGetBucketTagging() {
            object.getBucketTagging(req -> req.bucket(getBucketName()).build());
        }

        @Test
        public void testDeleteBucketTagging() {
            object.deleteBucketTagging(req -> req.bucket(getBucketName()).build());
        }

    }


    @Nested
    class BucketAclTest {
        @Test
        public void testPutBucketAcl() {
            object.putBucketAcl(req -> req.bucket(getBucketName()).build());
        }

        @Test
        public void testGetBucketAcl() {
            object.getBucketAcl(req -> req.bucket(getBucketName()).build());
        }

    }

    // 准备一个测试文件
    private void prepareTestFile(String key, String content) {
        object.putObject(PutObjectRequest.builder()
                        .bucket(getBucketName())
                        .key(key)
                        .build(),
                RequestBody.fromString(content)
        );
    }

    // 删除一个测试文件
    private void deleteTestFile(String key) {
        object.deleteObject(DeleteObjectRequest.builder()
                .bucket(getBucketName())
                .key(key)
                .build()
        );
    }

    @Nested
    class ObjectPutAndGet {
        @Test
        public void testPutObject() throws Exception {
            {
                // 不允许空key
                assertThrows(SdkClientException.class, () -> {
                    object.putObject(PutObjectRequest.builder()
                                    .bucket(getBucketName())
                                    .key("")
                                    .build(),
                            RequestBody.empty()
                    );
                });
            }
            String key = "testKey1";
            String content = "HelloWorld";
            recorder.startRecording();
            {
                PutObjectResponse putObjectResponse = object.putObject(PutObjectRequest.builder()
                                .bucket(getBucketName())
                                .key(key)
                                .storageClass("STANDARD")
                                .build(),
                        RequestBody.fromString(content)
                );
                assertNotNull(putObjectResponse);
                assertNotNull(putObjectResponse.eTag());
            }

            HttpClientRecorder.HttpRecord record = recorder.stopAndGetRecords().get(0);

            // 请求头与响应头判定
            SdkHttpRequest req = record.request.httpRequest();
            SdkHttpResponse resp = record.response.httpResponse();

            checkPublicRequestHeader(req);
            assertEquals(SdkHttpMethod.PUT, req.method());
            assertEquals(String.format("/%s/%s", getBucketName(), key), req.encodedPath());
            assertEquals(req.headers().get("Content-Length").get(0), String.valueOf(content.length()));

            if (req.headers().containsKey("X-Sufy-Storage-Class")) {
                assertEquals(req.headers().get("X-Sufy-Storage-Class").get(0), "STANDARD");
            }

            assertTrue(req.headers().containsKey("Content-Type"));
            // TODO: 缺少X-Sufy-Meta-前缀的请求头
            assertTrue(req.headers().containsKey("X-Sufy-Meta-" + key));

            checkPublicResponseHeader(resp);
            assertEquals(200, resp.statusCode());
            assertEquals("OK", resp.statusText().orElseThrow());
            assertTrue(resp.firstMatchingHeader("ETag").isPresent());
        }

        @Test
        public void testFormUpload() {
            // TODO: SDK未实现表单上传
        }

        @Test
        public void testGetObject() {
            object.getObject(req -> req.bucket(getBucketName()).build());
        }
    }

    @Nested
    class MultipartUploadTest {
        @Test
        public void testCreateMultipartUpload() {
            String key = "testCreateMultipartUploadFile";
            String contentType = ContentType.TEXT_PLAIN.getMimeType();
            recorder.startRecording();
            {
                // TODO: 使用Sufy签名分片上传时，报错如下
                //  {
                //      "code":"SignatureDoesNotMatch",
                //      "message":"The request signature we calculated does not match the signature you provided",
                //      "resource":"/kodo-s3apiv2-test-miku-sufy-java-sdk-test/testCreateMultipartUploadFile",
                //      "requestId":"BwAAAK73TASwaVUX"
                //  }
                object.createMultipartUpload(CreateMultipartUploadRequest.builder()
                        .key(key)
                        .bucket(getBucketName())
                        .contentType(contentType)
                        .build()
                );
            }
            HttpClientRecorder.HttpRecord record = recorder.stopAndGetRecords().get(0);

            // 请求头与响应头判定
            SdkHttpRequest req = record.request.httpRequest();
            SdkHttpResponse resp = record.response.httpResponse();

            checkPublicRequestHeader(req);
            assertEquals(SdkHttpMethod.POST, req.method());
            assertEquals(String.format("/%s/%s", getBucketName(), key), req.encodedPath());
            assertEquals(contentType, req.headers().get("Content-Type").get(0));

            checkPublicResponseHeader(resp);
            assertEquals(200, resp.statusCode());
            assertEquals("OK", resp.statusText().orElseThrow());
        }

        @Test
        public void testUploadPart() {
            object.uploadPart(req -> req.bucket(getBucketName()).build(), RequestBody.empty());
        }

        @Test
        public void testCompleteMultipartUpload() {
            object.completeMultipartUpload(req -> req.bucket(getBucketName()).build());
        }

        @Test
        public void testAbortMultipartUpload() {
            object.abortMultipartUpload(req -> req.bucket(getBucketName()).build());
        }

        /**
         * 列举文件级别正在进行的分片
         */
        @Test
        public void testListMultipartUploads() {
            object.listMultipartUploads(req -> req.bucket(getBucketName()).build());
        }

        /**
         * 列举bucket级别正在进行的分片
         */
        @Test
        public void testListParts() {
            object.listParts(req -> req.bucket(getBucketName()).build());
        }

    }


    @Nested
    class ObjectManage {

        @Test
        public void testCopyObject() {
            String srcKey = "testCopyObjectFileKeySrc";
            String destKey = "testCopyObjectFileKeyDest";
            /*
             * TODO:
             *  The request signature we calculated does not match the signature you provided
             *  (Service: Object, Status Code: 403, Request ID: null)
             *  com.sufy.sdk.services.object.model.ObjectException: The request signature we
             *  calculated does not match the signature you provided (Service: Object,
             *  Status Code: 403, Request ID: null)
             * */
            object.copyObject(CopyObjectRequest.builder()
                    .sourceBucket(getBucketName())
                    .destinationBucket(getBucketName())
                    .sourceKey(srcKey)
                    .destinationKey(destKey)
                    .build()
            );
        }

        @Test
        public void testDeleteObject() {
            String key = "testDeleteObjectFileKey";
            prepareTestFile(key, "");
            // TODO: 由于Head无法使用
            HeadObjectResponse headObjectResponse = object.headObject(HeadObjectRequest.builder()
                    .bucket(getBucketName())
                    .key(key)
                    .build()
            );
            assertNotNull(headObjectResponse);
            {
                object.deleteObject(DeleteObjectRequest.builder()
                        .bucket(getBucketName())
                        .build()
                );
            }
            assertThrows(NoSuchKeyException.class, () -> {
                object.headObject(HeadObjectRequest.builder()
                        .bucket(getBucketName())
                        .key(key)
                        .build()
                );
            });
        }

        @Test
        public void testDeleteObjects() {
            // TODO: 批量删除操作未删除成功
            // TODO: SDK发出请求的请求体JSON中的key为Object而文档中为objects
            // TODO: 服务器端响应200但是返回的JSON中的deleted字段为空列表
            int nums = 10;
            // 准备空文件
            List<String> keys = new ArrayList<>();
            for (int i = 0; i < nums; i++) {
                String key = "testDeleteObjectsFileKey" + i;
                prepareTestFile(key, "");
                keys.add(key);
            }
            // 删除空文件
            DeleteObjectsResponse deleteObjectsResponse = object.deleteObjects(DeleteObjectsRequest.builder()
                    .bucket(getBucketName())
                    .delete(Delete.builder()
                            .objects(keys.stream()
                                    .map(key -> ObjectIdentifier.builder()
                                            .key(key)
                                            .build()
                                    ).collect(Collectors.toList())
                            )
                            .quiet(false)
                            .build())
                    .build()
            );
            assertNotNull(deleteObjectsResponse);
            assertEquals(nums, deleteObjectsResponse.deleted().size());

            // 验证这些文件不存在了
            for (String key : keys) {
                assertThrows(NoSuchKeyException.class, () -> {
                    object.headObject(HeadObjectRequest.builder()
                            .bucket(getBucketName())
                            .key(key)
                            .build()
                    );
                });
            }
        }

        @Test
        public void testHeadObject() {
            String key = "testHeadObjectFileKey";
            String content = "HelloWorld";
            prepareTestFile(key, content);

            // TODO: headObject 无法正常反序列化响应内容实体
            // TODO: Unable to unmarshall response (No marshaller/unmarshaller of type Map registered for location HEADER.).
            // TODO: Response Code: 200, Response Text: OK
            recorder.startRecording();
            HeadObjectResponse headBucketResponse = object.headObject(HeadObjectRequest.builder()
                    .bucket(getBucketName())
                    .key(key)
                    .build()
            );
            HttpClientRecorder.HttpRecord record = recorder.stopAndGetRecords().get(0);
            assertNotNull(headBucketResponse);
            assertEquals(content.length(), headBucketResponse.contentLength());

            deleteTestFile(key);
        }

        @Test
        public void testRestoreObject() {
            RestoreObjectResponse restoreObjectResponse = object.restoreObject(RestoreObjectRequest.builder()
                    .bucket(getBucketName())
                    .build()
            );
        }

    }

    @Nested
    class ObjectAclTest {
        private static final String KEY = "testObjectAclFileKey";

        // Sufy 签名的两个object acl接口，均返回 501 Not Implemented
        @Test
        public void testGetObjectAcl() {
            assertThrows(ObjectException.class, () -> {
                try {
                    object.getObjectAcl(GetObjectAclRequest.builder()
                            .bucket(getBucketName())
                            .key(KEY)
                            .build()
                    );
                } catch (ObjectException e) {
                    assertEquals(501, e.statusCode());
                    throw e;
                }
            });
        }

        @Test
        public void testPutObjectAcl() {
            assertThrows(ObjectException.class, () -> {
                try {
                    object.putObjectAcl(PutObjectAclRequest.builder()
                            .bucket(getBucketName())
                            .key(KEY)
                            .build()
                    );
                } catch (ObjectException e) {
                    assertEquals(501, e.statusCode());
                    throw e;
                }
            });
        }
    }
}
