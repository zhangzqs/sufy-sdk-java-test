package com.sufy.sufysdktest.object.object;

import com.sufy.sdk.services.object.model.*;
import com.sufy.sufysdktest.object.ObjectBaseTest;
import com.sufy.util.HttpClientRecorder;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;

import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class ObjectPutAndGetTest extends ObjectBaseTest {

    @Test
    public void testPutObject() {
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
        Map<String, String> metadata = Map.ofEntries(
                Map.entry("test-key1", "test-value1"),
                Map.entry("test-key2", "test-value2")
        );
        String storageClass = "STANDARD";
        recorder.startRecording();
        {
            PutObjectResponse putObjectResponse = object.putObject(PutObjectRequest.builder()
                            .bucket(getBucketName())
                            .key(key)
                            .storageClass(storageClass)
                            .metadata(metadata)
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

        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            assertTrue(req.headers().containsKey("X-Sufy-Meta-" + entry.getKey()));
            assertEquals(req.headers().get("X-Sufy-Meta-" + entry.getKey()).get(0), entry.getValue());
        }

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
    public void testGetObject() throws IOException {
        String key = "testGetObject";
        String content = "HelloWorld";
        Map<String, String> metadata = Map.ofEntries(
                Map.entry("test-key1", "test-value1"),
                Map.entry("test-key2", "test-value2")
        );
        PutObjectResponse putObjectResponse = object.putObject(PutObjectRequest.builder()
                        .bucket(getBucketName())
                        .key(key)
                        .metadata(metadata)
                        .build(),
                RequestBody.fromString(content)
        );

        recorder.startRecording();
        {
            ResponseInputStream<GetObjectResponse> ris = object.getObject(GetObjectRequest.builder()
                    .bucket(getBucketName())
                    .key(key)
                    .build()
            );
            GetObjectResponse getObjectResponse = ris.response();
            assertNotNull(getObjectResponse);
            assertEquals(content.length(), getObjectResponse.contentLength());
            assertEquals(putObjectResponse.eTag(), getObjectResponse.eTag());

            for (Map.Entry<String, String> entry : getObjectResponse.metadata().entrySet()) {
                assertEquals(entry.getValue(), metadata.get(entry.getKey()));
            }
            assertNotNull(getObjectResponse.lastModified());
            assertEquals(content, new String(ris.readAllBytes()));
        }
        HttpClientRecorder.HttpRecord record = recorder.stopAndGetRecords().get(0);
        SdkHttpRequest req = record.request.httpRequest();
        SdkHttpResponse resp = record.response.httpResponse();

        checkPublicRequestHeader(req);
        assertEquals(SdkHttpMethod.GET, req.method());
        assertEquals(String.format("/%s/%s", getBucketName(), key), req.encodedPath());

        checkPublicResponseHeader(resp);
        assertEquals(200, resp.statusCode());
        assertEquals("OK", resp.statusText().orElseThrow());
        {
            assertTrue(resp.firstMatchingHeader("ETag").isPresent());
            assertEquals(resp.firstMatchingHeader("ETag").get(), putObjectResponse.eTag());
        }
        {
            assertTrue(resp.firstMatchingHeader("Last-Modified").isPresent());
            // 校验是否符合 RFC 822 时间标准
            String lastModified = resp.firstMatchingHeader("Last-Modified").get();
            assertTrue(lastModified.matches("\\w{3}, \\d{2} \\w{3} \\d{4} \\d{2}:\\d{2}:\\d{2} GMT"));
        }
        {
            assertTrue(resp.firstMatchingHeader("Content-Length").isPresent());
            assertEquals(resp.firstMatchingHeader("Content-Length").get(), String.valueOf(content.length()));
        }
        {
            assertTrue(resp.firstMatchingHeader("X-Sufy-Meta-test-key1").isPresent());
            assertTrue(resp.firstMatchingHeader("X-Sufy-Meta-test-key2").isPresent());
            assertEquals(resp.firstMatchingHeader("X-Sufy-Meta-test-key1").get(), metadata.get("test-key1"));
            assertEquals(resp.firstMatchingHeader("X-Sufy-Meta-test-key2").get(), metadata.get("test-key2"));
        }
    }

    @Test
    public void testHeadObject() {
        String key = "testKey1";
        String content = "HelloWorld";
        Map<String, String> metadata = Map.ofEntries(
                Map.entry("test-key1", "test-value1"),
                Map.entry("test-key2", "test-value2")
        );
        StorageClass storageClass = StorageClass.DEEP_ARCHIVE;
        PutObjectResponse putObjectResponse = object.putObject(PutObjectRequest.builder()
                        .bucket(getBucketName())
                        .key(key)
                        .storageClass(storageClass)
                        .metadata(metadata)
                        .build(),
                RequestBody.fromString(content)
        );

        recorder.startRecording();
        {
            HeadObjectResponse headBucketResponse = object.headObject(HeadObjectRequest.builder()
                    .bucket(getBucketName())
                    .key(key)
                    .build()
            );
            assertNotNull(headBucketResponse);
            assertEquals(content.length(), headBucketResponse.contentLength());
            assertEquals(putObjectResponse.eTag(), headBucketResponse.eTag());
            assertEquals(storageClass, headBucketResponse.storageClass());

            for (Map.Entry<String, String> entry : headBucketResponse.metadata().entrySet()) {
                assertEquals(entry.getValue(), metadata.get(entry.getKey()));
            }
            assertNotNull(headBucketResponse.lastModified());

        }
        HttpClientRecorder.HttpRecord record = recorder.stopAndGetRecords().get(0);
        SdkHttpRequest req = record.request.httpRequest();
        SdkHttpResponse resp = record.response.httpResponse();

        checkPublicRequestHeader(req);
        assertEquals(SdkHttpMethod.HEAD, req.method());
        assertEquals(String.format("/%s/%s", getBucketName(), key), req.encodedPath());

        checkPublicResponseHeader(resp);
        assertEquals(200, resp.statusCode());
        assertEquals("OK", resp.statusText().orElseThrow());
        {
            assertTrue(resp.firstMatchingHeader("ETag").isPresent());
            assertEquals(resp.firstMatchingHeader("ETag").get(), putObjectResponse.eTag());
        }
        {
            assertTrue(resp.firstMatchingHeader("Last-Modified").isPresent());
            // 校验是否符合 RFC 822 时间标准
            String lastModified = resp.firstMatchingHeader("Last-Modified").get();
            assertTrue(lastModified.matches("\\w{3}, \\d{2} \\w{3} \\d{4} \\d{2}:\\d{2}:\\d{2} GMT"));
        }
        {
            assertTrue(resp.firstMatchingHeader("Content-Length").isPresent());
            assertEquals(resp.firstMatchingHeader("Content-Length").get(), String.valueOf(content.length()));
        }
        {
            assertTrue(resp.firstMatchingHeader("X-Sufy-Storage-Class").isPresent());
            assertEquals(resp.firstMatchingHeader("X-Sufy-Storage-Class").get(), storageClass.toString());
        }
        {
            assertTrue(resp.firstMatchingHeader("X-Sufy-Meta-test-key1").isPresent());
            assertTrue(resp.firstMatchingHeader("X-Sufy-Meta-test-key2").isPresent());
            assertEquals(resp.firstMatchingHeader("X-Sufy-Meta-test-key1").get(), metadata.get("test-key1"));
            assertEquals(resp.firstMatchingHeader("X-Sufy-Meta-test-key2").get(), metadata.get("test-key2"));
        }
    }
}
