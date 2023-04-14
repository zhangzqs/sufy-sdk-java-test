package com.sufy.sdktest.object.object;

import com.sufy.sdk.services.object.model.PutObjectRequest;
import com.sufy.sdk.services.object.model.PutObjectResponse;
import com.sufy.sdktest.HttpClientRecorder;
import com.sufy.sdktest.object.ObjectTestBase;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;

import static org.junit.jupiter.api.Assertions.*;

public class ObjectPutAndGetTest extends ObjectTestBase {

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
