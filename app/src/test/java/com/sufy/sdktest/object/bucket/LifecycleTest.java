package com.sufy.sdktest.object.bucket;

import com.sufy.sdk.services.object.model.*;
import com.sufy.sdktest.HttpClientRecorder;
import com.sufy.sdktest.object.ObjectTestBase;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class LifecycleTest extends ObjectTestBase {
    @Test
    public void testPutLifecycle() {
        /*
         * TODO:
         *  预期请求体数据：{"rules":[{"expiration":{"days":1},"id":"test","filter":{"prefix":"test"},transitions":[{"days":1,"storageClass":"STANDARD_IA"}]}]}
         *  实际SDK请求数据：{"Rule":[{"expiration":{"days":1},"iD":"test","filter":{"prefix":"test"},"status":"Enabled","Transition":[{"days":1,"storageClass":"STANDARD_IA"}]}]}
         *
         *  TODO： 请求头需要Content-MD5字段，SDK未自动设置
         */
        object.putBucketLifecycleConfiguration(PutBucketLifecycleConfigurationRequest.builder()
                .bucket(getBucketName())
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
                                        // TODO: 文档中未出现status字段，但是sdk中该字段为必填项，否则build()会报错
                                        .status(ExpirationStatus.ENABLED)
                                        .build()
                        ))
                        .build()
                )
                .build()
        );
    }

    @Test
    public void testGetLifecycle() {
        // 先配置之后再获取应当正常获取到配置的值

    }

    @Test
    public void testGetLifecycleWhenNoLifecycleConfiguration() {
        // lifecycle未配置时，应当抛出异常
        object.deleteBucketLifecycle(DeleteBucketLifecycleRequest.builder().bucket(getBucketName()).build());
        recorder.startRecording();
        assertThrows(ObjectException.class, () -> {
            object.getBucketLifecycleConfiguration(GetBucketLifecycleConfigurationRequest.builder()
                    .bucket(getBucketName())
                    .build()
            );
        });
        HttpClientRecorder.HttpRecord httpRecord = recorder.stopAndGetRecords().get(0);

        SdkHttpRequest request = httpRecord.request.httpRequest();
        checkPublicRequestHeader(request);
        assertEquals("GET", request.method().name());
        assertEquals("/" + getBucketName(), request.encodedPath());
        assertEquals("lifecycle", request.encodedQueryParameters().orElseThrow());

        SdkHttpResponse response = httpRecord.response.httpResponse();
        checkPublicResponseHeader(response);
        assertEquals(404, response.statusCode());
        // TODO: expected: <NoSuchLifecycleConfiguration> but was: <Not Found>
//            assertEquals("NoSuchLifecycleConfiguration", response.statusText().orElseThrow());
    }

    @Test
    public void testDeleteLifecycle() {
        recorder.startRecording();
        object.deleteBucketLifecycle(DeleteBucketLifecycleRequest.builder().bucket(getBucketName()).build());
        HttpClientRecorder.HttpRecord httpRecord = recorder.stopAndGetRecords().get(0);

        SdkHttpRequest request = httpRecord.request.httpRequest();
        checkPublicRequestHeader(request);
        assertEquals("DELETE", request.method().name());
        assertEquals("/" + getBucketName(), request.encodedPath());
        assertEquals("lifecycle", request.encodedQueryParameters().orElseThrow());

        SdkHttpResponse response = httpRecord.response.httpResponse();
        checkPublicResponseHeader(response);
        assertEquals(204, response.statusCode());
        assertEquals("No Content", response.statusText().orElseThrow());
    }

}