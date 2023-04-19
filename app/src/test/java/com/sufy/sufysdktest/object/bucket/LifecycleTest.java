package com.sufy.sufysdktest.object.bucket;

import com.sufy.sdk.services.object.model.*;
import com.sufy.sufysdktest.object.ObjectBaseTest;
import com.sufy.util.HttpClientRecorder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class LifecycleTest extends ObjectBaseTest {
    BucketLifecycleConfiguration lifecycleConfiguration;

    @BeforeEach
    public void setup() throws IOException {
        super.setup();
        lifecycleConfiguration = BucketLifecycleConfiguration.builder()
                .rules(List.of(
                        LifecycleRule.builder()
                                .id("test")
                                .filter(LifecycleRuleFilter.builder().prefix("test").build())
                                .transitions(List.of(
                                        Transition.builder()
                                                .days(1)
                                                .storageClass(TransitionStorageClass.DEEP_ARCHIVE)
                                                .build()
                                ))
                                .expiration(LifecycleExpiration.builder()
                                        .days(3)
                                        .build()
                                )
                                .status(ExpirationStatus.ENABLED)
                                .build()
                ))
                .build();
    }

    @Test
    public void testPutLifecycle() {
        recorder.startRecording();
        {
            PutBucketLifecycleConfigurationResponse putResponse = object.putBucketLifecycleConfiguration(
                    PutBucketLifecycleConfigurationRequest.builder()
                            .bucket(getBucketName())
                            .lifecycleConfiguration(lifecycleConfiguration)
                            .build()
            );
            assertNotNull(putResponse);
        }
        HttpClientRecorder.HttpRecord httpRecord = recorder.stopAndGetRecords().get(0);
        SdkHttpRequest request = httpRecord.request.httpRequest();
        checkPublicRequestHeader(request);
        assertEquals(SdkHttpMethod.PUT, request.method());
        assertEquals("/" + getBucketName(), request.encodedPath());
        assertEquals("lifecycle", request.encodedQueryParameters().orElseThrow());

        SdkHttpResponse response = httpRecord.response.httpResponse();
        checkPublicResponseHeader(response);
        assertEquals(200, response.statusCode());
        assertEquals("OK", response.statusText().orElseThrow());
    }

    @Test
    public void testGetLifecycle() {
        // 先删除原有的lifecycle配置
        object.deleteBucketLifecycle(DeleteBucketLifecycleRequest.builder().bucket(getBucketName()).build());
        // 再设置新的lifecycle配置
        object.putBucketLifecycleConfiguration(PutBucketLifecycleConfigurationRequest.builder()
                .bucket(getBucketName())
                .lifecycleConfiguration(lifecycleConfiguration)
                .build()
        );
        // 获取lifecycle配置
        {
            GetBucketLifecycleConfigurationResponse response = object.getBucketLifecycleConfiguration(
                    GetBucketLifecycleConfigurationRequest.builder()
                            .bucket(getBucketName())
                            .build());
            assertEquals(lifecycleConfiguration.rules().size(), response.rules().size());
            LifecycleRule expectRule = lifecycleConfiguration.rules().get(0);
            LifecycleRule actualRule = response.rules().get(0);
            assertEquals(expectRule.id(), actualRule.id());
            assertEquals(expectRule.filter().prefix(), actualRule.filter().prefix());
            assertEquals(expectRule.expiration().days(), actualRule.expiration().days());
            // TODO: 服务端还不支持 Status: Enable 字段，这里会 null
//            assertEquals(expectRule.status(), actualRule.status());
            {
                assertEquals(expectRule.transitions().size(), actualRule.transitions().size());
                Transition expectTransition = expectRule.transitions().get(0);
                Transition actualTransition = actualRule.transitions().get(0);
                assertEquals(expectTransition.days(), actualTransition.days());
                assertEquals(expectTransition.storageClass(), actualTransition.storageClass());
            }
        }
    }


    @Test
    public void testGetLifecycleWhenNoLifecycleConfiguration() {
        // lifecycle未配置时，应当抛出异常
        object.deleteBucketLifecycle(DeleteBucketLifecycleRequest.builder().bucket(getBucketName()).build());
        recorder.startRecording();
        {
            assertThrows(ObjectException.class, () -> {
                object.getBucketLifecycleConfiguration(GetBucketLifecycleConfigurationRequest.builder()
                        .bucket(getBucketName())
                        .build()
                );
            });
        }
        HttpClientRecorder.HttpRecord httpRecord = recorder.stopAndGetRecords().get(0);

        SdkHttpRequest request = httpRecord.request.httpRequest();
        checkPublicRequestHeader(request);
        assertEquals("GET", request.method().name());
        assertEquals("/" + getBucketName(), request.encodedPath());
        assertEquals("lifecycle", request.encodedQueryParameters().orElseThrow());

        SdkHttpResponse response = httpRecord.response.httpResponse();
        checkPublicResponseHeader(response);
        assertEquals(404, response.statusCode());
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