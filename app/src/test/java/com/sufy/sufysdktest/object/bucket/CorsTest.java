package com.sufy.sufysdktest.object.bucket;

import com.sufy.sdk.services.object.model.*;
import com.sufy.sufysdktest.object.ObjectBaseTest;
import com.sufy.util.HttpClientRecorder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class CorsTest extends ObjectBaseTest {
    CORSConfiguration corsConfiguration;

    @BeforeEach
    public void setup() throws IOException {
        super.setup();
        corsConfiguration = CORSConfiguration.builder()
                .corsRules(List.of(
                        CORSRule.builder()
                                .id("test-cors-1")
                                .allowedOrigins("http://www.a.com")
                                .allowedMethods("GET", "POST")
                                .allowedHeaders("x-sufy-meta-abc", "x-sufy-meta-data")
                                .exposeHeaders("x-sufy-meta-abc", "x-sufy-meta-data")
                                .maxAgeSeconds(100)
                                .build()
                ))
                .build();
    }

    @Test
    public void testPutBucketCors() {
        makeSureBucketExists();
        recorder.startRecording();
        {
            object.putBucketCors(
                    PutBucketCorsRequest.builder()
                            .bucket(getBucketName())
                            .corsConfiguration(corsConfiguration)
                            .build()
            );
        }
        HttpClientRecorder.HttpRecord httpRecord = recorder.stopAndGetRecords().get(0);

        SdkHttpRequest request = httpRecord.request.httpRequest();
        checkPublicRequestHeader(request);
        assertEquals("PUT", request.method().name());
        assertEquals("/" + getBucketName(), request.encodedPath());
        assertEquals("cors", request.encodedQueryParameters().orElseThrow());
        assertTrue(request.headers().containsKey("Content-MD5")); // 必填项，base64编码的md5值
        assertEquals("application/json", request.headers().get("Content-Type").get(0));

        SdkHttpResponse response = httpRecord.response.httpResponse();
        assertEquals(200, response.statusCode());
        assertEquals("OK", response.statusText().orElseThrow());
    }

    @Test
    public void testGetBucketCors() {
        object.putBucketCors(PutBucketCorsRequest.builder()
                .bucket(getBucketName())
                .corsConfiguration(corsConfiguration)
                .build());
        recorder.startRecording();
        {
            GetBucketCorsResponse getBucketCorsResponse = object.getBucketCors(GetBucketCorsRequest.builder()
                    .bucket(getBucketName())
                    .build());
            assertTrue(getBucketCorsResponse.hasCorsRules());
            assertEquals(corsConfiguration.corsRules(), getBucketCorsResponse.corsRules());
        }
        HttpClientRecorder.HttpRecord httpRecord = recorder.stopAndGetRecords().get(0);
        SdkHttpRequest request = httpRecord.request.httpRequest();
        checkPublicRequestHeader(request);
        assertEquals("GET", request.method().name());
        Assertions.assertEquals("/" + getBucketName(), request.encodedPath());
        assertEquals("cors", request.encodedQueryParameters().orElseThrow());

        SdkHttpResponse response = httpRecord.response.httpResponse();
        checkPublicResponseHeader(response);
        assertEquals(200, response.statusCode());
        assertEquals("OK", response.statusText().orElseThrow());
    }

    @Test
    public void testGetBucketCorsWhenNoCors() {
        object.deleteBucketCors(DeleteBucketCorsRequest.builder().bucket(getBucketName()).build());
        recorder.startRecording();
        assertThrows(ObjectException.class, () -> {
            object.getBucketCors(GetBucketCorsRequest.builder().bucket(getBucketName()).build());
        });
        HttpClientRecorder.HttpRecord httpRecord = recorder.stopAndGetRecords().get(0);

        SdkHttpRequest request = httpRecord.request.httpRequest();
        checkPublicRequestHeader(request);
        assertEquals("GET", request.method().name());
        Assertions.assertEquals("/" + getBucketName(), request.encodedPath());
        assertEquals("cors", request.encodedQueryParameters().orElseThrow());

        SdkHttpResponse response = httpRecord.response.httpResponse();
        checkPublicResponseHeader(response);
        assertEquals(404, response.statusCode());
    }

    @Test
    public void testDeleteBucketCors() {
        recorder.startRecording();
        object.deleteBucketCors(DeleteBucketCorsRequest.builder().bucket(getBucketName()).build());
        HttpClientRecorder.HttpRecord httpRecord = recorder.stopAndGetRecords().get(0);

        SdkHttpRequest request = httpRecord.request.httpRequest();
        checkPublicRequestHeader(request);
        assertEquals("DELETE", request.method().name());
        Assertions.assertEquals("/" + getBucketName(), request.encodedPath());
        assertEquals("cors", request.encodedQueryParameters().orElseThrow());

        SdkHttpResponse response = httpRecord.response.httpResponse();
        checkPublicResponseHeader(response);
        assertEquals(204, response.statusCode());
        assertEquals("No Content", response.statusText().orElseThrow());
    }

}
