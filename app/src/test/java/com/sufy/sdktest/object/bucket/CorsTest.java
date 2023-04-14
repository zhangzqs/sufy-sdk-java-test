package com.sufy.sdktest.object.bucket;

import com.sufy.sdk.services.object.model.*;
import com.sufy.sdktest.HttpClientRecorder;
import com.sufy.sdktest.object.ObjectTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CorsTest extends ObjectTestBase {
    @Test
    public void testPutBucketCors() {
        /*
         * TODO:
         *  SDK发出的实际请求："{"CORSRule":[{"iD":"test-cors-1","AllowedMethod":["GET","POST"],"AllowedOrigin":["http://www.a.com"]}]}"
         *  预期的请求体数据：{"CORSRules":[{"id":"test-cors-1","allowedOrigins":["GET","POST"],"allowedOrigins":["http://www.a.com"]}]}"
         *  猜测这些字段的不一致可能是由于沿袭了XML字段命名规则导致的
         */
        object.putBucketCors(PutBucketCorsRequest.builder()
                .bucket(getBucketName())
                .corsConfiguration(CORSConfiguration.builder()
                        .corsRules(List.of(
                                CORSRule.builder()
                                        .id("test-cors-1")
                                        .allowedMethods("GET", "POST")
                                        .allowedOrigins("http://www.a.com")
                                        .build()
                        ))
                        .build()
                )
                // .contentMD5("") 该字段自动完成了计算，无需手动填写
                .build()
        );
    }

    @Test
    public void testGetBucketCors() {
        object.getBucketCors(GetBucketCorsRequest.builder().bucket(getBucketName()).build());
    }

    @Test
    public void testGutBucketCorsWhenNoCors() {
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

        // TODO: expected: <NoSuchCORSConfiguration> but was: <Not Found>
//            assertEquals("NoSuchCORSConfiguration", response.statusText().orElseThrow());
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
