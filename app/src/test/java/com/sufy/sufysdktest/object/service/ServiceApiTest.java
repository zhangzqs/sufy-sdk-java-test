package com.sufy.sufysdktest.object.service;

import com.sufy.sdk.services.object.model.Bucket;
import com.sufy.sdk.services.object.model.ListBucketsRequest;
import com.sufy.sdk.services.object.model.ListBucketsResponse;
import com.sufy.sufysdktest.object.ObjectBaseTest;
import com.sufy.util.HttpClientRecorder;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;

import static org.junit.jupiter.api.Assertions.*;

public class ServiceApiTest extends ObjectBaseTest {

    @Test
    public void testListBuckets() {
        recorder.startRecording();
        {
            ListBucketsResponse listBucketsResponse = object.listBuckets(ListBucketsRequest.builder().build());
            // buckets
            {
                for (Bucket bucket : listBucketsResponse.buckets()) {
                    assertNotNull(bucket.name());
                    assertNotNull(bucket.creationDate());
                    // TODO: SDK 缺少该扩展字段定义
//                assertNotNull(bucket.locationConstraint());
                }
            }
            // owner
            {
                assertNotNull(listBucketsResponse.owner().id());
                assertNotNull(listBucketsResponse.owner().displayName());
            }
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
