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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

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
                    assertNotNull(bucket.locationConstraintAsString());
                    // TODO: 未内置region获得到null
//                    assertNotNull(bucket.locationConstraint());
                    if (bucket.name().equals(getBucketName())) {
                        assertEquals(config.getRegion(), bucket.locationConstraintAsString());
                    }
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

        checkPublicResponseHeader(resp);
        assertEquals(200, resp.statusCode());
        assertEquals("OK", resp.statusText().orElseThrow());
    }
}
