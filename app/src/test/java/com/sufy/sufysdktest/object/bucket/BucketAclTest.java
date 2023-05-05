package com.sufy.sufysdktest.object.bucket;

import com.sufy.sdk.services.object.model.ObjectException;
import com.sufy.sufysdktest.object.ObjectBaseTest;
import com.sufy.util.HttpClientRecorder;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/*
 * Sufy 不支持 Bucket ACL 功能，Sufy SDK中均应当获得501 NotImplemented错误
 * */
public class BucketAclTest extends ObjectBaseTest {

    @Test
    public void testPutBucketAcl() {
        recorder.startRecording();
        {
            ObjectException e = assertThrows(ObjectException.class, () -> {
                object.putBucketAcl(req -> req.bucket(getBucketName()).build());
            });
            assertEquals(501, e.statusCode());
        }
        HttpClientRecorder.HttpRecord record = recorder.stopAndGetRecords().get(0);
        SdkHttpRequest req = record.request.httpRequest();
        checkPublicRequestHeader(req);
        assertEquals(SdkHttpMethod.PUT, req.method());
        assertEquals("/" + getBucketName(), req.encodedPath());

        SdkHttpResponse resp = record.response.httpResponse();
        checkPublicResponseHeader(resp);
        assertEquals(501, resp.statusCode());
        assertEquals("Not Implemented", resp.statusText().orElseThrow());
    }

    @Test
    public void testGetBucketAcl() {
        recorder.startRecording();
        {
            ObjectException e = assertThrows(ObjectException.class, () -> {
                object.getBucketAcl(req -> req.bucket(getBucketName()).build());
            });
            assertEquals(501, e.statusCode());
        }
        HttpClientRecorder.HttpRecord record = recorder.stopAndGetRecords().get(0);
        SdkHttpRequest req = record.request.httpRequest();
        assertEquals(SdkHttpMethod.GET, req.method());
        checkPublicRequestHeader(req);
        assertEquals("/" + getBucketName(), req.encodedPath());

        SdkHttpResponse resp = record.response.httpResponse();
        checkPublicResponseHeader(resp);
        assertEquals(501, resp.statusCode());
        assertEquals("Not Implemented", resp.statusText().orElseThrow());
    }

}
