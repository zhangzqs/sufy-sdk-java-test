package com.sufy.sufysdktest.object.bucket;

import com.sufy.sdk.services.object.model.*;
import com.sufy.sufysdktest.object.ObjectBaseTest;
import com.sufy.util.HttpClientRecorder;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;

import static org.junit.jupiter.api.Assertions.*;

public class BucketManageTest extends ObjectBaseTest {

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
            assertNotNull(headBucketResponse.regionAsString());
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
            String locationConstraint = response.locationConstraintAsString();
            assertNotNull(locationConstraint);
            assertEquals(config.getRegion(), locationConstraint);
            BucketLocationConstraint blc = response.locationConstraint();
            assertNotNull(blc);
            assertEquals(config.getRegion(), response.locationConstraint().toString());
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
        // 先确保存在这个bucket
        makeSureBucketExists();

        recorder.startRecording();
        {
            DeleteBucketResponse response = object.deleteBucket(DeleteBucketRequest.builder()
                    .bucket(getBucketName())
                    .build()
            );
            assertNotNull(response);
        }
        HttpClientRecorder.HttpRecord record = recorder.stopAndGetRecords().get(0);

        // 请求头与响应头判定
        SdkHttpRequest req = record.request.httpRequest();
        SdkHttpResponse resp = record.response.httpResponse();

        checkPublicRequestHeader(req);
        assertEquals(SdkHttpMethod.DELETE, req.method());
        assertEquals("/" + getBucketName(), req.encodedPath());

        checkPublicResponseHeader(resp);
        assertEquals(204, resp.statusCode());
        assertEquals("No Content", resp.statusText().orElseThrow());

        assertThrows(NoSuchBucketException.class, () -> {
            object.headBucket(HeadBucketRequest.builder()
                    .bucket(getBucketName())
                    .build()
            );
        });
    }
}