package com.sufy.sufysdktest.object.object;

import com.sufy.sdk.services.object.model.CreateMultipartUploadRequest;
import com.sufy.sdk.services.object.model.CreateMultipartUploadResponse;
import com.sufy.sdk.services.object.model.UploadPartRequest;
import com.sufy.sdk.services.object.model.UploadPartResponse;
import com.sufy.sufysdktest.object.ObjectBaseTest;
import com.sufy.util.HttpClientRecorder;
import org.apache.http.entity.ContentType;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;

import static org.junit.jupiter.api.Assertions.*;

public class MultipartUploadTest extends ObjectBaseTest {

    @Test
    public void testCreateMultipartUpload() {
        String key = "testCreateMultipartUploadFile";
        String contentType = ContentType.TEXT_PLAIN.getMimeType();
        recorder.startRecording();
        {
            CreateMultipartUploadResponse createMultipartUploadResponse = object.createMultipartUpload(
                    CreateMultipartUploadRequest.builder()
                            .key(key)
                            .bucket(getBucketName())
                            .contentType(contentType)
                            .build()
            );
            assertEquals(key, createMultipartUploadResponse.key());
            assertEquals(getBucketName(), createMultipartUploadResponse.bucket());
            assertFalse(createMultipartUploadResponse.uploadId().isEmpty());
        }
        HttpClientRecorder.HttpRecord record = recorder.stopAndGetRecords().get(0);

        // 请求头与响应头判定
        SdkHttpRequest req = record.request.httpRequest();
        SdkHttpResponse resp = record.response.httpResponse();

        checkPublicRequestHeader(req);
        assertEquals(SdkHttpMethod.POST, req.method());
        assertEquals(String.format("/%s/%s", getBucketName(), key), req.encodedPath());
        assertEquals(contentType, req.headers().get("Content-Type").get(0));

        checkPublicResponseHeader(resp);
        assertEquals(200, resp.statusCode());
        assertEquals("OK", resp.statusText().orElseThrow());
    }

    @Test
    public void testUploadPart() {
        String key = "testUploadPartFile";
        String contentType = ContentType.APPLICATION_OCTET_STREAM.getMimeType();
        CreateMultipartUploadResponse createMultipartUploadResponse = object.createMultipartUpload(
                CreateMultipartUploadRequest.builder()
                        .key(key)
                        .bucket(getBucketName())
                        .contentType(contentType)
                        .build()
        );
        String uploadId = createMultipartUploadResponse.uploadId();

        recorder.startRecording();
        {
            UploadPartResponse uploadPartResponse = object.uploadPart(
                    UploadPartRequest.builder()
                            .bucket(getBucketName())
                            .key(key)
                            .uploadId(uploadId)
                            .partNumber(1)
                            .build(),
                    RequestBody.fromBytes(randomBytes(1024))
            );
            assertNotNull(uploadPartResponse.eTag());
        }
        HttpClientRecorder.HttpRecord record = recorder.stopAndGetRecords().get(0);

        // 请求头与响应头判定
        SdkHttpRequest req = record.request.httpRequest();

        checkPublicRequestHeader(req);
        assertEquals(SdkHttpMethod.PUT, req.method());
        assertEquals(String.format("/%s/%s", getBucketName(), key), req.encodedPath());
        assertEquals(String.format("partNumber=1&uploadId=%s", uploadId), req.encodedQueryParameters().orElseThrow());
        
        SdkHttpResponse resp = record.response.httpResponse();
        checkPublicResponseHeader(resp);
        assertEquals(200, resp.statusCode());
        assertEquals("OK", resp.statusText().orElseThrow());
    }

    @Test
    public void testCompleteMultipartUpload() {
        object.completeMultipartUpload(req -> req.bucket(getBucketName()).build());
    }

    @Test
    public void testAbortMultipartUpload() {
        object.abortMultipartUpload(req -> req.bucket(getBucketName()).build());
    }

    /**
     * 列举文件级别正在进行的分片
     */
    @Test
    public void testListMultipartUploads() {
        object.listMultipartUploads(req -> req.bucket(getBucketName()).build());
    }

    /**
     * 列举bucket级别正在进行的分片
     */
    @Test
    public void testListParts() {
        object.listParts(req -> req.bucket(getBucketName()).build());
    }

}
