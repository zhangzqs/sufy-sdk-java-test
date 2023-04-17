package com.sufy.sufysdktest.object.object;

import com.sufy.sdk.services.object.model.CreateMultipartUploadRequest;
import com.sufy.sufysdktest.object.ObjectBaseTest;
import com.sufy.util.HttpClientRecorder;
import org.apache.http.entity.ContentType;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MultipartUploadTest extends ObjectBaseTest {

    @Test
    public void testCreateMultipartUpload() {
        String key = "testCreateMultipartUploadFile";
        String contentType = ContentType.TEXT_PLAIN.getMimeType();
        recorder.startRecording();
        {
            // TODO: 使用Sufy签名分片上传时，报错如下
            //  {
            //      "code":"SignatureDoesNotMatch",
            //      "message":"The request signature we calculated does not match the signature you provided",
            //      "resource":"/kodo-s3apiv2-test-miku-sufy-java-sdk-test/testCreateMultipartUploadFile",
            //      "requestId":"BwAAAK73TASwaVUX"
            //  }
            object.createMultipartUpload(CreateMultipartUploadRequest.builder()
                    .key(key)
                    .bucket(getBucketName())
                    .contentType(contentType)
                    .build()
            );
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
        object.uploadPart(req -> req.bucket(getBucketName()).build(), RequestBody.empty());
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
