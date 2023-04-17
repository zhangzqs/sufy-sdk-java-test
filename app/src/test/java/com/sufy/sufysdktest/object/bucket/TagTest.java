package com.sufy.sufysdktest.object.bucket;

import com.sufy.sdk.services.object.model.*;
import com.sufy.util.HttpClientRecorder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;
import sufy.util.ObjectTestBase;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TagTest extends ObjectTestBase {
    @Test
    public void testPutBucketTagging() {
        /*
         * TODO: SDK实际请求体数据： {"tagSet":[{"key":"key1","value":"value1"},{"key":"key2","value":"value2"}]}
         *  与文档中描述的期望请求：{"tagSet":{"tags":[{"key":"key1","value":"value1"},{"key":"key2","value":"value2"}]}}
         *  格式不一致，服务器端无法完成序列化
         * */
        object.putBucketTagging(PutBucketTaggingRequest.builder()
                .bucket(getBucketName())
                .tagging(Tagging.builder()
                        .tagSet(Set.of(
                                        Tag.builder().key("key1").value("value1").build(),
                                        Tag.builder().key("key2").value("value2").build()
                                )
                        )
                        .build())
                .build()
        );
    }

    @Test
    public void testGetBucketTagging() {

    }


    @Test
    public void testGetBucketTaggingWhenNoTagging() {
        /*
         * TODO: Sufy文档上，并未说明可能发生404错误，但是实际上服务器返回了404
         * */
        object.deleteBucketTagging(DeleteBucketTaggingRequest.builder().bucket(getBucketName()).build());
        recorder.startRecording();
        object.getBucketTagging(GetBucketTaggingRequest.builder().bucket(getBucketName()).build());
        HttpClientRecorder.HttpRecord httpRecord = recorder.stopAndGetRecords().get(0);

        SdkHttpRequest request = httpRecord.request.httpRequest();
        checkPublicRequestHeader(request);
        assertEquals("GET", request.method().name());
        Assertions.assertEquals("/" + getBucketName(), request.encodedPath());
        assertEquals("tagging", request.encodedQueryParameters().orElseThrow());

        SdkHttpResponse response = httpRecord.response.httpResponse();
        checkPublicResponseHeader(response);
        assertEquals(404, response.statusCode());
    }

    @Test
    public void testDeleteBucketTagging() {
        recorder.startRecording();
        object.deleteBucketTagging(DeleteBucketTaggingRequest.builder().bucket(getBucketName()).build());
        HttpClientRecorder.HttpRecord httpRecord = recorder.stopAndGetRecords().get(0);

        SdkHttpRequest request = httpRecord.request.httpRequest();
        checkPublicRequestHeader(request);
        assertEquals("DELETE", request.method().name());
        Assertions.assertEquals("/" + getBucketName(), request.encodedPath());
        assertEquals("tagging", request.encodedQueryParameters().orElseThrow());

        SdkHttpResponse response = httpRecord.response.httpResponse();
        checkPublicResponseHeader(response);
        assertEquals(204, response.statusCode());
        assertEquals("No Content", response.statusText().orElseThrow());
    }

}
