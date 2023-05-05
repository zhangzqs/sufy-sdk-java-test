package com.sufy.sufysdktest.object.bucket;

import com.alibaba.fastjson2.JSONObject;
import com.sufy.sdk.services.object.model.*;
import com.sufy.sufysdktest.object.ObjectBaseTest;
import com.sufy.util.HttpClientRecorder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

public class PolicyTest extends ObjectBaseTest {
    PutBucketPolicyRequest putBucketPolicyRequest;

    @BeforeEach
    public void setup() throws IOException {
        super.setup();
        String bucketName = getBucketName();
        putBucketPolicyRequest = PutBucketPolicyRequest.builder()
                .bucket(bucketName)
                .policy(String.format("{\n" +
                        "    \"Version\": \"sufy\",\n" +
                        "    \"Id\": \"public\",\n" +
                        "    \"Statement\": [\n" +
                        "      {\n" +
                        "        \"Sid\": \"publicGet\",\n" +
                        "        \"Effect\": \"Allow\",\n" +
                        "        \"Principal\": \"*\",\n" +
                        "        \"Action\": [\"miku:MOSGetObject\"],\n" +
                        "        \"Resource\": [\"srn:miku:::%s/*\"]\n" +
                        "      }\n" +
                        "    ]\n" +
                        "}", bucketName))
                .build();
        System.out.println(putBucketPolicyRequest.policy());
    }

    /**
     * 测试查看空间是否公开
     */
    @Test
    public void testGetBucketPolicyStatus() {

        // aws在没配置BucketPolicy时会报NoSuchBucketPolicy,对客户端不友好，我们返回200(isPublic值为false)
        object.deleteBucketPolicy(DeleteBucketPolicyRequest.builder().bucket(getBucketName()).build());
        recorder.startRecording();
        {
            GetBucketPolicyStatusResponse getBucketLocationResponse = object.getBucketPolicyStatus(GetBucketPolicyStatusRequest.builder()
                    .bucket(getBucketName())
                    .build()
            );
            assertNotNull(getBucketLocationResponse.policyStatus());
            assertFalse(getBucketLocationResponse.policyStatus().isPublic());

            // 配置后为true
            object.putBucketPolicy(putBucketPolicyRequest);
            getBucketLocationResponse = object.getBucketPolicyStatus(GetBucketPolicyStatusRequest.builder()
                    .bucket(getBucketName())
                    .build()
            );
            assertNotNull(getBucketLocationResponse.policyStatus());
            assertTrue(getBucketLocationResponse.policyStatus().isPublic());
        }
        HttpClientRecorder.HttpRecord httpRecord = recorder.stopAndGetRecords().get(0);
        SdkHttpRequest request = httpRecord.request.httpRequest();
        SdkHttpResponse response = httpRecord.response.httpResponse();

        checkPublicRequestHeader(request);
        assertEquals("GET", request.method().name());
        Assertions.assertEquals("/" + getBucketName(), request.encodedPath());
        assertEquals("policyStatus", request.encodedQueryParameters().orElseThrow());

        checkPublicResponseHeader(response);
        assertEquals(200, response.statusCode());
        assertEquals("OK", response.statusText().orElseThrow());
    }

    @Test
    public void testPutBucketPolicy() {
        recorder.startRecording();
        {
            PutBucketPolicyResponse putBucketPolicyResponse = object.putBucketPolicy(putBucketPolicyRequest);
            assertNotNull(putBucketPolicyResponse);
        }
        HttpClientRecorder.HttpRecord httpRecord = recorder.stopAndGetRecords().get(0);
        SdkHttpRequest request = httpRecord.request.httpRequest();
        SdkHttpResponse response = httpRecord.response.httpResponse();

        checkPublicRequestHeader(request);
        assertEquals("PUT", request.method().name());
        Assertions.assertEquals("/" + getBucketName(), request.encodedPath());
        assertEquals("policy", request.encodedQueryParameters().orElseThrow());

        checkPublicResponseHeader(response);
        assertEquals(204, response.statusCode());
        assertEquals("No Content", response.statusText().orElseThrow());
    }

    @Test
    public void testGetBucketPolicy() {
        object.putBucketPolicy(putBucketPolicyRequest);
        recorder.startRecording();
        {
            GetBucketPolicyResponse getBucketPolicyResponse = object.getBucketPolicy(GetBucketPolicyRequest.builder().bucket(getBucketName()).build());
            assertNotNull(getBucketPolicyResponse);
            assertNotNull(getBucketPolicyResponse.policy());
            JSONObject expected = JSONObject.parseObject(putBucketPolicyRequest.policy());
            JSONObject actual = JSONObject.parseObject(getBucketPolicyResponse.policy());
            assertEquals(expected, actual);
        }
        HttpClientRecorder.HttpRecord httpRecord = recorder.stopAndGetRecords().get(0);
        SdkHttpRequest request = httpRecord.request.httpRequest();
        SdkHttpResponse response = httpRecord.response.httpResponse();

        checkPublicRequestHeader(request);
        assertEquals("GET", request.method().name());
        Assertions.assertEquals("/" + getBucketName(), request.encodedPath());
        assertEquals("policy", request.encodedQueryParameters().orElseThrow());

        checkPublicResponseHeader(response);
        assertEquals(200, response.statusCode());
        assertEquals("OK", response.statusText().orElseThrow());
    }

    @Test
    public void testGetBucketPolicyWhenNoPolicy() {
        object.deleteBucketPolicy(DeleteBucketPolicyRequest.builder().bucket(getBucketName()).build());
        recorder.startRecording();
        assertThrows(ObjectException.class, () -> {
            object.getBucketPolicy(GetBucketPolicyRequest.builder().bucket(getBucketName()).build());
        });
        HttpClientRecorder.HttpRecord httpRecord = recorder.stopAndGetRecords().get(0);

        SdkHttpRequest request = httpRecord.request.httpRequest();
        checkPublicRequestHeader(request);
        assertEquals("GET", request.method().name());
        Assertions.assertEquals("/" + getBucketName(), request.encodedPath());
        assertEquals("policy", request.encodedQueryParameters().orElseThrow());

        SdkHttpResponse response = httpRecord.response.httpResponse();
        checkPublicResponseHeader(response);
        assertEquals(404, response.statusCode());
    }

    @Test
    public void testDeleteBucketPolicy() {
        recorder.startRecording();
        object.deleteBucketPolicy(DeleteBucketPolicyRequest.builder().bucket(getBucketName()).build());
        HttpClientRecorder.HttpRecord httpRecord = recorder.stopAndGetRecords().get(0);

        SdkHttpRequest request = httpRecord.request.httpRequest();
        checkPublicRequestHeader(request);
        assertEquals("DELETE", request.method().name());
        Assertions.assertEquals("/" + getBucketName(), request.encodedPath());
        assertEquals("policy", request.encodedQueryParameters().orElseThrow());

        SdkHttpResponse response = httpRecord.response.httpResponse();
        checkPublicResponseHeader(response);
        assertEquals(204, response.statusCode());
        assertEquals("No Content", response.statusText().orElseThrow());
    }

}
