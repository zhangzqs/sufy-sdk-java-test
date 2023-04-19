package com.sufy.sufysdktest.object.object;

import com.sufy.sdk.services.object.model.*;
import com.sufy.sufysdktest.object.ObjectBaseTest;
import com.sufy.util.HttpClientRecorder;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public class ObjectManageTest extends ObjectBaseTest {

    @Test
    public void testCopyObject() {
        String srcKey = "testCopyObjectFileKeySrc";
        String destKey = "testCopyObjectFileKeyDest";
        /*
         * TODO:
         *  The request signature we calculated does not match the signature you provided
         *  (Service: Object, Status Code: 403, Request ID: null)
         *  com.sufy.sdk.services.object.model.ObjectException: The request signature we
         *  calculated does not match the signature you provided (Service: Object,
         *  Status Code: 403, Request ID: null)
         * */
        object.copyObject(CopyObjectRequest.builder()
                .sourceBucket(getBucketName())
                .destinationBucket(getBucketName())
                .sourceKey(srcKey)
                .destinationKey(destKey)
                .build()
        );
    }

    @Test
    public void testDeleteObject() {
        String key = "testDeleteObjectFileKey";
        String content = "testDeleteObjectFileContent";
        prepareTestFile(key, content);
        assertDoesNotThrow(() -> {
            // 文件存在
            HeadObjectResponse headObjectResponse = object.headObject(HeadObjectRequest.builder()
                    .bucket(getBucketName())
                    .key(key)
                    .build()
            );
            assertNotNull(headObjectResponse);
            assertEquals(content.length(), headObjectResponse.contentLength());
        });
        recorder.startRecording();
        {
            object.deleteObject(DeleteObjectRequest.builder()
                    .bucket(getBucketName())
                    .key(key)
                    .build()
            );
        }
        HttpClientRecorder.HttpRecord record = recorder.stopAndGetRecords().get(0);
        SdkHttpRequest request = record.request.httpRequest();
        checkPublicRequestHeader(request);
        assertEquals(SdkHttpMethod.DELETE, request.method());
        assertEquals(String.format("/%s/%s", getBucketName(), key), request.encodedPath());

        SdkHttpResponse response = record.response.httpResponse();
        checkPublicResponseHeader(response);
        assertEquals(204, record.response.httpResponse().statusCode());
        assertEquals("No Content", record.response.httpResponse().statusText().orElseThrow());

        assertThrows(NoSuchKeyException.class, () -> {
            object.headObject(HeadObjectRequest.builder()
                    .bucket(getBucketName())
                    .key(key)
                    .build()
            );
        });
    }

    @Test
    public void testDeleteObjects() {
        int nums = 10;
        // 准备空文件
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < nums; i++) {
            String key = "testDeleteObjectsFileKey" + i;
            prepareTestFile(key, "");
            keys.add(key);
        }
        // 删除空文件
        DeleteObjectsResponse deleteObjectsResponse = object.deleteObjects(DeleteObjectsRequest.builder()
                .bucket(getBucketName())
                .delete(Delete.builder()
                        .objects(keys.stream()
                                .map(key -> ObjectIdentifier.builder()
                                        .key(key)
                                        .build()
                                ).collect(Collectors.toList())
                        )
                        .quiet(false)
                        .build())
                .build()
        );
        assertNotNull(deleteObjectsResponse);
        assertEquals(nums, deleteObjectsResponse.deleted().size());

        // 验证这些文件不存在了
        for (String key : keys) {
            assertThrows(NoSuchKeyException.class, () -> {
                object.headObject(HeadObjectRequest.builder()
                        .bucket(getBucketName())
                        .key(key)
                        .build()
                );
            });
        }
    }

    @Test
    public void testRestoreObject() {
        String key = "testRestoreObjectFileKey";
        String content = "testRestoreObjectFileContent";
        prepareTestFile(key, content);

        RestoreObjectResponse restoreObjectResponse = object.restoreObject(RestoreObjectRequest.builder()
                .bucket(getBucketName())
                .key(key)
                .restoreRequest(RestoreRequest.builder()
                        .days(1)
                        .build()
                )
                .build()
        );
    }

}
