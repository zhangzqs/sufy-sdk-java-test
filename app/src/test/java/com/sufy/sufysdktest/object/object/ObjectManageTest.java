package com.sufy.sufysdktest.object.object;

import com.sufy.sdk.services.object.model.*;
import com.sufy.sufysdktest.object.ObjectBaseTest;
import com.sufy.util.HttpClientRecorder;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
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
        String content = "testCopyObjectFileContent";
        String metadataDirective = "REPLACE";

        prepareAsyncEnv();
        submitAsyncTask(() -> deleteTestFile(srcKey));
        submitAsyncTask(() -> deleteTestFile(destKey));
        awaitAllAsyncTasks();

        prepareTestFile(srcKey, content);

        // 复制文件
        recorder.startRecording();
        {
            object.copyObject(CopyObjectRequest.builder()
                    .sourceBucket(getBucketName())
                    .destinationBucket(getBucketName())
                    .sourceKey(srcKey)
                    .destinationKey(destKey)
                    .metadataDirective(metadataDirective)
                    .build()
            );
        }
        HttpClientRecorder.HttpRecord record = recorder.stopAndGetRecords().get(0);
        SdkHttpRequest request = record.request.httpRequest();
        checkPublicRequestHeader(request);
        assertEquals(SdkHttpMethod.PUT, request.method());
        assertEquals(String.format("/%s/%s", getBucketName(), destKey), request.encodedPath());
        assertEquals(metadataDirective, request.headers().get("x-sufy-metadata-directive").get(0));
        if (request.headers().containsKey("content-length")) {
            assertEquals("0", request.headers().get("content-length").get(0));
        }

        assertEquals(
                String.format("%s/%s", getBucketName(), srcKey), // 这里严格来说应该是url encode过的
                request.headers().get("x-sufy-copy-source").get(0)
        );

        SdkHttpResponse response = record.response.httpResponse();
        checkPublicResponseHeader(response);
        assertEquals(200, response.statusCode());
        assertEquals("OK", response.statusText().orElseThrow());


        // 复制文件后，目标文件和源文件都存在
        assertDoesNotThrow(() -> {
            HeadObjectResponse src = object.headObject(HeadObjectRequest.builder()
                    .bucket(getBucketName())
                    .key(srcKey)
                    .build()
            );
            assertNotNull(src);
            assertEquals(content.length(), src.contentLength());

            HeadObjectResponse dest = object.headObject(HeadObjectRequest.builder()
                    .bucket(getBucketName())
                    .key(destKey)
                    .build()
            );
            assertNotNull(dest);
            assertEquals(content.length(), dest.contentLength());
        });
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

        // 这里有服务端缓存，无法测试
//        assertThrows(NoSuchKeyException.class, () -> {
//            Thread.sleep(5000);
//            object.headObject(HeadObjectRequest.builder()
//                    .bucket(getBucketName())
//                    .key(key)
//                    .build()
//            );
//        });
    }

    @Test
    public void testDeleteObjects() {
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 10; i++) keys.add("testDeleteObjectsFileKey" + i);
        prepareAsyncEnv();
        keys.forEach((key) -> submitAsyncTask(() -> prepareTestFile(key, key + "-content")));
        awaitAllAsyncTasks();

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
        assertEquals(
                keys,
                deleteObjectsResponse.deleted()
                        .stream()
                        .map(DeletedObject::key)
                        .collect(Collectors.toList())
        );

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
        // 上传文件
        object.putObject(
                PutObjectRequest.builder()
                        .bucket(getBucketName())
                        .key(key)
                        .storageClass(StorageClass.DEEP_ARCHIVE)
                        .build(),
                RequestBody.fromString(content)
        );

        recorder.startRecording();
        {
            RestoreObjectResponse restoreObjectResponse = object.restoreObject(RestoreObjectRequest.builder()
                    .bucket(getBucketName())
                    .key(key)
                    .restoreRequest(
                            RestoreRequest.builder()
                                    .days(1)
                                    .build()
                    )
                    .build()
            );
            // TODO: 这俩字段返回null?
            System.out.println("requestCharged: " + restoreObjectResponse.requestCharged());
            System.out.println("restoreOutputPath: " + restoreObjectResponse.restoreOutputPath());
        }
        HttpClientRecorder.HttpRecord record = recorder.stopAndGetRecords().get(0);
        SdkHttpRequest request = record.request.httpRequest();
        checkPublicRequestHeader(request);
        assertEquals(SdkHttpMethod.POST, request.method());
        assertEquals(String.format("/%s/%s", getBucketName(), key), request.encodedPath());
        assertEquals("restore", request.encodedQueryParameters().orElseThrow());

        SdkHttpResponse response = record.response.httpResponse();
        checkPublicResponseHeader(response);
        assertEquals(202, record.response.httpResponse().statusCode());
        assertEquals("Accepted", record.response.httpResponse().statusText().orElseThrow());

        // TODO: 第一次发起解冻后，第二次发起解冻仍然是202？
        {
            RestoreObjectResponse restoreObjectResponse = object.restoreObject(RestoreObjectRequest.builder()
                    .bucket(getBucketName())
                    .key(key)
                    .restoreRequest(
                            RestoreRequest.builder()
                                    .days(1)
                                    .build()
                    )
                    .build()
            );
        }
    }

}
