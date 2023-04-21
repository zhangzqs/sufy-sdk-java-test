package com.sufy.sufysdktest.object.bucket;

import com.sufy.sdk.services.object.model.*;
import com.sufy.sufysdktest.object.ObjectBaseTest;
import com.sufy.util.HttpClientRecorder;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class ListObjectTest extends ObjectBaseTest {

    /**
     * 总计上传20个文件，其中10个文件在dir1/目录下，10个文件在dir1/subdir/目录下
     * 计划分两次列举，每次列举最多7个条目
     * 第一次列举，列举dir1/目录下的文件和目录前缀，期望结果应当有6个文件和1个目录前缀
     * 第二次列举，接着第一次的列举，应当剩余4个文件和0个目录前缀
     */
    @Test
    public void testListObjects() {
        String prefix = "dir1/";
        String subdir = prefix + "subdir/";
        int N = 10;
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < N; i++) keys.add(prefix + "test-list-objects-v1-" + i);
        for (int i = 0; i < N; i++) keys.add(subdir + "test-list-objects-v1-" + i);
        prepareAsyncEnv();
        for (String key : keys) submitAsyncTask(() -> prepareTestFile(key, key));
        awaitAllAsyncTasks();

        recorder.startRecording();
        {
            ListObjectsResponse listObjectsResponse1 = object.listObjects(ListObjectsRequest.builder()
                    .bucket(getBucketName())
                    .delimiter("/")
                    .prefix(prefix)
                    .maxKeys(7)
                    .build()
            );
            assertEquals(getBucketName(), listObjectsResponse1.name());
            assertEquals(prefix, listObjectsResponse1.prefix());
            assertEquals("/", listObjectsResponse1.delimiter());
            assertEquals(7, listObjectsResponse1.maxKeys());
//            assertEquals("", listObjectsResponse1.marker());
            assertFalse(listObjectsResponse1.nextMarker().isEmpty());
            assertTrue(listObjectsResponse1.isTruncated()); // 未列举完毕，被截断了
            {
                List<SufyObject> contents = listObjectsResponse1.contents();
                assertEquals(6, contents.size());
                for (final SufyObject content : contents) {
                    assertFalse(content.eTag().isEmpty());
                    assertFalse(content.key().isEmpty());
                    assertTrue(keys.stream().anyMatch(key -> key.equals(content.key())));
                    assertNotNull(content.lastModified());
                    assertEquals(ObjectStorageClass.STANDARD, content.storageClass());
                }
            }
            {
                List<CommonPrefix> commonPrefixes = listObjectsResponse1.commonPrefixes();
                assertEquals(1, commonPrefixes.size());
                assertEquals(subdir, commonPrefixes.get(0).prefix());
            }
            ListObjectsResponse listObjectsResponse2 = object.listObjects(ListObjectsRequest.builder()
                    .bucket(getBucketName())
                    .delimiter("/")
                    .prefix(prefix)
                    .marker(listObjectsResponse1.nextMarker())
                    .maxKeys(7)
                    .build()
            );
            assertEquals(getBucketName(), listObjectsResponse2.name());
            assertEquals(prefix, listObjectsResponse2.prefix());
            assertEquals("/", listObjectsResponse2.delimiter());
            assertEquals(7, listObjectsResponse2.maxKeys());
            assertEquals(listObjectsResponse1.nextMarker(), listObjectsResponse2.marker());
            assertFalse(listObjectsResponse2.isTruncated()); // 列举完毕
            {
                List<SufyObject> contents = listObjectsResponse2.contents();
                assertEquals(4, contents.size());
                for (final SufyObject content : contents) {
                    assertFalse(content.eTag().isEmpty());
                    assertFalse(content.key().isEmpty());
                    assertTrue(keys.stream().anyMatch(key -> key.equals(content.key())));
                    assertNotNull(content.lastModified());
                    assertEquals(ObjectStorageClass.STANDARD, content.storageClass());
                }
            }
            {
                List<CommonPrefix> commonPrefixes = listObjectsResponse2.commonPrefixes();
                assertTrue(commonPrefixes.isEmpty());
            }
        }

        List<HttpClientRecorder.HttpRecord> records = recorder.stopAndGetRecords();

        HttpClientRecorder.HttpRecord record1 = records.get(0);
        SdkHttpRequest req1 = record1.request.httpRequest();
        checkPublicRequestHeader(req1);
        assertEquals(SdkHttpMethod.GET, req1.method());
        assertEquals("/" + getBucketName(), req1.encodedPath());

        SdkHttpResponse resp1 = record1.response.httpResponse();
        checkPublicResponseHeader(resp1);
        assertEquals(200, resp1.statusCode());
        assertEquals("OK", resp1.statusText().orElseThrow());

        HttpClientRecorder.HttpRecord record2 = records.get(1);
        SdkHttpRequest req2 = record2.request.httpRequest();
        checkPublicRequestHeader(req2);
        assertEquals(SdkHttpMethod.GET, req2.method());
        assertEquals("/" + getBucketName(), req2.encodedPath());

        SdkHttpResponse resp2 = record2.response.httpResponse();
        checkPublicResponseHeader(resp2);
        assertEquals(200, resp2.statusCode());
        assertEquals("OK", resp2.statusText().orElseThrow());
    }

    @Test
    public void testListObjectsV2() {
        String prefix = "dir1/";
        String subdir = prefix + "subdir/";
        int N = 10;
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < N; i++) keys.add(prefix + "test-list-objects-v2-" + i);
        for (int i = 0; i < N; i++) keys.add(subdir + "test-list-objects-v2-" + i);
        prepareAsyncEnv();
        for (String key : keys) submitAsyncTask(() -> prepareTestFile(key, key));
        awaitAllAsyncTasks();

        recorder.startRecording();
        {
            ListObjectsV2Response listObjectsResponse1 = object.listObjectsV2(ListObjectsV2Request.builder()
                    .bucket(getBucketName())
                    .delimiter("/")
                    .prefix(prefix)
                    .maxKeys(7)
                    .build()
            );
            assertEquals(getBucketName(), listObjectsResponse1.name());
            assertEquals(prefix, listObjectsResponse1.prefix());
            assertEquals("/", listObjectsResponse1.delimiter());
            assertEquals(7, listObjectsResponse1.maxKeys());
//            assertEquals("", listObjectsResponse1.marker());
            assertFalse(listObjectsResponse1.nextContinuationToken().isEmpty());
            assertTrue(listObjectsResponse1.isTruncated()); // 未列举完毕，被截断了
            {
                List<SufyObject> contents = listObjectsResponse1.contents();
                assertEquals(6, contents.size());
                for (final SufyObject content : contents) {
                    assertFalse(content.eTag().isEmpty());
                    assertFalse(content.key().isEmpty());
                    assertTrue(keys.stream().anyMatch(key -> key.equals(content.key())));
                    assertNotNull(content.lastModified());
                    assertEquals(ObjectStorageClass.STANDARD, content.storageClass());
                }
            }
            {
                List<CommonPrefix> commonPrefixes = listObjectsResponse1.commonPrefixes();
                assertEquals(1, commonPrefixes.size());
                assertEquals(subdir, commonPrefixes.get(0).prefix());
            }
            ListObjectsV2Response listObjectsResponse2 = object.listObjectsV2(ListObjectsV2Request.builder()
                    .bucket(getBucketName())
                    .delimiter("/")
                    .prefix(prefix)
                    .continuationToken(listObjectsResponse1.nextContinuationToken())
                    .maxKeys(7)
                    .build()
            );
            assertEquals(getBucketName(), listObjectsResponse2.name());
            assertEquals(prefix, listObjectsResponse2.prefix());
            assertEquals("/", listObjectsResponse2.delimiter());
            assertEquals(7, listObjectsResponse2.maxKeys());
            assertEquals(listObjectsResponse1.nextContinuationToken(), listObjectsResponse2.continuationToken());
            assertFalse(listObjectsResponse2.isTruncated()); // 列举完毕
            {
                List<SufyObject> contents = listObjectsResponse2.contents();
                assertEquals(4, contents.size());
                for (final SufyObject content : contents) {
                    assertFalse(content.eTag().isEmpty());
                    assertFalse(content.key().isEmpty());
                    assertTrue(keys.stream().anyMatch(key -> key.equals(content.key())));
                    assertNotNull(content.lastModified());
                    assertEquals(ObjectStorageClass.STANDARD, content.storageClass());
                }
            }
            {
                List<CommonPrefix> commonPrefixes = listObjectsResponse2.commonPrefixes();
                assertTrue(commonPrefixes.isEmpty());
            }
        }

        List<HttpClientRecorder.HttpRecord> records = recorder.stopAndGetRecords();

        HttpClientRecorder.HttpRecord record1 = records.get(0);
        SdkHttpRequest req1 = record1.request.httpRequest();
        checkPublicRequestHeader(req1);
        assertEquals(SdkHttpMethod.GET, req1.method());
        assertEquals("/" + getBucketName(), req1.encodedPath());

        SdkHttpResponse resp1 = record1.response.httpResponse();
        checkPublicResponseHeader(resp1);
        assertEquals(200, resp1.statusCode());
        assertEquals("OK", resp1.statusText().orElseThrow());

        HttpClientRecorder.HttpRecord record2 = records.get(1);
        SdkHttpRequest req2 = record2.request.httpRequest();
        checkPublicRequestHeader(req2);
        assertEquals(SdkHttpMethod.GET, req2.method());
        assertEquals("/" + getBucketName(), req2.encodedPath());

        SdkHttpResponse resp2 = record2.response.httpResponse();
        checkPublicResponseHeader(resp2);
        assertEquals(200, resp2.statusCode());
        assertEquals("OK", resp2.statusText().orElseThrow());
    }

}