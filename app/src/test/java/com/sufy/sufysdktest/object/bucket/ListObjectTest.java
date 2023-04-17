package com.sufy.sufysdktest.object.bucket;

import com.sufy.sdk.services.object.model.*;
import com.sufy.util.HttpClientRecorder;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;
import sufy.util.ObjectTestBase;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class ListObjectTest extends ObjectTestBase {

    @Test
    public void testListObjects() {
        makeSureBucketExists();
        cleanAllFiles();

        String prefix = "dir1/test-list-objects-v1-";
        // 准备 10 个文件
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 10; i++) keys.add(prefix + i);
        for (String key : keys) prepareTestFile(key, key);

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
                assertEquals(7, contents.size());
                for (final SufyObject content : contents) {
                    // TODO: etag 为null，由于服务器端返回的字段为etag，这里解析的方式为eTag
//                    assertFalse(content.eTag().isEmpty());
                    assertFalse(content.key().isEmpty());
                    assertTrue(keys.stream().anyMatch(key -> key.equals(content.key())));
                    assertNotNull(content.lastModified());
                    assertEquals(ObjectStorageClass.STANDARD, content.storageClass());
                }
            }
            {
                List<CommonPrefix> commonPrefixes = listObjectsResponse1.commonPrefixes();
                assertEquals(1, commonPrefixes.size());
                assertEquals("dir1/", commonPrefixes.get(0).prefix());
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
            // TODO: nextMarker 应当为空字符串还是 null? sufy返回了null
//            assertTrue(listObjectsResponse2.nextMarker().isEmpty());
            assertFalse(listObjectsResponse2.isTruncated()); // 列举完毕
            {
                List<SufyObject> contents = listObjectsResponse2.contents();
                assertEquals(3, contents.size());
                for (final SufyObject content : contents) {
//                    assertFalse(content.eTag().isEmpty());
                    assertFalse(content.key().isEmpty());
                    assertTrue(keys.stream().anyMatch(key -> key.equals(content.key())));
                    assertNotNull(content.lastModified());
                    assertEquals(ObjectStorageClass.STANDARD, content.storageClass());
                }
            }
            {
                List<CommonPrefix> commonPrefixes = listObjectsResponse1.commonPrefixes();
                assertEquals(1, commonPrefixes.size());
                assertEquals("dir1/", commonPrefixes.get(0).prefix());
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
        makeSureBucketExists();
        cleanAllFiles();

        String prefix = "dir2/test-list-objects-v2-";
        // 准备 10 个文件
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 10; i++) keys.add(prefix + i);
        for (String key : keys) prepareTestFile(key, key);

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
            assertNull(listObjectsResponse1.continuationToken());
            assertNotNull(listObjectsResponse1.nextContinuationToken());
            assertTrue(listObjectsResponse1.isTruncated()); // 未列举完毕，被截断了
            {
                List<SufyObject> contents = listObjectsResponse1.contents();
                assertEquals(7, contents.size());
                for (final SufyObject content : contents) {
                    // TODO: etag 为null，由于服务器端返回的字段为etag，这里解析的方式为eTag
//                    assertFalse(content.eTag().isEmpty());
                    assertFalse(content.key().isEmpty());
                    assertTrue(keys.stream().anyMatch(key -> key.equals(content.key())));
                    assertNotNull(content.lastModified());
                    assertEquals(ObjectStorageClass.STANDARD, content.storageClass());
                }
            }
            {
                List<CommonPrefix> commonPrefixes = listObjectsResponse1.commonPrefixes();
                // TODO: commonPrefixes为空
                assertEquals(1, commonPrefixes.size());
                assertEquals("dir2/", commonPrefixes.get(0).prefix());
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
            assertTrue(listObjectsResponse2.nextContinuationToken().isEmpty());
            assertFalse(listObjectsResponse2.isTruncated()); // 列举完毕
            {
                List<SufyObject> contents = listObjectsResponse2.contents();
                assertEquals(3, contents.size());
                for (final SufyObject content : contents) {
//                    assertFalse(content.eTag().isEmpty());
                    assertFalse(content.key().isEmpty());
                    assertTrue(keys.stream().anyMatch(key -> key.equals(content.key())));
                    assertNotNull(content.lastModified());
                    assertEquals(ObjectStorageClass.STANDARD, content.storageClass());
                }
            }
            {
                List<CommonPrefix> commonPrefixes = listObjectsResponse1.commonPrefixes();
                assertEquals(1, commonPrefixes.size());
                assertEquals("dir2/", commonPrefixes.get(0).prefix());
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