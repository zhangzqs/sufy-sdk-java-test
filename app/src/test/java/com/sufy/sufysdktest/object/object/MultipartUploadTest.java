package com.sufy.sufysdktest.object.object;

import com.sufy.sdk.services.object.model.*;
import com.sufy.sufysdktest.object.ObjectBaseTest;
import com.sufy.util.HttpClientRecorder;
import org.apache.http.entity.ContentType;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

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

    /**
     * 创建一个分片上传任务，并发上传2个分片，单分片大小5MB, 并发度为2，等待完成分片上传，最后获取文件是否上传成功
     */
    @Test
    public void testCompleteMultipartUpload() throws IOException {
        String key = "testCompleteMultipartUploadFile";
        String contentType = ContentType.APPLICATION_OCTET_STREAM.getMimeType();
        int parts = 2;
        int nThreads = 2;
        int partSize = 5 * 1024 * 1024;
        CreateMultipartUploadResponse createMultipartUploadResponse = object.createMultipartUpload(
                CreateMultipartUploadRequest.builder()
                        .key(key)
                        .bucket(getBucketName())
                        .contentType(contentType)
                        .build()
        );
        String uploadId = createMultipartUploadResponse.uploadId();
        List<byte[]> bytesList = new ArrayList<>(parts);

        // 构造一个并发安全的Map<partNumber, eTag>，用于最终完成分片上传
        ConcurrentHashMap<Integer, String> partNumber2ETag = new ConcurrentHashMap<>(parts);

        prepareAsyncEnv(nThreads);
        for (int i = 1; i <= parts; i++) {
            final byte[] bytes = randomBytes(partSize);
            bytesList.add(bytes);
            final int partNumber = i;
            submitAsyncTask(() -> {
                UploadPartResponse uploadPartResponse = object.uploadPart(
                        UploadPartRequest.builder()
                                .bucket(getBucketName())
                                .key(key)
                                .uploadId(uploadId)
                                .partNumber(partNumber)
                                .build(),
                        RequestBody.fromBytes(bytes)
                );
                assertNotNull(uploadPartResponse.eTag());
                partNumber2ETag.put(partNumber, uploadPartResponse.eTag());
            });
        }
        awaitAllAsyncTasks();

        String eTag;
        recorder.startRecording();
        {
            CompleteMultipartUploadResponse completeMultipartUploadResponse = object.completeMultipartUpload(
                    CompleteMultipartUploadRequest.builder()
                            .bucket(getBucketName())
                            .key(key)
                            .uploadId(uploadId)
                            .multipartUpload(CompletedMultipartUpload.builder()
                                    .parts(partNumber2ETag.entrySet()
                                            .stream()
                                            .map(entry -> CompletedPart.builder()
                                                    .partNumber(entry.getKey())
                                                    .eTag(entry.getValue())
                                                    .build()
                                            )
                                            .collect(Collectors.toList())
                                    )
                                    .build())
                            .build()
            );
            assertEquals(key, completeMultipartUploadResponse.key());
            assertEquals(getBucketName(), completeMultipartUploadResponse.bucket());
            assertEquals(String.format("/%s/%s", getBucketName(), key), completeMultipartUploadResponse.location());
            eTag = completeMultipartUploadResponse.eTag();
            assertNotNull(eTag);
        }
        HttpClientRecorder.HttpRecord record = recorder.stopAndGetRecords().get(0);
        {        // 请求头与响应头判定
            SdkHttpRequest req = record.request.httpRequest();
            checkPublicRequestHeader(req);
            assertEquals(SdkHttpMethod.POST, req.method());
            assertEquals(String.format("/%s/%s", getBucketName(), key), req.encodedPath());
            assertEquals(String.format("uploadId=%s", uploadId), req.encodedQueryParameters().orElseThrow());

            SdkHttpResponse resp = record.response.httpResponse();
            checkPublicResponseHeader(resp);
            assertEquals(200, resp.statusCode());
            assertEquals("OK", resp.statusText().orElseThrow());
        }

        // 获取文件内容，判断是否与上传的文件内容一致
        {
            ResponseInputStream<GetObjectResponse> ris = object.getObject(GetObjectRequest.builder()
                    .bucket(getBucketName())
                    .key(key)
                    .build()
            );

            GetObjectResponse response = ris.response();
            assertEquals(eTag, response.eTag());
            assertEquals(contentType, response.contentType());
            assertEquals((long) bytesList.size() * (long) partSize, response.contentLength());

            // 拼接bytesList的所有byte
            byte[] bytes1 = new byte[bytesList.size() * partSize];
            for (int i = 0; i < bytesList.size(); i++) {
                System.arraycopy(bytesList.get(i), 0, bytes1, i * partSize, partSize);
            }
            assertArrayEquals(bytes1, ris.readAllBytes());
        }
    }

    /**
     * 上传一个分片，然后拷贝2次分片
     */
    @Test
    public void testMultipartCopyUpload() throws IOException {
        String key = "testMultipartCopyUploadFile";
        String contentType = ContentType.APPLICATION_OCTET_STREAM.getMimeType();
        int parts = 2;
        int partSize = 5 * 1024 * 1024;
        CreateMultipartUploadResponse createMultipartUploadResponse = object.createMultipartUpload(
                CreateMultipartUploadRequest.builder()
                        .key(key)
                        .bucket(getBucketName())
                        .contentType(contentType)
                        .build()
        );
        String uploadId = createMultipartUploadResponse.uploadId();
        byte[] bytes = randomBytes(partSize);

        // 上传一个文件
        object.putObject(
                PutObjectRequest.builder()
                        .bucket(getBucketName())
                        .key(key)
                        .contentType(contentType)
                        .build(),
                RequestBody.fromBytes(bytes)
        );

        Map<Integer, String> partNumber2ETag = new HashMap<>(parts);
        for (int i = 1; i <= parts; i++) {
            // 拷贝分片
            recorder.startRecording();
            {
                UploadPartCopyResponse copyPartResponse = object.uploadPartCopy(
                        UploadPartCopyRequest.builder()
                                .uploadId(uploadId)
                                .partNumber(i)
                                .sourceBucket(getBucketName())
                                .sourceKey(key)
                                .destinationBucket(getBucketName())
                                .destinationKey(key)
                                .copySourceRange(String.format("bytes=%d-%d", 0, partSize - 1))
                                .build()
                );
                assertNotNull(copyPartResponse.copyPartResult().eTag());
                assertNotNull(copyPartResponse.copyPartResult().lastModified());
                partNumber2ETag.put(i, copyPartResponse.copyPartResult().eTag());
            }
            HttpClientRecorder.HttpRecord record = recorder.stopAndGetRecords().get(0);
            {
                // 请求头与响应头判定
                SdkHttpRequest req = record.request.httpRequest();
                checkPublicRequestHeader(req);
                assertEquals(SdkHttpMethod.PUT, req.method());
                assertEquals(String.format("/%s/%s", getBucketName(), key), req.encodedPath());
                assertEquals(String.format("partNumber=%d&uploadId=%s", i, uploadId), req.encodedQueryParameters().orElseThrow());

                SdkHttpResponse resp = record.response.httpResponse();
                checkPublicResponseHeader(resp);
                assertEquals(200, resp.statusCode());
                assertEquals("OK", resp.statusText().orElseThrow());
            }
        }


        // 完成分片上传
        CompleteMultipartUploadResponse completeMultipartUploadResponse = object.completeMultipartUpload(
                CompleteMultipartUploadRequest.builder()
                        .bucket(getBucketName())
                        .key(key)
                        .uploadId(uploadId)
                        .multipartUpload(CompletedMultipartUpload.builder()
                                .parts(partNumber2ETag.entrySet()
                                        .stream()
                                        .map(entry -> CompletedPart.builder()
                                                .partNumber(entry.getKey())
                                                .eTag(entry.getValue())
                                                .build()
                                        )
                                        .collect(Collectors.toList())
                                )
                                .build()
                        )
                        .build()
        );

        // 获取文件内容，判断是否与上传的文件内容一致
        {
            ResponseInputStream<GetObjectResponse> ris = object.getObject(GetObjectRequest.builder()
                    .bucket(getBucketName())
                    .key(key)
                    .build()
            );

            GetObjectResponse response = ris.response();
            assertEquals(completeMultipartUploadResponse.eTag(), response.eTag());
            assertEquals(contentType, response.contentType());
            assertEquals((long) bytes.length * (long) parts, response.contentLength());

            // 拼接bytesList的所有byte
            byte[] bytes1 = new byte[bytes.length * parts];
            for (int i = 0; i < parts; i++) {
                System.arraycopy(bytes, 0, bytes1, i * partSize, partSize);
            }
            assertArrayEquals(bytes1, ris.readAllBytes());
        }
    }

    @Test
    public void testAbortMultipartUpload() {
        String key = "testAbortMultipartUploadFile";
        String contentType = ContentType.APPLICATION_OCTET_STREAM.getMimeType();
        int partSize = 5 * 1024 * 1024;
        CreateMultipartUploadResponse createMultipartUploadResponse = object.createMultipartUpload(
                CreateMultipartUploadRequest.builder()
                        .key(key)
                        .bucket(getBucketName())
                        .contentType(contentType)
                        .build()
        );
        String uploadId = createMultipartUploadResponse.uploadId();
        byte[] bytes = randomBytes(partSize);

        // 上传一个文件
        object.putObject(
                PutObjectRequest.builder()
                        .bucket(getBucketName())
                        .key(key)
                        .contentType(contentType)
                        .build(),
                RequestBody.fromBytes(bytes)
        );

        // 拷贝分片
        UploadPartCopyResponse copyPartResponse = object.uploadPartCopy(
                UploadPartCopyRequest.builder()
                        .uploadId(uploadId)
                        .partNumber(1)
                        .sourceBucket(getBucketName())
                        .sourceKey(key)
                        .destinationBucket(getBucketName())
                        .destinationKey(key)
                        .copySourceRange(String.format("bytes=%d-%d", 0, partSize - 1))
                        .build()
        );
        assertNotNull(copyPartResponse.copyPartResult().eTag());
        assertNotNull(copyPartResponse.copyPartResult().lastModified());

        // 取消分片上传
        object.abortMultipartUpload(
                AbortMultipartUploadRequest.builder()
                        .bucket(getBucketName())
                        .key(key)
                        .uploadId(uploadId)
                        .build()
        );
    }

    /**
     * 拷贝两个分片，再上传一个分片，然后列举文件级别正在进行的分片
     */
    @Test
    public void testListParts() {
        String key = "testListParts";
        String contentType = ContentType.APPLICATION_OCTET_STREAM.getMimeType();
        int parts = 3;
        int partSize = 5 * 1024 * 1024;
        CreateMultipartUploadResponse createMultipartUploadResponse = object.createMultipartUpload(
                CreateMultipartUploadRequest.builder()
                        .key(key)
                        .bucket(getBucketName())
                        .contentType(contentType)
                        .build()
        );
        String uploadId = createMultipartUploadResponse.uploadId();
        byte[] bytes = randomBytes(partSize);

        // 上传一个文件
        object.putObject(
                PutObjectRequest.builder()
                        .bucket(getBucketName())
                        .key(key)
                        .contentType(contentType)
                        .build(),
                RequestBody.fromBytes(bytes)
        );

        Map<Integer, String> partNumber2ETag = new HashMap<>(parts);
        for (int i = 1; i < parts; i++) {
            // 拷贝分片
            UploadPartCopyResponse copyPartResponse = object.uploadPartCopy(
                    UploadPartCopyRequest.builder()
                            .uploadId(uploadId)
                            .partNumber(i)
                            .sourceBucket(getBucketName())
                            .sourceKey(key)
                            .destinationBucket(getBucketName())
                            .destinationKey(key)
                            .copySourceRange(String.format("bytes=%d-%d", 0, partSize - 1))
                            .build()
            );
            partNumber2ETag.put(i, copyPartResponse.copyPartResult().eTag());
        }

        // 再上传一个分片
        UploadPartResponse uploadPartResponse = object.uploadPart(
                UploadPartRequest.builder()
                        .uploadId(uploadId)
                        .partNumber(parts)
                        .bucket(getBucketName())
                        .key(key)
                        .build(),
                RequestBody.fromBytes(bytes)
        );
        partNumber2ETag.put(parts, uploadPartResponse.eTag());


        // 列举文件级别正在进行的分片
        ListPartsResponse response = object.listParts(ListPartsRequest.builder()
                .bucket(getBucketName())
                .key(key)
                .uploadId(uploadId)
                .maxParts(parts)
                .build()
        );
        assertEquals(parts, response.parts().size());
        for (Part part : response.parts()) {
            assertEquals(partNumber2ETag.get(part.partNumber()), part.eTag());
            assertNotNull(part.lastModified());
            assertEquals(partSize, part.size());
        }
        assertEquals(getBucketName(), response.bucket());
        assertEquals(key, response.key());
        assertEquals(uploadId, response.uploadId());
        assertEquals(parts, response.maxParts());
        assertFalse(response.isTruncated());
        assertEquals(StorageClass.STANDARD, response.storageClass());
    }

    /**
     * 创建两个不同key的分片上传任务，第一个任务上传一个分片，第二个任务上传两个分片，然后列举bucket级别正在进行的分片
     */
    @Test
    public void testListMultipartUploads() {
        String key = "testKey";
        String prefix = "testListMultipartUploadsFile-";
        String key1 = prefix + "1";
        String key2 = prefix + "2";
        String contentType = ContentType.APPLICATION_OCTET_STREAM.getMimeType();
        int partSize = 5 * 1024 * 1024;
        byte[] bytes = randomBytes(partSize);
        // 上传一个文件
        object.putObject(
                PutObjectRequest.builder()
                        .bucket(getBucketName())
                        .key(key)
                        .contentType(contentType)
                        .build(),
                RequestBody.fromBytes(bytes)
        );

        CreateMultipartUploadResponse createMultipartUploadResponse1 = object.createMultipartUpload(
                CreateMultipartUploadRequest.builder()
                        .key(key1)
                        .bucket(getBucketName())
                        .contentType(contentType)
                        .build()
        );
        String uploadId1 = createMultipartUploadResponse1.uploadId();

        // 拷贝分片
        object.uploadPartCopy(
                UploadPartCopyRequest.builder()
                        .uploadId(uploadId1)
                        .partNumber(1)
                        .sourceBucket(getBucketName())
                        .destinationBucket(getBucketName())
                        .sourceKey(key)
                        .destinationKey(key1)
                        .copySourceRange(String.format("bytes=%d-%d", 0, partSize - 1))
                        .build()
        );

        CreateMultipartUploadResponse createMultipartUploadResponse2 = object.createMultipartUpload(
                CreateMultipartUploadRequest.builder()
                        .key(key2)
                        .bucket(getBucketName())
                        .contentType(contentType)
                        .build()
        );
        String uploadId2 = createMultipartUploadResponse2.uploadId();

        for (int i = 1; i <= 2; i++) {
            // 拷贝分片
            object.uploadPartCopy(
                    UploadPartCopyRequest.builder()
                            .uploadId(uploadId2)
                            .partNumber(i)
                            .sourceBucket(getBucketName())
                            .sourceKey(key)
                            .destinationBucket(getBucketName())
                            .destinationKey(key2)
                            .copySourceRange(String.format("bytes=%d-%d", 0, partSize - 1))
                            .build()
            );
        }

        // 列举bucket级别正在进行的分片
        ListMultipartUploadsResponse response = object.listMultipartUploads(
                ListMultipartUploadsRequest.builder()
                        .bucket(getBucketName())
                        .prefix(prefix)
                        .maxUploads(1000)
                        .build()
        );

        List<MultipartUpload> multipartUploads = new ArrayList<>(response.uploads());
        while (response.isTruncated()) {
            response = object.listMultipartUploads(ListMultipartUploadsRequest.builder()
                    .bucket(getBucketName())
                    .prefix(prefix)
                    .uploadIdMarker(response.nextUploadIdMarker())
                    .keyMarker(response.nextKeyMarker())
                    .maxUploads(1000)
                    .build()
            );
            multipartUploads.addAll(response.uploads());
        }

        // 必须要在multipartUploads中找到两个正在进行的两个分片上传任务
        // 并且第一个分片上传任务只有一个分片，第二个分片上传任务只有两个分片
        Set<MultipartUpload> upload1 = multipartUploads.stream()
                .filter(x -> x.uploadId().equals(uploadId1))
                .collect(Collectors.toSet());
        assertEquals(1, upload1.size());
        for (MultipartUpload upload : upload1) {
            assertEquals(key1, upload.key());
            assertNotNull(upload.initiated());
        }

        Set<MultipartUpload> upload2 = multipartUploads.stream()
                .filter(x -> x.uploadId().equals(uploadId2))
                .collect(Collectors.toSet());
        assertEquals(1, upload2.size());
        for (MultipartUpload upload : upload2) {
            assertEquals(key2, upload.key());
            assertNotNull(upload.initiated());
        }
    }
}
