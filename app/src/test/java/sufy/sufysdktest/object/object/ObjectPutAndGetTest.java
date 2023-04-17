package sufy.sufysdktest.object.object;

import com.sufy.sdk.services.object.model.*;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;
import sufy.sufysdktest.HttpClientRecorder;
import sufy.util.ObjectTestBase;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class ObjectPutAndGetTest extends ObjectTestBase {

    @Test
    public void testPutObject() {
        {
            // 不允许空key
            assertThrows(SdkClientException.class, () -> {
                object.putObject(PutObjectRequest.builder()
                                .bucket(getBucketName())
                                .key("")
                                .build(),
                        RequestBody.empty()
                );
            });
        }
        String key = "testKey1";
        String content = "HelloWorld";
        Map<String, String> metadata = Map.ofEntries(
                Map.entry("testKey1", "testValue1"),
                Map.entry("testKey2", "testValue2")
        );
        String storageClass = "STANDARD";
        recorder.startRecording();
        {
            PutObjectResponse putObjectResponse = object.putObject(PutObjectRequest.builder()
                            .bucket(getBucketName())
                            .key(key)
                            .storageClass(storageClass)
                            .metadata(metadata)
                            .build(),
                    RequestBody.fromString(content)
            );
            assertNotNull(putObjectResponse);
            assertNotNull(putObjectResponse.eTag());
        }

        HttpClientRecorder.HttpRecord record = recorder.stopAndGetRecords().get(0);

        // 请求头与响应头判定
        SdkHttpRequest req = record.request.httpRequest();
        SdkHttpResponse resp = record.response.httpResponse();

        checkPublicRequestHeader(req);
        assertEquals(SdkHttpMethod.PUT, req.method());
        assertEquals(String.format("/%s/%s", getBucketName(), key), req.encodedPath());
        assertEquals(req.headers().get("Content-Length").get(0), String.valueOf(content.length()));

        if (req.headers().containsKey("X-Sufy-Storage-Class")) {
            assertEquals(req.headers().get("X-Sufy-Storage-Class").get(0), "STANDARD");
        }

        assertTrue(req.headers().containsKey("Content-Type"));

        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            assertTrue(req.headers().containsKey("X-Sufy-Meta-" + entry.getKey()));
            assertEquals(req.headers().get("X-Sufy-Meta-" + entry.getKey()).get(0), entry.getValue());
        }

        checkPublicResponseHeader(resp);
        assertEquals(200, resp.statusCode());
        assertEquals("OK", resp.statusText().orElseThrow());
        assertTrue(resp.firstMatchingHeader("ETag").isPresent());
    }

    @Test
    public void testFormUpload() {
        // TODO: SDK未实现表单上传
    }

    @Test
    public void testGetObject() {
        object.getObject(req -> req.bucket(getBucketName()).build());
    }

    @Test
    public void testHeadObject() {
        String key = "testKey1";
        String content = "HelloWorld";
        // TODO: key中包含大写字母时会
        Map<String, String> metadata = Map.ofEntries(
                Map.entry("testKey1", "testValue1"),
                Map.entry("testKey2", "testValue2")
        );
        StorageClass storageClass = StorageClass.DEEP_ARCHIVE;
        PutObjectResponse putObjectResponse = object.putObject(PutObjectRequest.builder()
                        .bucket(getBucketName())
                        .key(key)
                        .storageClass(storageClass)
                        .metadata(metadata)
                        .build(),
                RequestBody.fromString(content)
        );

        recorder.startRecording();
        {
            HeadObjectResponse headBucketResponse = object.headObject(HeadObjectRequest.builder()
                    .bucket(getBucketName())
                    .key(key)
                    .build()
            );
            assertNotNull(headBucketResponse);
            assertEquals(content.length(), headBucketResponse.contentLength());
            assertEquals(putObjectResponse.eTag(), headBucketResponse.eTag());
            assertEquals(storageClass, headBucketResponse.storageClass());

            for (Map.Entry<String, String> entry : headBucketResponse.metadata().entrySet()) {
                assertEquals(entry.getValue(), metadata.get(entry.getKey()));
            }
        }
        HttpClientRecorder.HttpRecord record = recorder.stopAndGetRecords().get(0);
    }
}
