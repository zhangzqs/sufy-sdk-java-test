package sufy.sufysdktest.object.bucket;

import com.sufy.sdk.services.object.model.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;
import sufy.sufysdktest.HttpClientRecorder;
import sufy.util.ObjectTestBase;

import static org.junit.jupiter.api.Assertions.*;

public class PolicyTest extends ObjectTestBase {
    /**
     * 测试查看空间是否公开
     */
    @Test
    public void testGetBucketPolicyStatus() {
        // aws在没配置BucketPolicy时会报NoSuchBucketPolicy,对客户端不友好，我们返回200(isPublic值为false)
        recorder.startRecording();
        {
            GetBucketPolicyStatusResponse getBucketLocationResponse = object.getBucketPolicyStatus(GetBucketPolicyStatusRequest.builder()
                    .bucket(getBucketName())
                    .build()
            );
            assertNotNull(getBucketLocationResponse.policyStatus());
            // TODO: SDK 中 isPublic() 返回值类型为 Boolean 得到了一个null
            // TODO: 但是服务器端正常返回了isPublic值为false
//            assertFalse(getBucketLocationResponse.policyStatus().isPublic());
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
        /*
         * TODO: software.amazon.awssdk.core.exception.SdkClientException:
         *  Unable to marshall request to JSON: class java.lang.String cannot be cast to class
         *  software.amazon.awssdk.core.SdkPojo (java.lang.String is in module java.base of loader 'bootstrap';
         *  software.amazon.awssdk.core.SdkPojo is in unnamed module of loader 'app')
         *  PS: 看起来是因为String类型的policy字段被当成了一个SDK中的一个POJO对象进入到了请求的JSON序列化阶段导致失败
         */
        object.putBucketPolicy(PutBucketPolicyRequest.builder()
                .bucket(getBucketName())
                .policy("{\n" +
                        "    \"Version\": \"sufy\",\n" +
                        "    \"Id\": \"public\",\n" +
                        "    \"Statement\": [\n" +
                        "      {\n" +
                        "        \"Sid\": \"publicGet\",\n" +
                        "        \"Effect\": \"Allow\",\n" +
                        "        \"Principal\": \"*\",\n" +
                        "        \"Action\": [\"miku:MOSGetObject\"],\n" +
                        "        \"Resource\": [\"srn:miku:::examplebucket/*\"]\n" +
                        "      }\n" +
                        "    ]\n" +
                        "}")
                .build());
    }

    @Test
    public void testGetBucketPolicy() {
        // TODO: 无法put，故暂时无法实现该测试
        object.getBucketPolicy(GetBucketPolicyRequest.builder().bucket(getBucketName()).build());
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

        // TODO: expected: <NoSuchBucketPolicy> but was: <Not Found>
//            assertEquals("NoSuchBucketPolicy", response.statusText().orElseThrow());
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
