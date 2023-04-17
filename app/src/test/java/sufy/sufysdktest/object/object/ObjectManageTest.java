package sufy.sufysdktest.object.object;

import com.sufy.sdk.services.object.model.*;
import org.junit.jupiter.api.Test;
import sufy.util.ObjectTestBase;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public class ObjectManageTest extends ObjectTestBase {

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
        prepareTestFile(key, "");
        // TODO: 由于Head无法使用
        HeadObjectResponse headObjectResponse = object.headObject(HeadObjectRequest.builder()
                .bucket(getBucketName())
                .key(key)
                .build()
        );
        assertNotNull(headObjectResponse);
        {
            object.deleteObject(DeleteObjectRequest.builder()
                    .bucket(getBucketName())
                    .build()
            );
        }
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
        // TODO: 批量删除操作未删除成功
        //  SDK发出请求的请求体JSON中的key为Object而文档中为objects
        //  服务器端响应200但是返回的JSON中的deleted字段为空列表
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
        RestoreObjectResponse restoreObjectResponse = object.restoreObject(RestoreObjectRequest.builder()
                .bucket(getBucketName())
                .build()
        );
    }

}
