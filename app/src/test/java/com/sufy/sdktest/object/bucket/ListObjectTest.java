package com.sufy.sdktest.object.bucket;

import com.sufy.sdk.services.object.model.*;
import com.sufy.sdktest.object.ObjectTestBase;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ListObjectTest extends ObjectTestBase {
    @Test
    public void testListObjects() {
        String prefix = "test-list-objects-";
        List<String> keys = new ArrayList<>();
        // 准备 10 个文件
        for (int i = 0; i < 10; i++) {
            prepareTestFile(prefix + i, prefix + i);
            keys.add(prefix + i);
        }

        /*
         * TODO：此处报错如下
         *  Unable to unmarshall response (Unable to parse date : 2023-04-13T09:16:56.000Z).
         *  Response Code: 200, Response Text: OK
         *  这个时间来自于文件列表中文件的lastModified字段
         *  https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
         * */
        ListObjectsResponse listObjectsResponse1 = object.listObjects(ListObjectsRequest.builder()
                .bucket(getBucketName())
                .prefix(prefix)
                .maxKeys(7)
                .build()
        );
        assertEquals(7, listObjectsResponse1.contents().size());

        ListObjectsResponse listObjectsResponse2 = object.listObjects(ListObjectsRequest.builder()
                .bucket(getBucketName())
                .prefix(prefix)
                .marker(listObjectsResponse1.nextMarker())
                .maxKeys(7)
                .build()
        );
        assertEquals(3, listObjectsResponse2.contents().size());

        // 删除所有文件
        keys.forEach(key -> {
            object.deleteObject(DeleteObjectRequest.builder()
                    .bucket(getBucketName())
                    .key(key)
                    .build()
            );
        });
    }

    @Test
    public void testListObjectsV2() {
        String prefix = "test-list-objects-v2-";
        List<String> keys = new ArrayList<>();
        // 准备 10 个文件
        for (int i = 0; i < 10; i++) {
            prepareTestFile(prefix + i, prefix + i);
            keys.add(prefix + i);
        }

        // TODO: Unable to unmarshall response (Unable to parse date : 2023-04-13T09:28:21.000Z).
        // TODO: Response Code: 200, Response Text: OK
        ListObjectsV2Response listObjectsV2Response1 = object.listObjectsV2(ListObjectsV2Request.builder()
                .bucket(getBucketName())
                .prefix(prefix)
                .maxKeys(7)
                .build()
        );
        assertEquals(7, listObjectsV2Response1.contents().size());

        ListObjectsV2Response listObjectsResponse2 = object.listObjectsV2(ListObjectsV2Request.builder()
                .bucket(getBucketName())
                .prefix(prefix)
                .continuationToken(listObjectsV2Response1.nextContinuationToken())
                .maxKeys(7)
                .build()
        );
        assertEquals(3, listObjectsResponse2.contents().size());

        // 删除所有文件
        keys.forEach(key -> {
            object.deleteObject(DeleteObjectRequest.builder()
                    .bucket(getBucketName())
                    .key(key)
                    .build()
            );
        });
    }

}