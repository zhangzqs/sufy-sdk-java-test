package com.sufy.sufysdktest.object.bucket;

import com.sufy.sdk.services.object.model.ObjectException;
import org.junit.jupiter.api.Test;
import sufy.util.ObjectTestBase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/*
 * Sufy 不支持 Bucket ACL 功能，Sufy SDK中均应当获得501 NotImplemented错误
 * */
public class BucketAclTest extends ObjectTestBase {

    @Test
    public void testPutBucketAcl() {
        ObjectException e = assertThrows(ObjectException.class, () -> {
            object.putBucketAcl(req -> req.bucket(getBucketName()).build());
        });
        assertEquals(501, e.statusCode());
    }

    @Test
    public void testGetBucketAcl() {
        ObjectException e = assertThrows(ObjectException.class, () -> {
            object.getBucketAcl(req -> req.bucket(getBucketName()).build());
        });
        assertEquals(501, e.statusCode());
    }

}
