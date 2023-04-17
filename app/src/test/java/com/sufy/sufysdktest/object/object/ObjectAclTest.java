package com.sufy.sufysdktest.object.object;

import com.sufy.sdk.services.object.model.GetObjectAclRequest;
import com.sufy.sdk.services.object.model.ObjectException;
import com.sufy.sdk.services.object.model.PutObjectAclRequest;
import org.junit.jupiter.api.Test;
import sufy.util.ObjectTestBase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ObjectAclTest extends ObjectTestBase {
    private static final String KEY = "testObjectAclFileKey";

    // Sufy 签名的两个object acl接口，均返回 501 Not Implemented
    @Test
    public void testGetObjectAcl() {
        ObjectException e = assertThrows(ObjectException.class, () -> {
            object.getObjectAcl(GetObjectAclRequest.builder()
                    .bucket(getBucketName())
                    .key(KEY)
                    .build()
            );
        });
        assertEquals(501, e.statusCode());
    }

    @Test
    public void testPutObjectAcl() {
        ObjectException e = assertThrows(ObjectException.class, () -> {
            object.putObjectAcl(PutObjectAclRequest.builder()
                    .bucket(getBucketName())
                    .key(KEY)
                    .build()
            );
        });
        assertEquals(501, e.statusCode());
    }
}
