package com.alibaba.datax.plugin.writer.mongodbwriter.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.mongodb.BasicDBObject;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by changhua.wch on 2018/11/5
 */
public class MongoUtilTest {


    Logger log = LoggerFactory.getLogger(MongoUtilTest.class);


    @Test
    public  void testPutValueWithSubDocumentSupport() {

        BasicDBObject data = new BasicDBObject();

        String fullFieldName = "key.parent.sub.field";

        MongoUtil.putValueWithSubDocumentSupport( data, fullFieldName, "String1-Value");
        MongoUtil.putValueWithSubDocumentSupport( data,  "key.parent.sub1.field", 22L);
        MongoUtil.putValueWithSubDocumentSupport( data,  "key.parent.sub1.fieldB", null);

        MongoUtil.putValueWithSubDocumentSupport( data,  "key.sub12.field3", 56.04);

        String dumpJson = JSON.toJSONString(data, SerializerFeature.PrettyFormat);
        log.info("data: {}", dumpJson);


        BasicDBObject layer1 = (BasicDBObject)data.get("key");
        Assert.assertNotNull( layer1 );

        BasicDBObject layer2 = (BasicDBObject)layer1.get("parent");
        Assert.assertNotNull( layer2 );

        BasicDBObject layer3 = (BasicDBObject)layer2.get("sub");
        Assert.assertNotNull( layer3 );

        String layer4 = layer3.getString("field");
        Assert.assertNotNull( layer4 );
        Assert.assertEquals( "String1-Value", layer4 );
    }

}
