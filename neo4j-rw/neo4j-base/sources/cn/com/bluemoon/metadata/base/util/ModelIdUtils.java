package cn.com.bluemoon.metadata.base.util;

import java.io.InputStream;
import java.util.Properties;

/* loaded from: neo4j-base-1.0-SNAPSHOT.jar:cn/com/bluemoon/metadata/base/util/ModelIdUtils.class */
public class ModelIdUtils {
    private static Properties modelIdProperties;

    static {
        modelIdProperties = null;
        try {
            InputStream inputStream = ModelIdUtils.class.getResourceAsStream("/modelId.properties");
            Properties prop = new Properties();
            prop.load(inputStream);
            modelIdProperties = prop;
        } catch (Exception e) {
            throw new Error("无法加载到模型对应的ID!");
        }
    }

    public static String getModelMoiDataIdPrefix(String label) {
        return modelIdProperties.getProperty(label);
    }
}
