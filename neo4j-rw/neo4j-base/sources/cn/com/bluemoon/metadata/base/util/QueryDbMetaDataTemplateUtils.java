package cn.com.bluemoon.metadata.base.util;

import cn.com.bluemoon.metadata.base.dto.QueryMetaDataTemplateDto;
import cn.com.bluemoon.metadata.common.enums.DbTypeEnum;
import cn.com.bluemoon.metadata.common.exception.DescribeException;
import java.lang.reflect.Field;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

/* loaded from: neo4j-base-1.0-SNAPSHOT.jar:cn/com/bluemoon/metadata/base/util/QueryDbMetaDataTemplateUtils.class */
public class QueryDbMetaDataTemplateUtils {
    public static QueryMetaDataTemplateDto getDbMetaDataTemplate(String type) {
        if (DbTypeEnum.getByCode(type) == null) {
            throw new RuntimeException("当前不支持数据库类型为[" + type + "]的数据源的元数据抓取!");
        }
        String configPath = "/metadata_query_files/relational_" + type + ".xml";
        QueryMetaDataTemplateDto queryMetaDataTemplateDto = new QueryMetaDataTemplateDto();
        try {
            for (Element childElement : new SAXReader().read(QueryDbMetaDataTemplateUtils.class.getResourceAsStream(configPath)).getRootElement().elements()) {
                Field field = QueryMetaDataTemplateDto.class.getDeclaredField(childElement.getName());
                field.setAccessible(true);
                field.set(queryMetaDataTemplateDto, childElement.getText());
            }
            return queryMetaDataTemplateDto;
        } catch (Exception ex) {
            throw new DescribeException(ex.getMessage(), 500);
        }
    }
}
