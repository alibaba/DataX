package cn.sensorsdata.datax.plugin.util;

import cn.sensorsdata.datax.plugin.KeyConstant;
import com.sensorsdata.analytics.javasdk.SensorsAnalytics;
import com.sensorsdata.analytics.javasdk.bean.ItemRecord;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Slf4j
public class ItemSetUtil {

    public static void process(SensorsAnalytics sa, Map<String, Object> properties) {
        String itemItemType = (String) properties.get(KeyConstant.ITEM_ITEM_TYPE);
        Boolean itemTypeIsColumn = (Boolean) properties.get(KeyConstant.ITEM_TYPE_IS_COLUMN);
        String itemItemIdColumn = (String) properties.get(KeyConstant.ITEM_ITEM_ID_COLUMN);
        String itemId = String.valueOf(properties.get(itemItemIdColumn));

        String typeTmp = itemItemType;
        itemItemType = itemTypeIsColumn ? String.valueOf(properties.get(itemItemType)) : itemItemType;
        properties.remove(KeyConstant.ITEM_ITEM_TYPE);
        properties.remove(KeyConstant.ITEM_TYPE_IS_COLUMN);
        properties.remove(KeyConstant.ITEM_ITEM_ID_COLUMN);
        if (itemTypeIsColumn) {
            properties.remove(typeTmp);
        }
        properties.remove(itemItemIdColumn);

        try {
            ItemRecord addRecord = ItemRecord.builder().setItemId(itemId).setItemType(itemItemType)
                    .addProperties(properties)
                    .build();
            sa.itemSet(addRecord);
        } catch (InvalidArgumentException e) {
            log.info("item Exception: {}", e);
            e.printStackTrace();
        }

    }

}
