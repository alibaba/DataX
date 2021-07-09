package cn.sensorsdata.datax.plugin.util;

import cn.sensorsdata.datax.plugin.KeyConstant;
import com.sensorsdata.analytics.javasdk.SensorsAnalytics;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Slf4j
public class EventUtil {

    public static void process(SensorsAnalytics sa, Map<String, Object> properties) {
        String eventDistinctIdCol = (String) properties.get(KeyConstant.EVENT_DISTINCT_ID_COL);
        Boolean eventIsLoginId = (Boolean) properties.get(KeyConstant.EVENT_IS_LOGIN_ID);
        String eventEventName = (String) properties.get(KeyConstant.EVENT_EVENT_NAME);

        String distinctId = String.valueOf(properties.get(eventDistinctIdCol));
        properties.remove(eventDistinctIdCol);

        try {
            properties.remove(KeyConstant.EVENT_DISTINCT_ID_COL);
            properties.remove(KeyConstant.EVENT_IS_LOGIN_ID);
            properties.remove(KeyConstant.EVENT_EVENT_NAME);
            sa.track(distinctId, eventIsLoginId, eventEventName, properties);
        } catch (Exception e) {
            log.error("Event Exception: {}", e);
            e.printStackTrace();
        }
    }
}
