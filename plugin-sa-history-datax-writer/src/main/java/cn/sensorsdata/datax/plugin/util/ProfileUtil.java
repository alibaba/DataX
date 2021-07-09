package cn.sensorsdata.datax.plugin.util;

import cn.sensorsdata.datax.plugin.KeyConstant;
import com.sensorsdata.analytics.javasdk.SensorsAnalytics;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Slf4j
public class ProfileUtil {

    public static void process(SensorsAnalytics sa, Map<String, Object> properties) {
        String userDistinctId = (String) properties.get(KeyConstant.USER_DISTINCT_ID);
        Boolean userIsLoginId = (Boolean) properties.get(KeyConstant.user_is_login_id);

        String distinctId = String.valueOf(properties.get(userDistinctId));

        properties.remove(userDistinctId);
        properties.remove(KeyConstant.USER_DISTINCT_ID);
        properties.remove(KeyConstant.user_is_login_id);
        try {
            sa.profileSet(distinctId, userIsLoginId, properties);
        } catch (Exception e) {
            log.error("user Profile Exception: {}", e);
            e.printStackTrace();
        }
    }


}
