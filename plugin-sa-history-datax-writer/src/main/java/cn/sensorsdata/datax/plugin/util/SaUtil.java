package cn.sensorsdata.datax.plugin.util;

import cn.hutool.core.util.StrUtil;
import cn.sensorsdata.datax.plugin.KeyConstant;
import com.sensorsdata.analytics.javasdk.SensorsAnalytics;
import com.sensorsdata.analytics.javasdk.consumer.ConcurrentLoggingConsumer;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Map;

@Slf4j
public class SaUtil {

    private static String sdkDataAddress;

    private static volatile SensorsAnalytics instance;

    public static void setSdkDataAddress(String sdkDataAddress) {
        SaUtil.sdkDataAddress = sdkDataAddress;
    }

    /**
     * 初始化sa实体
     *
     * @return
     * @throws IOException
     */
    public static SensorsAnalytics getInstance() {
        if (instance == null) {
            synchronized (SaUtil.class) {
                if (instance == null) {
                    try {
                        instance = new SensorsAnalytics(new ConcurrentLoggingConsumer(sdkDataAddress));
                        log.info("日志生产");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return instance;
                }
            }
        }
        return instance;
    }

    public static void process(SensorsAnalytics sa, String type, Map<String, Object> map) {
        if (StrUtil.isBlank(type)) {
            log.info("sa类型不能为空");
            return;
        }
        if (KeyConstant.TRACK.equalsIgnoreCase(type)) {
            EventUtil.process(sa, map);
        } else if (KeyConstant.USER.equalsIgnoreCase(type)) {
            ProfileUtil.process(sa, map);
        } else if (KeyConstant.ITEM.equalsIgnoreCase(type)) {
            ItemSetUtil.process(sa, map);
        } else {
            log.info("sa类型错误,type:{}", type);
        }

    }


}
