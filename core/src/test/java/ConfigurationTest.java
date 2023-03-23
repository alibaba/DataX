import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson2.JSON;
import org.junit.Test;

import java.io.File;

/**
 * @Author weizhao.dong
 * @Date 2023/3/22 18:26
 * @Version 1.0
 */
public class ConfigurationTest {
    @Test
    public void configParseTest(){
        Configuration configuration=Configuration.from(new File("/Users/weizhao.dong/Documents/soft/datax_install_d/script/dwd_g2park_inout_report_s.json"));
        System.out.println(JSON.toJSONString(configuration));
    }
}
