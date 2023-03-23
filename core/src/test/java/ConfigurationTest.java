import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.JobDataBasePwdDecryptUtil;
import com.alibaba.datax.core.util.SecretUtil;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.fastjson2.JSON;
import org.junit.Test;

import java.io.File;

/**
 * @Author weizhao.dong
 * @Date 2023/3/22 18:26
 * @Version 1.0
 */
public class ConfigurationTest {
    String key = "1qaz2wsx";
    String nativePassword = "12345678";

    /**
     * 测试密码解密
     */
    @Test
    public void dbEncryptPasswrdTest() {
        System.setProperty("datax.home",System.getProperty("user.dir")+"/src/test/resources");
        Configuration configuration = Configuration.from(new File("src/test/resources/dwd_g2park_inout_report_s.json"));
        JobDataBasePwdDecryptUtil.decrypt(configuration);
        assert configuration.getString(CoreConstant.DATA_JOB_READER_PARAMETER_PASSWORD).equals(nativePassword);
        assert configuration.getString(CoreConstant.DATA_JOB_WRITER_PARAMETER_PASSWORD).equals(nativePassword);
    }

    /**
     * 生成加密密码
     */
    @Test
    public void generateEncryptPassword() {
        //密码为：ZnOZROJwLiMeI3FQluEhHg==
        System.out.println(SecretUtil.encrypt3DES(nativePassword, key));
    }
}
