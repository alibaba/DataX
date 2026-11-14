package com.alibaba.datax.core.util;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.container.CoreConstant;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 数据库密码解密
 *
 * @Author weizhao.dong
 * @Date 2023/3/23 14:37
 * @Version 1.0
 */
public class JobDataBasePwdDecryptUtil {
    private static final Logger LOG = LoggerFactory.getLogger(JobDataBasePwdDecryptUtil.class);

    public static void decrypt(Configuration configuration) {
        if (configuration.getBool(CoreConstant.DATAX_JOB_SETTING_PASSWD_ENCRYPT, false)) {
            String readerPwd = configuration.getString(CoreConstant.DATA_JOB_READER_PARAMETER_PASSWORD);
            String writePwd = configuration.getString(CoreConstant.DATA_JOB_WRITER_PARAMETER_PASSWORD);
            //加密key
            String key = SecretUtil.getSecurityProperties().getProperty(CoreConstant.DB_ENCRYPT_KEY);
            if (StringUtils.isEmpty(key)) {
                LOG.warn("{} is empty，use original password", CoreConstant.DB_ENCRYPT_KEY);
                return;
            }
            if (StringUtils.isNotEmpty(readerPwd)) {
                configuration.set(CoreConstant.DATA_JOB_READER_PARAMETER_PASSWORD, SecretUtil.decrypt3DES(readerPwd, key));
            }
            if (StringUtils.isNotEmpty(writePwd)) {
                configuration.set(CoreConstant.DATA_JOB_WRITER_PARAMETER_PASSWORD, SecretUtil.decrypt3DES(writePwd, key));
            }
        }
    }
}
