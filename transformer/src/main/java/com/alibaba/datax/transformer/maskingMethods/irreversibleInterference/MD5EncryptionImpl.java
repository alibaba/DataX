package com.alibaba.datax.transformer.maskingMethods.irreversibleInterference;

import com.alibaba.datax.transformer.maskingMethods.utils.math.algorithm.MD5Util;

import java.security.NoSuchAlgorithmException;
import java.util.List;

/**
 * MD5实现的不可逆干扰
 */
public class MD5EncryptionImpl extends IrreversibleInterferenceMasking{

    public double execute(double d) throws Exception {
        return 0;
    }

    /**
     * 对String对象执行MD5加密算法
     * @param originData
     * @return
     * @throws Exception
     */
    public String execute(String originData) throws NoSuchAlgorithmException {
        return MD5Util.md5Cipher(originData);
    }

    /**
     * 对List<String>对象执行MD5加密算法
     * @param originData
     * @return
     * @throws Exception
     */
    public List<String> execute(List<String> originData) throws Exception {
        return MD5Util.md5Cipher(originData);
    }

    public void mask() throws Exception {

    }
}
