package com.alibaba.datax.transformer.maskingMethods.cryptology;

import com.alibaba.datax.transformer.maskingMethods.utils.math.algorithm.AESUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * @author liujiaye
 *use AES-128-CBC from JEC
 */
public class AESEncryptionImpl extends CryptologyMasking{

    @Override
    public double execute(double d) throws Exception {
        return 0;
    }

    /**
     * 对参数String进行对称加密
     * @param originData
     * @return
     * @throws Exception
     */
    public String execute(String originData) throws Exception {
	    byte[] cipherBytes = AESUtil.encrypt(originData);
	    return AESUtil.changeCipherBytesToString(cipherBytes);
    }

    /**
     * 对 List<String> originData 进行对称加密
     * @param originData
     * @return
     * @throws Exception
     */
    public List<String> execute(List<String> originData) throws Exception {
        List<String> cipherData = new ArrayList<String>();
        for (String str : originData) {
            String cipherStr = AESUtil.changeCipherBytesToString(AESUtil.encrypt(str));
            cipherData.add(cipherStr);
        }
        return cipherData;
    }

    /**
     * 对加密后的字符串进行解密操作
     * @param cipherData
     * @return
     * @throws Exception
     */
    public String decryption(String cipherData) throws Exception {
	    return AESUtil.decrypt(cipherData.getBytes());
    }

}
