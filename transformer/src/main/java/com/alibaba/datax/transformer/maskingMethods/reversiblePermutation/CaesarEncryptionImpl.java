package com.alibaba.datax.transformer.maskingMethods.reversiblePermutation;

import com.alibaba.datax.transformer.maskingMethods.utils.math.algorithm.CaesarUtil;

import java.util.List;

/**
 * 可逆置换——替换式Substitution脱敏（凯撒）
 */
public class CaesarEncryptionImpl extends ReversiblePermutationMasking {

    public double execute(double d) throws Exception {
        return 0;
    }

    /**
     * 执行脱敏算法
     * @param originData
     * @param k
     * @return
     * @throws Exception
     */
    public List<String> execute(List<String> originData, int k) throws Exception {
        return CaesarUtil.caesarCipher(originData, k);
    }

    /**
     * 恢复
     * @param encrytionData
     * @param k
     * @return
     * @throws Exception
     */
    public List<String> decryption(List<String> encrytionData, int k) throws Exception {
        return CaesarUtil.caesarCipherDecrypt(encrytionData, k);
    }

    public void mask() throws Exception {

    }
}
