package com.alibaba.datax.transformer.maskingMethods.reversiblePermutation;

import com.alibaba.datax.transformer.maskingMethods.utils.math.algorithm.RailFenceUtil;

import java.util.List;

/**
 * 栅栏置换脱敏算法
 */
public class RailFenceEncryptionImpl extends ReversiblePermutationMasking{
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
        return RailFenceUtil.railFenceCipher(originData, k);
    }

    @Override
    public void mask() throws Exception {

    }
}
