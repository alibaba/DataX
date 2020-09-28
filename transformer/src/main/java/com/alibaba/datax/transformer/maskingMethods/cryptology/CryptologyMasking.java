package com.alibaba.datax.transformer.maskingMethods.cryptology;

import com.alibaba.datax.transformer.maskingMethods.AbstractMasking;

/**
 * Cryptology Masking.密码学加密接口
 *
 * @author Wenyan Liu
 */
public abstract class CryptologyMasking extends AbstractMasking {

    protected void setup() throws Exception {
    }

    public void mask() throws Exception {
        setup();
    }

    @Override
    public double evaluate() throws Exception {
        // TODO: Cryptology Evaluate
        return -1;
    }

    // 定义三个接口
    // 保形加密


    // AES


    // RSA
}