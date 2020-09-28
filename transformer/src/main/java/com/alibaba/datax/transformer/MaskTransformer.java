package com.alibaba.datax.transformer;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.transformer.maskingMethods.anonymity.*;
import com.alibaba.datax.transformer.maskingMethods.cryptology.AESEncryptionImpl;
import com.alibaba.datax.transformer.maskingMethods.cryptology.FormatPreservingEncryptionImpl;
import com.alibaba.datax.transformer.maskingMethods.cryptology.RSAEncryptionImpl;
import com.alibaba.datax.transformer.maskingMethods.differentialPrivacy.EpsilonDifferentialPrivacyImpl;
import com.alibaba.datax.transformer.maskingMethods.irreversibleInterference.MD5EncryptionImpl;

import java.util.Arrays;
import java.util.Date;

/**
 * Mask data to protect privacy.
 * Created by Liukun on 18/4/12.
 */

public class MaskTransformer extends  Transformer{
    private Object masker;
    String maskMethodId = "";
    String key;
    int columnIndex;

    public  MaskTransformer(){ setTransformerName("dx_cryp");}

    @Override
    public Record evaluate(Record record, Object... paras) {
        try {
            if (paras.length != 3) {
                throw new RuntimeException("masker paras must be 3");
            }
            if (maskMethodId.equals("")) {
                maskMethodId = String.valueOf((String) paras[1]);
                System.out.println("Using " + maskMethodId + " encryption");
            }
            columnIndex = (Integer) paras[0];
            key = String.valueOf((String) paras[2]);
        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" + Arrays.asList(paras).toString() + " => " + e.getMessage());
        }
        Column column = record.getColumn(columnIndex);
        try {
            String oriValue = column.asString();
            if(oriValue == null){
                return  record;
            }
            if(maskMethodId.equals("AES")){
                String newValue;
                AESEncryptionImpl masker = new AESEncryptionImpl();
                newValue = masker.execute(oriValue);
                record.setColumn(columnIndex, new StringColumn(newValue));
            }
            else if(maskMethodId.equals("FPE")){
                FormatPreservingEncryptionImpl masker = new FormatPreservingEncryptionImpl();
                String newValue = masker.execute(oriValue);
                record.setColumn(columnIndex, new StringColumn(newValue));
            }
            else if(maskMethodId.equals("RSA")){
                RSAEncryptionImpl masker = new RSAEncryptionImpl();
                String newValue = "";
                if (key.equals("private_decrypt")){
                    newValue = masker.executeWithPrivateDecrypt(oriValue, masker.PKCS1);
                }
                else if(key.equals("private_encrypt")){
                    newValue = masker.executeWithPrivateEncrypt(oriValue, masker.PKCS1);
                }
                else if(key.equals("public_decrypt")){
                    newValue = masker.executeWithPublicDecrypt(oriValue, masker.PKCS1);
                }
                else if(key.equals("public_encrypt")){
                    newValue = masker.executeWithPublicEncrypt(oriValue, masker.PKCS1);
                }
                record.setColumn(columnIndex, new StringColumn(newValue));
            }
        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(),e);
        }
        return record;
    }

}
