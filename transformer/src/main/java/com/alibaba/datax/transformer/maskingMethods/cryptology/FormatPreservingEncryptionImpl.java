package com.alibaba.datax.transformer.maskingMethods.cryptology;

import com.idealista.fpe.FormatPreservingEncryption;
import com.idealista.fpe.builder.FormatPreservingEncryptionBuilder;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/**
 * Format Preserving Encryption
 *
 * @author Wenyan Liu
 */
public class FormatPreservingEncryptionImpl {

    FormatPreservingEncryption formatPreservingEncryption;

    public void setup() throws NoSuchAlgorithmException {

//        KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
//        keyGenerator.init(RandomValuesProvider.getRandomKeyLength());
//        byte[] key = keyGenerator.generateKey().getEncoded();

        // TODO: anyKey
        byte[] anyKey = new byte[]{
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x01, (byte) 0x01, (byte) 0x01, (byte) 0x01,
                (byte) 0x02, (byte) 0x02, (byte) 0x02, (byte) 0x02,
                (byte) 0x03, (byte) 0x03, (byte) 0x03, (byte) 0x03
        };

        formatPreservingEncryption = FormatPreservingEncryptionBuilder
                .ff1Implementation()
                .withDefaultDomain()
                .withDefaultPseudoRandomFunction(anyKey)
                .withDefaultLengthRange()
                .build();
    }

    public FormatPreservingEncryptionImpl() throws NoSuchAlgorithmException{
            setup();
    }

    // TODO: Capital letter & numbers
    public String execute(String plainText) throws NoSuchAlgorithmException {
        return formatPreservingEncryption.encrypt(plainText, new byte[0]);
    }

    public List<String> execute(List<String> originData) throws NoSuchAlgorithmException {
        List<String> resValue = new ArrayList<String>();
        for (String data : originData) {
            resValue.add(execute(data));
        }
        return resValue;
    }

}
