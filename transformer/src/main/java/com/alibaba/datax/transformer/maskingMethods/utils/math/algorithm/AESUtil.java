package com.alibaba.datax.transformer.maskingMethods.utils.math.algorithm;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.util.encoders.Hex;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.*;

/**
 * use AES-128-CBC from JEC
 */
public class AESUtil {


    //密钥空间
    private static byte[] keybytes = { 0x70, 0x2F, 0x17, 0x7F, 0x6C, 0x3A, 0x22, 0x11, 0x3F, 0x44, 0x5A, 0x66, 0x77, 0x1A, 0x12, 0x1C };
    //AlgorithmParameterSpec 缓冲区
    private static byte[] iv = { 0x38, 0x37, 0x36, 0x35, 0x34, 0x33, 0x32, 0x31, 0x38, 0x37, 0x36, 0x35, 0x34, 0x33, 0x32, 0x31 };

    private static Key key = new SecretKeySpec(keybytes, "AES");

    public static byte[] encrypt(String plaintext){
        return encrypt(plaintext.getBytes());
    }
    public static byte[] encrypt(byte[] plaintext){
        //encrypt
        Security.addProvider(new BouncyCastleProvider());

        try {
            //use AES-128-CBC from JEC
            Cipher in=Cipher.getInstance("AES/CBC/PKCS7Padding","BC");
            //the third parameter is algorithm parameter space
            in.init(Cipher.ENCRYPT_MODE, key, new IvParameterSpec(iv));
            byte[] cipherBytes=in.doFinal(plaintext);
            return cipherBytes;

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            System.out.println("NoSuchAlgorithm");
        } catch (NoSuchProviderException e) {
            e.printStackTrace();
            System.out.println("NoSuchProvider");
        } catch (NoSuchPaddingException e) {
            e.printStackTrace();
            System.out.println("NoSuchPadding");
        }catch (InvalidKeyException e) {
            e.printStackTrace();
            System.out.println("init-InvalidKey");
        } catch (InvalidAlgorithmParameterException e) {
            e.printStackTrace();
            System.out.println("init-InvalidAlgorithmParameter");
        }catch (IllegalBlockSizeException e) {
            e.printStackTrace();
            System.out.println("dofinal-IllegalBlockSize");
        } catch (BadPaddingException e) {
            e.printStackTrace();
            System.out.println("dofinal-BadPadding");
        }
        return null;
    }
    public static String changeCipherBytesToString(byte[] cipherBytes){
        String cipherText;
        cipherText=new String(Hex.encode(cipherBytes));
        return cipherText;
    }
    public static String decrypt(byte[] cipherBytes){
        String plainText;

        try {
            Cipher out = Cipher.getInstance("AES/CBC/PKCS7Padding","BC");
            out.init(Cipher.DECRYPT_MODE, key,new IvParameterSpec(iv));
            byte[] plainBytes = out.doFinal(cipherBytes);
            plainText=new String(plainBytes);
            return plainText;
        }  catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            System.out.println("NoSuchAlgorithm");
        } catch (NoSuchProviderException e) {
            e.printStackTrace();
            System.out.println("NoSuchProvider");
        } catch (NoSuchPaddingException e) {
            e.printStackTrace();
            System.out.println("NoSuchPadding");
        }catch (InvalidKeyException e) {
            e.printStackTrace();
            System.out.println("init-InvalidKey");
        } catch (InvalidAlgorithmParameterException e) {
            e.printStackTrace();
            System.out.println("init-InvalidAlgorithmParameter");
        }catch (IllegalBlockSizeException e) {
            e.printStackTrace();
            System.out.println("dofinal-IllegalBlockSize");
        } catch (BadPaddingException e) {
            e.printStackTrace();
            System.out.println("dofinal-BadPadding");
        }
        return null;

    }

}
