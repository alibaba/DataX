package com.alibaba.datax.transformer.maskingConfigure;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemReader;

import java.io.*;
import java.security.*;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;

/**
 * Created by LabUser on 2018/4/24.
 */
public class RSAKey {
    public static final int KEY_SIZE = 1024;

    /*
     * 将公钥和秘钥以.pem文件的形式存储到本地，会覆盖同名文件
     *
     * private key file: id_rsa
     * public key file: id_rsa.pub
     * */
    public static void dumpRSAKeyPair() throws FileNotFoundException, IOException, NoSuchAlgorithmException, NoSuchProviderException {
        Security.addProvider(new BouncyCastleProvider());
        KeyPair keyPair = generateRSAKeyPair();
        RSAPrivateKey priv = (RSAPrivateKey) keyPair.getPrivate();
        RSAPublicKey pub = (RSAPublicKey) keyPair.getPublic();
        writePemFile(priv, "RSA PRIVATE KEY", "id_rsa");
        writePemFile(pub, "RSA PUBLIC KEY", "id_rsa.pub");
    }

    /*
    * Generate RSA KEY PAIRS
    * */
    private static KeyPair generateRSAKeyPair() throws NoSuchAlgorithmException, NoSuchProviderException {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA", "BC");
        generator.initialize(KEY_SIZE);
        KeyPair keyPair = generator.generateKeyPair();
        return keyPair;
    }

    private static void writePemFile(Key key, String description, String filename)
            throws FileNotFoundException, IOException {
        PemFile pemFile = new PemFile(key, description);
        pemFile.write(filename);
    }

    /*
    * 每次随机生成密钥对*/
    public static RSAPrivateKey getPrivateKey() throws Exception{
        try {
            File file = new File("id_rsa");
            if(!file.exists()){
                dumpRSAKeyPair();
            }
            Reader reader = new FileReader("id_rsa");
            PemReader pemReader = new PemReader(reader);
            PemObject pemObject = pemReader.readPemObject();
            byte[] binaries = pemObject.getContent();
            PKCS8EncodedKeySpec privKeySpec = new PKCS8EncodedKeySpec(binaries);
            KeyFactory factory = KeyFactory.getInstance("RSA");
            RSAPrivateKey privateKey = (RSAPrivateKey) factory.generatePrivate(privKeySpec);
//            String privateKeyModulus = privateKey.getModulus().toString();
//            System.out.println(privateKeyModulus);
            return privateKey;
        }
        catch (Exception e){
            System.out.println(e);
        }
        return null;
    }

    public static RSAPublicKey getPublicKey() throws Exception{
        try {
            File file = new File("id_rsa.pub");
            if(!file.exists()){
                dumpRSAKeyPair();
            }
            Reader reader = new FileReader("id_rsa.pub");
            PemReader pemReader = new PemReader(reader);
            PemObject pemObject = pemReader.readPemObject();
            byte[] binaries = pemObject.getContent();
            X509EncodedKeySpec pubKeySpec = new X509EncodedKeySpec(binaries);
            KeyFactory factory = KeyFactory.getInstance("RSA");
            RSAPublicKey publicKey = (RSAPublicKey) factory.generatePublic(pubKeySpec);
            return publicKey;
        }
        catch (Exception e){
            System.out.println(e);
        }
        return null;
    }
}
