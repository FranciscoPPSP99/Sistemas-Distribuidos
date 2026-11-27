package tpf;

import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.HexFormat;
import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import static java.nio.charset.StandardCharsets.US_ASCII;

public final class ChunkCrypto {
    private static final int VERSION = 1;
    private static final SecureRandom RANDOM = new SecureRandom();
    private static final byte[] AAD_PREFIX = "tpf-v1".getBytes(US_ASCII);
    private static final byte[] INFO = "tpf-chunk-v1".getBytes(US_ASCII);

    private ChunkCrypto() {}

    public static Sealed seal(byte[] masterKey, byte[] claro) throws GeneralSecurityException {
        byte[] salt = new byte[32];
        byte[] nonce = new byte[12];
        RANDOM.nextBytes(salt);
        RANDOM.nextBytes(nonce);
        byte[] cifrado = crypt(Cipher.ENCRYPT_MODE, masterKey, VERSION, salt, nonce, claro);
        return new Sealed(hash(cifrado), VERSION, salt, nonce, cifrado);
    }

    public static byte[] open(byte[] masterKey, Sealed sealed) throws GeneralSecurityException {
        byte[] cifrado = sealed.cifrado();
        if (!hash(cifrado).equals(sealed.id())) {
            throw new GeneralSecurityException("SHA-256 incompatível com o chunkId");
        }
        if (sealed.keyVersion() != VERSION) {
            throw new GeneralSecurityException("Versão de chave não suportada");
        }
        return crypt(Cipher.DECRYPT_MODE, masterKey, sealed.keyVersion(), sealed.salt(), sealed.nonce(), cifrado);
    }

    private static byte[] crypt(int mode, byte[] masterKey, int version, byte[] salt, byte[] nonce, byte[] dados)
            throws GeneralSecurityException {
        if (masterKey.length != 32 || salt.length != 32 || nonce.length != 12) {
            throw new GeneralSecurityException("Comprimento inválido de chave, salt ou nonce");
        }
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(salt, "HmacSHA256"));
        byte[] prk = mac.doFinal(masterKey);
        mac.init(new SecretKeySpec(prk, "HmacSHA256"));
        mac.update(INFO);
        byte[] dek = mac.doFinal(new byte[]{1});

        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(mode, new SecretKeySpec(dek, "AES"), new GCMParameterSpec(128, nonce));
        cipher.updateAAD(ByteBuffer.allocate(AAD_PREFIX.length + Integer.BYTES + salt.length + nonce.length)
                .put(AAD_PREFIX).putInt(version).put(salt).put(nonce).array());
        return cipher.doFinal(dados);
    }

    private static String hash(byte[] bytes) throws GeneralSecurityException {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }
}
