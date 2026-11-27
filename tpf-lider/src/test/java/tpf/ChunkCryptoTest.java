package tpf;

import java.security.GeneralSecurityException;

import org.junit.Test;
import static org.junit.Assert.*;

public class ChunkCryptoTest {
    @Test
    public void abreOriginalERejeitaAlteracoes() throws Exception {
        byte[] chave = new byte[32];
        byte[] claro = {1, 2, 3, 4};
        Sealed s = ChunkCrypto.seal(chave, claro);
        assertEquals(32, s.salt().length);
        assertEquals(12, s.nonce().length);
        assertEquals(claro.length + 16, s.cifrado().length);
        assertArrayEquals(claro, ChunkCrypto.open(chave, s));

        byte[] cifrado = s.cifrado();
        cifrado[0] ^= 1;
        assertThrows(GeneralSecurityException.class, () -> ChunkCrypto.open(chave,
                new Sealed(s.id(), s.keyVersion(), s.salt(), s.nonce(), cifrado)));

        byte[] salt = s.salt();
        salt[0] ^= 1;
        assertThrows(GeneralSecurityException.class, () -> ChunkCrypto.open(chave,
                new Sealed(s.id(), s.keyVersion(), salt, s.nonce(), s.cifrado())));

        byte[] nonce = s.nonce();
        nonce[0] ^= 1;
        assertThrows(GeneralSecurityException.class, () -> ChunkCrypto.open(chave,
                new Sealed(s.id(), s.keyVersion(), s.salt(), nonce, s.cifrado())));

        assertThrows(GeneralSecurityException.class, () -> ChunkCrypto.open(chave,
                new Sealed(s.id(), 2, s.salt(), s.nonce(), s.cifrado())));

        byte[] outraChave = chave.clone();
        outraChave[0] ^= 1;
        assertThrows(GeneralSecurityException.class, () -> ChunkCrypto.open(outraChave, s));
    }
}
