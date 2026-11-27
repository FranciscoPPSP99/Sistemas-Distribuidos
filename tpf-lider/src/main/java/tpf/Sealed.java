package tpf;

public record Sealed(String id, int keyVersion, byte[] salt, byte[] nonce, byte[] cifrado) {
    public Sealed {
        salt = salt.clone();
        nonce = nonce.clone();
        cifrado = cifrado.clone();
    }

    @Override public byte[] salt() { return salt.clone(); }
    @Override public byte[] nonce() { return nonce.clone(); }
    @Override public byte[] cifrado() { return cifrado.clone(); }
}
