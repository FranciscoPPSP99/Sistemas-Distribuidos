package raft04;
 
/** Mensagem PEDIDO_VOTO;<termo>;<candidato>;<ultimoSeq>;<ultimoTermo>. */
public final class VoteRequest {
    public final long term;
    public final int candidate;
    public final long lastSeq;
    public final long lastTerm;
 
    public VoteRequest(long term, int candidate, long lastSeq, long lastTerm) {
        if (term < 0) throw new IllegalArgumentException("termo negativo");
        if (candidate < 1) throw new IllegalArgumentException("candidato inválido: " + candidate);
        if (lastSeq < 0 || lastTerm < 0) throw new IllegalArgumentException("último registo inválido");
        this.term = term;
        this.candidate = candidate;
        this.lastSeq = lastSeq;
        this.lastTerm = lastTerm;
    }
 
    public String encode() {
        return "PEDIDO_VOTO;" + term + ";" + candidate + ";" + lastSeq + ";" + lastTerm;
    }
 
    public static VoteRequest decode(String msg) {
        String[] f = msg.trim().split(";", -1);
        if (f.length != 5 || !f[0].equals("PEDIDO_VOTO")) {
            throw new IllegalArgumentException("PEDIDO_VOTO mal formado: " + msg.trim());
        }
        return new VoteRequest(Long.parseLong(f[1].trim()), Integer.parseInt(f[2].trim()),
                Long.parseLong(f[3].trim()), Long.parseLong(f[4].trim()));
    }
 
    @Override public String toString() { return encode(); }
}
