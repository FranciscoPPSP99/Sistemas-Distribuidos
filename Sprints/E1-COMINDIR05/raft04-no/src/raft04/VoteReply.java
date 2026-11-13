package raft04;
 
/** Mensagem VOTO;<termo>;<eleitor>;<candidato>;<S|N>. */
public final class VoteReply {
    public final long term;
    public final int voter;
    public final int candidate;
    public final boolean granted;
 
    public VoteReply(long term, int voter, int candidate, boolean granted) {
        if (term < 0) throw new IllegalArgumentException("termo negativo");
        if (voter < 1) throw new IllegalArgumentException("eleitor inválido: " + voter);
        if (candidate < 1) throw new IllegalArgumentException("candidato inválido: " + candidate);
        this.term = term;
        this.voter = voter;
        this.candidate = candidate;
        this.granted = granted;
    }
 
    public String encode() {
        return "VOTO;" + term + ";" + voter + ";" + candidate + ";" + (granted ? "S" : "N");
    }
 
    public static VoteReply decode(String msg) {
        String[] f = msg.trim().split(";", -1);
        if (f.length != 5 || !f[0].equals("VOTO")) {
            throw new IllegalArgumentException("VOTO mal formado: " + msg.trim());
        }
        String decisao = f[4].trim();
        if (!decisao.equals("S") && !decisao.equals("N")) {
            throw new IllegalArgumentException("decisão do VOTO tem de ser S ou N: " + msg.trim());
        }
        return new VoteReply(Long.parseLong(f[1].trim()), Integer.parseInt(f[2].trim()),
                Integer.parseInt(f[3].trim()), decisao.equals("S"));
    }
 
    @Override public String toString() { return encode(); }
}
