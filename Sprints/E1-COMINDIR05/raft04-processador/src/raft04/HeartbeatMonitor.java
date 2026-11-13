package raft04;
 
import java.util.function.Consumer;
 
/**
 * Detetor de falhas passivo: cada LOTE recebido é um heartbeat.
 * Com eleição, a liderança identifica-se pelo termo (e já não pela incarnação).
 */
public final class HeartbeatMonitor {
    public enum State { DESCONHECIDO, ATIVO, SUSPEITO }
 
    private final int partition;
    private final long timeoutNanos;
    private final Consumer<String> log;
 
    private State state = State.DESCONHECIDO;
    private long lastNanos;   // instante local da última receção válida
    private long term = -1;   // maior termo observado
    private int leader = 0;   // líder desse termo (0 = desconhecido)
    private long hb = 0;      // último heartbeat observado nesse termo
 
    public HeartbeatMonitor(int partition, long periodMs, int factor, Consumer<String> log) {
        this.partition = partition;
        this.timeoutNanos = periodMs * factor * 1_000_000L;
        this.log = log;
        this.lastNanos = System.nanoTime();
    }
 
    // thread de receção: chegou um LOTE da partição
    public synchronized void onHeartbeat(long term, int leader, long hb) {
        if (term < this.term) return;                      // líder obsoleto: ignorado
        if (term == this.term) {
            if (hb <= this.hb) return;                     // duplicado ou atrasado
            if (hb != this.hb + 1) {
                log.accept("HB_PERDIDO;p" + partition + ";termo=" + term
                        + ";esperado=" + (this.hb + 1) + ";recebido=" + hb);
            }
        } else {                                           // termo superior: nova liderança
            log.accept("NOVO_LIDER;p" + partition + ";termo=" + term + ";lider=n" + leader
                    + ";anterior=" + (this.term < 0 ? "-" : this.term + "/n" + this.leader));
        }
        if (state == State.SUSPEITO) {
            log.accept("RETOMA;p" + partition + ";termo=" + term + ";hb=" + hb);
        }
        this.term = term;
        this.leader = leader;
        this.hb = hb;
        this.lastNanos = System.nanoTime();
        this.state = State.ATIVO;
    }
 
    // thread de verificação: chamada a cada VERIFICACAO_MS
    public synchronized void check() {
        long elapsed = System.nanoTime() - lastNanos;
        if (state != State.SUSPEITO && elapsed > timeoutNanos) {
            state = State.SUSPEITO;
            log.accept("SUSPEITA;p" + partition + ";termo=" + term + ";lider=n" + leader
                    + ";silencio_ms=" + elapsed / 1_000_000);
        }
    }
 
    public synchronized State state() { return state; }
    public synchronized long term()   { return term; }
    public synchronized int leader()  { return leader; }
}
