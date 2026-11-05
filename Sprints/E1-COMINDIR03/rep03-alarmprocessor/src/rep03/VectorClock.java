package rep03;

import java.util.Arrays;
import java.util.stream.Collectors;

/** Relogio vetorial: uma componente por particao, contadores logicos nao negativos. */
public final class VectorClock {
    private final long[] values;

    public VectorClock(int size) {
        if (size < 1) throw new IllegalArgumentException("O vetor tem de ter pelo menos uma componente");
        this.values = new long[size];
    }

    private VectorClock(long[] values) {
        this.values = values.clone();
        for (long value : this.values) {
            if (value < 0) throw new IllegalArgumentException("Contador vetorial negativo");
        }
    }

    public static VectorClock zeros(int size) { return new VectorClock(size); }

    public static VectorClock parse(String text) {
        if (text == null || text.isBlank()) throw new IllegalArgumentException("Vetor vazio");
        String s = text.trim().replace("[", "").replace("]", "")
                .replace("(", "").replace(")", "").replaceAll("\\s+", "");
        String[] parts = s.split(",", -1);
        long[] values = new long[parts.length];
        for (int i = 0; i < parts.length; i++) {
            values[i] = Long.parseLong(parts[i]);
            if (values[i] < 0) throw new IllegalArgumentException("Contador vetorial negativo");
        }
        return new VectorClock(values);
    }

    public int size() { return values.length; }
    public long get(int i) { return values[i]; }

    public void increment(int localPartition) {
        checkIndex(localPartition);
        values[localPartition]++;
    }

    /** Atualiza cada componente para max(local, recebido). */
    public void mergeMax(VectorClock other) {
        checkSameSize(other);
        for (int i = 0; i < values.length; i++) values[i] = Math.max(values[i], other.values[i]);
    }

    public VectorClock copy() { return new VectorClock(values); }

    /** Verdadeiro se cada componente deste vetor <= componente correspondente de other. */
    public boolean lessThanOrEqual(VectorClock other) {
        checkSameSize(other);
        for (int i = 0; i < values.length; i++) if (values[i] > other.values[i]) return false;
        return true;
    }

    /** Aconteceu-antes estrito: <= em todas as componentes e < em pelo menos uma. */
    public boolean happensBefore(VectorClock other) {
        checkSameSize(other);
        boolean strictlyLess = false;
        for (int i = 0; i < values.length; i++) {
            if (values[i] > other.values[i]) return false;
            if (values[i] < other.values[i]) strictlyLess = true;
        }
        return strictlyLess;
    }

    public boolean concurrentWith(VectorClock other) {
        checkSameSize(other);
        return !equals(other) && !happensBefore(other) && !other.happensBefore(this);
    }

    private void checkIndex(int i) {
        if (i < 0 || i >= values.length) throw new IndexOutOfBoundsException("Particao: " + i);
    }

    private void checkSameSize(VectorClock other) {
        if (other == null || values.length != other.values.length)
            throw new IllegalArgumentException("Relogios vetoriais com dimensoes diferentes");
    }

    /** Campo serializado, sem espacos: 0,1,0. */
    public String toField() {
        return Arrays.stream(values).mapToObj(Long::toString).collect(Collectors.joining(","));
    }

    @Override public String toString() { return "[" + toField().replace(",", ", ") + "]"; }
    @Override public boolean equals(Object obj) {
         if (this == obj) return true;
         if (!(obj instanceof VectorClock)) return false;
        
         VectorClock other = (VectorClock) obj;
         return Arrays.equals(values, other.values);
    }
    @Override public int hashCode() { return Arrays.hashCode(values); }
}
