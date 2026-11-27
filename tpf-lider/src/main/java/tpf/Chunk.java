package tpf;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

final class Chunk {
    private final ByteArrayOutputStream dados = new ByteArrayOutputStream();
    private long primeiro;
    private long ultimo;

    void adicionar(Registo registo) {
        if (vazio()) {
            primeiro = registo.lsn();
        }
        byte[] linha = registo.linha().getBytes(UTF_8);
        dados.writeBytes(ByteBuffer.allocate(Integer.BYTES).putInt(linha.length).array());
        dados.writeBytes(linha);
        ultimo = registo.lsn();
    }

    boolean vazio() { return dados.size() == 0; }
    long primeiro() { return primeiro; }
    long ultimo() { return ultimo; }
    int tamanho() { return Integer.BYTES + dados.size(); }

    byte[] serializar() {
        int tamanho = tamanho();
        int padding = (Wire.PAD_BYTES - tamanho % Wire.PAD_BYTES) % Wire.PAD_BYTES;
        return ByteBuffer.allocate(Math.addExact(tamanho, padding))
                .putInt(dados.size()).put(dados.toByteArray()).array();
    }

    static List<Registo> ler(byte[] claro) {
        ByteBuffer buffer = ByteBuffer.wrap(claro);
        int tamanho = lerTamanho(buffer);
        buffer.limit(buffer.position() + tamanho);
        List<Registo> registos = new ArrayList<>();
        while (buffer.hasRemaining()) {
            byte[] linha = new byte[lerTamanho(buffer)];
            buffer.get(linha);
            registos.add(Registo.ler(new String(linha, UTF_8)));
        }
        return List.copyOf(registos);
    }

    private static int lerTamanho(ByteBuffer buffer) {
        if (buffer.remaining() < Integer.BYTES) {
            throw new IllegalArgumentException("Comprimento em falta no chunk");
        }
        int tamanho = buffer.getInt();
        if (tamanho <= 0 || tamanho > buffer.remaining()) {
            throw new IllegalArgumentException("Comprimento inválido no chunk");
        }
        return tamanho;
    }
}
