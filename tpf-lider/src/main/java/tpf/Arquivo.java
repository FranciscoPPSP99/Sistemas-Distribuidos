package tpf;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;

final class Arquivo {
    private final Path log;
    private final Path eventos;
    private final Path selados;
    private final byte[] chave;
    private final ScheduledExecutorService relogio;
    private Chunk aberto = new Chunk();
    private ScheduledFuture<?> prazo;
    private long ultimoLsn;
    private boolean falhou;

    Arquivo(Path pasta, byte[] chave, ScheduledExecutorService relogio) throws IOException {
        if (Wire.CHUNK_MAX_BYTES <= 0 || Wire.PAD_BYTES <= 0 || Wire.SELAGEM_MS <= 0) {
            throw new IllegalArgumentException("Os limites de chunk e selagem devem ser positivos");
        }
        this.log = pasta.resolve("registos.log");
        this.eventos = pasta.resolve("eventos-lider.txt");
        this.selados = Files.createDirectories(pasta.resolve("selados"));
        this.chave = chave.clone();
        this.relogio = relogio;
        retomar();
    }

    synchronized long registar(String autor, String canal, String texto) throws IOException {
        if (falhou) {
            throw new IOException("Arquivo indisponível. Consulta o terminal e reinicia após corrigir a causa.");
        }
        Registo registo = new Registo(Math.incrementExact(ultimoLsn), Instant.now(), autor, canal, texto);
        try {
            acrescentar(log, registo.linha());
            ultimoLsn = registo.lsn();
            evento("REGISTO", "lsn=" + ultimoLsn);
            adicionar(registo);
            return ultimoLsn;
        } catch (IOException failure) {
            falhou = true;
            throw failure;
        }
    }

    private void adicionar(Registo registo) throws IOException {
        boolean primeiro = aberto.vazio();
        aberto.adicionar(registo);
        if (aberto.tamanho() >= Wire.CHUNK_MAX_BYTES) {
            selar();
        } else if (primeiro) {
            Chunk chunk = aberto;
            long idade = Math.max(0, Duration.between(registo.instante(), Instant.now()).toMillis());
            prazo = relogio.schedule(() -> selarPorTempo(chunk),
                    Math.max(0, Wire.SELAGEM_MS - idade), TimeUnit.MILLISECONDS);
        }
    }

    private synchronized void selarPorTempo(Chunk chunk) {
        if (falhou || aberto != chunk) {
            return;
        }
        try {
            selar();
        } catch (IOException failure) {
            falhou = true;
            failure.printStackTrace(System.err);
        }
    }

    private void selar() throws IOException {
        Sealed sealed;
        try {
            sealed = ChunkCrypto.seal(chave, aberto.serializar());
        } catch (GeneralSecurityException failure) {
            throw new IOException("Não foi possível cifrar o chunk", failure);
        }
        Base64.Encoder base64 = Base64.getEncoder();
        String header = sealed.keyVersion() + ";" + base64.encodeToString(sealed.salt())
                + ";" + base64.encodeToString(sealed.nonce()) + "\n";
        publicar(selados.resolve(sealed.id() + ".hdr"), header.getBytes(UTF_8));
        byte[] cifrado = sealed.cifrado();
        publicar(selados.resolve(sealed.id() + ".bin"), cifrado);
        evento("SELADO", "chunkId=" + sealed.id() + " lsn=" + aberto.primeiro() + ".."
                + aberto.ultimo() + " tamanho=" + cifrado.length);
        if (prazo != null) {
            prazo.cancel(false);
            prazo = null;
        }
        aberto = new Chunk();
    }

    private void retomar() throws IOException {
        // Só os pares publicados com .bin representam chunks concluídos.
        Map<Long, Long> intervalos = new TreeMap<>();
        try (var ficheiros = Files.newDirectoryStream(selados, "*.bin")) {
            for (Path ficheiro : ficheiros) {
                String nome = ficheiro.getFileName().toString();
                List<Registo> registos = abrir(nome.substring(0, nome.length() - 4));
                long primeiro = registos.get(0).lsn();
                long ultimo = primeiro - 1;
                for (Registo registo : registos) {
                    if (registo.lsn() != ++ultimo) {
                        throw new IOException("Chunk com LSN fora de sequência: " + nome);
                    }
                }
                if (intervalos.putIfAbsent(primeiro, ultimo) != null) {
                    throw new IOException("Chunks publicados com o mesmo primeiro LSN");
                }
            }
        }
        long ultimoSelado = 0;
        for (var intervalo : intervalos.entrySet()) {
            if (intervalo.getKey() != ultimoSelado + 1) {
                throw new IOException("Os chunks publicados não formam uma sequência contínua");
            }
            ultimoSelado = intervalo.getValue();
        }
        removerLinhaIncompleta();
        if (Files.exists(log)) {
            try (var leitor = Files.newBufferedReader(log, UTF_8)) {
                String linha;
                while ((linha = leitor.readLine()) != null) {
                    Registo registo = Registo.ler(linha);
                    if (registo.lsn() != ++ultimoLsn) {
                        throw new IOException("LSN fora de sequência em registos.log");
                    }
                }
            }
        }
        if (ultimoSelado > ultimoLsn) {
            throw new IOException("Existem chunks que ultrapassam o último LSN no log");
        }
        if (ultimoSelado < ultimoLsn) {
            // O timer pode correr de imediato para registos antigos, após reconstruir a cauda.
            synchronized (this) {
                try (var leitor = Files.newBufferedReader(log, UTF_8)) {
                    String linha;
                    while ((linha = leitor.readLine()) != null) {
                        Registo registo = Registo.ler(linha);
                        if (registo.lsn() > ultimoSelado) {
                            adicionar(registo);
                        }
                    }
                }
            }
        }
    }

    List<Registo> abrir(String id) throws IOException {
        if (!id.matches("[0-9a-f]{64}")) {
            throw new IOException("Identificador de chunk inválido");
        }
        try {
            String[] campos = Files.readString(selados.resolve(id + ".hdr"), UTF_8).strip().split(";", -1);
            if (campos.length != 3) {
                throw new IOException("Cabeçalho de chunk inválido: " + id);
            }
            Base64.Decoder base64 = Base64.getDecoder();
            Sealed sealed = new Sealed(id, Integer.parseInt(campos[0]), base64.decode(campos[1]),
                    base64.decode(campos[2]), Files.readAllBytes(selados.resolve(id + ".bin")));
            return Chunk.ler(ChunkCrypto.open(chave, sealed));
        } catch (GeneralSecurityException | IllegalArgumentException failure) {
            throw new IOException("Não foi possível abrir o chunk " + id, failure);
        }
    }

    private void removerLinhaIncompleta() throws IOException {
        if (!Files.exists(log)) {
            return;
        }
        try (RandomAccessFile ficheiro = new RandomAccessFile(log.toFile(), "rw")) {
            long fim = ficheiro.length();
            long posicao = fim;
            while (posicao > 0) {
                ficheiro.seek(posicao - 1);
                if (ficheiro.read() == '\n') {
                    break;
                }
                posicao--;
            }
            if (posicao != fim) {
                ficheiro.setLength(posicao);
                ficheiro.getChannel().force(true);
            }
        }
    }

    private void evento(String tipo, String detalhe) throws IOException {
        acrescentar(eventos, Instant.now() + ";" + tipo + ";" + detalhe);
    }

    private static void acrescentar(Path destino, String linha) throws IOException {
        try (FileChannel canal = FileChannel.open(destino, CREATE, WRITE, APPEND)) {
            escrever(canal, (linha + "\n").getBytes(UTF_8));
        }
    }

    private static void publicar(Path destino, byte[] bytes) throws IOException {
        Path temporario = Files.createTempFile(destino.getParent(), ".chunk-", ".tmp");
        try {
            try (FileChannel canal = FileChannel.open(temporario, WRITE)) {
                escrever(canal, bytes);
            }
            Files.move(temporario, destino, ATOMIC_MOVE);
        } finally {
            Files.deleteIfExists(temporario);
        }
    }

    private static void escrever(FileChannel canal, byte[] bytes) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        while (buffer.hasRemaining()) {
            canal.write(buffer);
        }
        canal.force(true);
    }
}
