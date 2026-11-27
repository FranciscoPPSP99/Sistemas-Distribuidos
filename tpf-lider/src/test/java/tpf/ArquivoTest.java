package tpf;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import static org.junit.Assert.*;

public class ArquivoTest {
    @Rule public TemporaryFolder temporaria = new TemporaryFolder();

    @Test
    public void escreveRecuperaLsnESelaPorTempo() throws Exception {
        Path pasta = temporaria.newFolder().toPath();
        var relogio = Executors.newSingleThreadScheduledExecutor();
        var novoRelogio = Executors.newSingleThreadScheduledExecutor();
        try {
            byte[] chave = MasterKey.loadOrCreate(pasta.resolve("master.key"));
            Arquivo arquivo = new Arquivo(pasta, chave, relogio);
            String texto = "Olá; teste\nsegunda linha";
            assertEquals(1, arquivo.registar("123", "456", texto));
            var linhas = Files.readAllLines(pasta.resolve("registos.log"));
            assertEquals(1, linhas.size());
            Registo registo = Registo.ler(linhas.get(0));
            assertEquals(texto, registo.texto());

            relogio.shutdownNow();
            Files.writeString(pasta.resolve("registos.log"), "2;linha-partida",
                    java.nio.file.StandardOpenOption.APPEND);
            assertTrue(relogio.awaitTermination(2, TimeUnit.SECONDS));
            Arquivo reaberto = new Arquivo(pasta, chave, novoRelogio);
            Path bin = aguardarSelado(pasta);
            String id = bin.getFileName().toString().replace(".bin", "");
            byte[] cifrado = Files.readAllBytes(bin);
            assertEquals(id, HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(cifrado)));
            assertEquals(0, (cifrado.length - 16) % Wire.PAD_BYTES);
            assertEquals(List.of(registo), reaberto.abrir(id));
            assertEquals(2, reaberto.registar("123", "456", "Segundo"));
            String eventos = Files.readString(pasta.resolve("eventos-lider.txt"));
            assertTrue(eventos.contains(";REGISTO;lsn=1"));
            assertTrue(eventos.contains(";SELADO;chunkId=" + id + " lsn=1..1"));
        } finally {
            relogio.shutdownNow();
            novoRelogio.shutdownNow();
        }
    }

    @Test
    public void selaVariosChunksNumaRajadaDe500Registos() throws Exception {
        Path pasta = temporaria.newFolder().toPath();
        var relogio = Executors.newSingleThreadScheduledExecutor();
        try {
            Arquivo arquivo = new Arquivo(pasta, new byte[32], relogio);
            String texto = "Registo sintético " + "0123456789abcdef".repeat(20);
            for (int lsn = 1; lsn <= 500; lsn++) {
                assertEquals(lsn, arquivo.registar("123", "456", texto));
            }
            List<List<Registo>> chunks;
            try (var ficheiros = Files.newDirectoryStream(pasta.resolve("selados"), "*.bin")) {
                chunks = new java.util.ArrayList<>();
                for (Path bin : ficheiros) {
                    String id = bin.getFileName().toString().replace(".bin", "");
                    assertEquals(0, (Files.size(bin) - 16) % Wire.PAD_BYTES);
                    chunks.add(arquivo.abrir(id));
                }
            }
            assertTrue(chunks.size() >= 2);
            chunks.sort(Comparator.comparingLong(chunk -> chunk.get(0).lsn()));
            long esperado = 1;
            for (List<Registo> chunk : chunks) {
                for (Registo registo : chunk) {
                    assertEquals(esperado++, registo.lsn());
                    assertEquals(texto, registo.texto());
                }
            }
            assertEquals(500, Files.readAllLines(pasta.resolve("registos.log")).size());

        } finally {
            relogio.shutdownNow();
        }
    }

    @Test
    public void reutilizaChaveERejeitaTamanhoInvalido() throws Exception {
        Path chave = temporaria.newFolder().toPath().resolve("master.key");
        byte[] original = MasterKey.loadOrCreate(chave);
        assertEquals(32, original.length);
        assertArrayEquals(original, MasterKey.loadOrCreate(chave));
        Files.write(chave, new byte[31]);
        assertThrows(IOException.class, () -> MasterKey.loadOrCreate(chave));
    }

    private static Path aguardarSelado(Path pasta) throws Exception {
        long prazo = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(Wire.SELAGEM_MS + 3000);
        while (System.nanoTime() < prazo) {
            try (var ficheiros = Files.list(pasta.resolve("selados"))) {
                var selado = ficheiros.filter(p -> p.toString().endsWith(".bin")).findFirst();
                if (selado.isPresent()) {
                    return selado.get();
                }
            }
            Thread.sleep(20);
        }
        throw new AssertionError("O temporizador não selou o chunk");
    }
}
