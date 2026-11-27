package tpf;

import java.io.IOException;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import net.dv8tion.jda.api.JDA;
import net.dv8tion.jda.api.JDABuilder;
import net.dv8tion.jda.api.entities.Guild;
import net.dv8tion.jda.api.entities.Member;
import net.dv8tion.jda.api.entities.channel.ChannelType;
import net.dv8tion.jda.api.events.interaction.command.SlashCommandInteractionEvent;
import net.dv8tion.jda.api.events.message.MessageReceivedEvent;
import net.dv8tion.jda.api.hooks.ListenerAdapter;
import net.dv8tion.jda.api.interactions.commands.OptionMapping;
import net.dv8tion.jda.api.interactions.commands.OptionType;
import net.dv8tion.jda.api.interactions.commands.build.Commands;
import net.dv8tion.jda.api.interactions.commands.build.OptionData;
import net.dv8tion.jda.api.interactions.commands.build.SubcommandData;
import net.dv8tion.jda.api.requests.GatewayIntent;

public final class LeaderBot extends ListenerAdapter {
    private static final String ARCHIVE_CHANNEL = "arquivo";
    private static final String ADMIN_ROLE = "admin-arquivo";
    private static final Set<String> ADMIN_COMMANDS =
            Set.of("config", "replica-token", "replica-remover", "rajada");

    private final String guildId;

    private final Arquivo arquivo;
    private final ExecutorService escritas = Executors.newSingleThreadExecutor();
    private final ExecutorService rajadas = Executors.newSingleThreadExecutor();

    private LeaderBot(String guildId, Arquivo arquivo) {
        this.guildId = guildId;
        this.arquivo = arquivo;
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        String token = requiredEnvironmentVariable("DISCORD_BOT_TOKEN");
        String guildId = requiredEnvironmentVariable("DISCORD_GUILD_ID");
        if (!guildId.matches("[0-9]{17,20}")) {
            throw new IllegalArgumentException("DISCORD_GUILD_ID deve conter o ID do servidor.");
        }

        byte[] chave = MasterKey.loadOrCreate(Path.of("master.key"));
        var relogio = Executors.newSingleThreadScheduledExecutor();
        LeaderBot bot;
        try {
            bot = new LeaderBot(guildId, new Arquivo(Path.of("."), chave, relogio));
        } catch (IOException | RuntimeException failure) {
            relogio.shutdownNow();
            throw failure;
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            bot.escritas.shutdownNow();
            bot.rajadas.shutdownNow();
            relogio.shutdownNow();
        }));
        JDA jda = null;
        try {
            jda = JDABuilder.createLight(token,
                            EnumSet.of(GatewayIntent.GUILD_MESSAGES, GatewayIntent.MESSAGE_CONTENT))
                    .addEventListeners(bot)
                    .build();
            jda.awaitReady();
            Guild guild = jda.getGuildById(guildId);
            if (guild == null) {
                throw new IllegalStateException("Servidor de teste indisponível. Confirma o ID e o convite do bot.");
            }
            bot.registerCommands(guild);
            System.out.println("LeaderBot online. Sete comandos registados no servidor " + guild.getName() + ".");
        } catch (InterruptedException | RuntimeException failure) {
            if (jda != null) {
                jda.shutdownNow();
            }
            bot.escritas.shutdownNow();
            bot.rajadas.shutdownNow();
            relogio.shutdownNow();
            throw failure;
        }
    }

    private static String requiredEnvironmentVariable(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Define a variável de ambiente " + name + " antes de arrancar.");
        }
        return value.strip();
    }

    private void registerCommands(Guild guild) {
        // A aplicação de teste é dedicada a este bot. A lista do servidor é atualizada em cada arranque.
        guild.updateCommands().addCommands(
                Commands.slash("registar", "Receber um texto para registo")
                        .addOptions(textOption("texto", "Texto do registo")),
                Commands.slash("estado", "Consultar o estado do arquivo"),
                Commands.slash("config", "Configurar o arquivo")
                        .addSubcommands(new SubcommandData("r", "Definir o fator de replicação")
                                .addOption(OptionType.INTEGER, "n", "Fator de replicação", true)),
                Commands.slash("recuperar", "Pedir um intervalo de registos")
                        .addOption(OptionType.INTEGER, "de", "Primeiro LSN", true)
                        .addOption(OptionType.INTEGER, "ate", "Último LSN", true),
                Commands.slash("replica-token", "Pedir um token de registo de réplica"),
                Commands.slash("replica-remover", "Pedir a remoção de uma réplica")
                        .addOptions(textOption("id", "Identificador da réplica")),
                Commands.slash("rajada", "Pedir uma rajada de registos de teste")
                        .addOption(OptionType.INTEGER, "n", "Número de registos", true)
                        .addOption(OptionType.INTEGER, "intervaloms", "Intervalo em milissegundos", true)
        ).complete();
    }

    private static OptionData textOption(String name, String description) {
        return new OptionData(OptionType.STRING, name, description, true).setMaxLength(1800);
    }

    @Override
    public void onSlashCommandInteraction(SlashCommandInteractionEvent event) {
        if (!event.isFromGuild() || !guildId.equals(event.getGuild().getId())) {
            return;
        }

        String command = event.getName();
        Member member = event.getMember();
        if (ADMIN_COMMANDS.contains(command)
                && (member == null || member.getRoles().stream().noneMatch(role -> ADMIN_ROLE.equals(role.getName())))) {
            reply(event, "Este comando requer o papel `admin-arquivo`.", true);
            return;
        }

        try {
            if ("registar".equals(command)) {
                registarComando(event, textArgument(event, "texto"));
                return;
            }
            if ("rajada".equals(command)) {
                rajada(event, integerArgument(event, "n", 1), integerArgument(event, "intervaloms", 0));
                return;
            }
            String arguments = switch (command) {
                case "estado", "replica-token" -> "";
                case "config" -> "n=" + integerArgument(event, "n", 1);
                case "recuperar" -> recoveryArguments(event);
                case "replica-remover" -> "id=" + textArgument(event, "id");
                default -> throw new IllegalArgumentException("Comando desconhecido.");
            };
            String commandPath = "config".equals(command) ? "config r" : command;
            String details = arguments.isEmpty() ? "" : " (" + arguments + ")";
            reply(event, "`/" + commandPath + "` recebido" + details + ". Ainda por implementar.", false);
        } catch (IllegalArgumentException failure) {
            reply(event, "Erro: " + failure.getMessage(), true);
        }
    }

    private void registarComando(SlashCommandInteractionEvent event, String texto) {
        String autor = event.getUser().getId();
        String canal = event.getChannel().getId();
        event.deferReply().queue(hook -> escritas.execute(() -> {
            String resposta;
            try {
                resposta = "Registado com LSN " + arquivo.registar(autor, canal, texto) + ".";
            } catch (IOException failure) {
                resposta = erroDeEscrita(failure);
            }
            hook.editOriginal(resposta).queue();
        }));
    }

    private void rajada(SlashCommandInteractionEvent event, long n, long intervaloMs) {
        String autor = event.getUser().getId();
        String canal = event.getChannel().getId();
        event.reply("Rajada de " + n + " registos agendada.").setEphemeral(true).queue(hook ->
                rajadas.execute(() -> {
                    long escritos = 0;
                    try {
                        while (escritos < n) {
                            String texto = "Registo sintético de rajada " + (escritos + 1) + " "
                                    + "0123456789abcdef".repeat(20);
                            arquivo.registar(autor, canal, texto);
                            escritos++;
                            if (escritos < n && intervaloMs > 0) {
                                Thread.sleep(intervaloMs);
                            }
                        }
                        hook.editOriginal("Rajada concluída. Registos confirmados: " + escritos + ".").queue();
                    } catch (IOException failure) {
                        hook.editOriginal("Rajada interrompida após " + escritos + " confirmações. "
                                + erroDeEscrita(failure)).queue();
                    } catch (InterruptedException failure) {
                        Thread.currentThread().interrupt();
                    }
                }));
    }

    private static String erroDeEscrita(IOException failure) {
        failure.printStackTrace(System.err);
        return "Não foi possível confirmar o registo. Consulta o terminal do líder.";
    }

    private static String recoveryArguments(SlashCommandInteractionEvent event) {
        long from = integerArgument(event, "de", 1);
        long to = integerArgument(event, "ate", 1);
        if (from > to) {
            throw new IllegalArgumentException("`de` deve ser menor ou igual a `ate`.");
        }
        return "de=" + from + ", ate=" + to;
    }

    private static long integerArgument(SlashCommandInteractionEvent event, String name, long minimum) {
        OptionMapping argument = event.getOption(name);
        if (argument == null || argument.getType() != OptionType.INTEGER || argument.getAsLong() < minimum) {
            throw new IllegalArgumentException("`" + name + "` deve ser um inteiro maior ou igual a " + minimum + ".");
        }
        return argument.getAsLong();
    }

    private static String textArgument(SlashCommandInteractionEvent event, String name) {
        String value = event.getOption(name, "", OptionMapping::getAsString);
        if (value.isBlank()) {
            throw new IllegalArgumentException("`" + name + "` não pode estar vazio.");
        }
        return value;
    }

    private static void reply(SlashCommandInteractionEvent event, String message, boolean ephemeral) {
        event.reply(message).setEphemeral(ephemeral).setAllowedMentions(List.of()).queue();
    }

    @Override
    public void onMessageReceived(MessageReceivedEvent event) {
        if (!event.isFromGuild() || !guildId.equals(event.getGuild().getId())
                || event.getAuthor().isBot() || event.isWebhookMessage()
                || event.getChannelType() != ChannelType.TEXT
                || !ARCHIVE_CHANNEL.equals(event.getChannel().getName())) {
            return;
        }
        String autor = event.getAuthor().getId();
        String canal = event.getChannel().getId();
        String texto = event.getMessage().getContentRaw();
        escritas.execute(() -> {
            String resposta;
            try {
                resposta = "Registado com LSN " + arquivo.registar(autor, canal, texto) + ".";
            } catch (IOException failure) {
                resposta = erroDeEscrita(failure);
            }
            event.getChannel().sendMessage(resposta).queue();
        });
    }
}
