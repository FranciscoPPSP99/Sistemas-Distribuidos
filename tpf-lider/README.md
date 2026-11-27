# tpf-lider — Sprint E2-02

Bot Discord em Java 17 e JDA. O líder grava registos localmente, agrupa-os em chunks e guarda os chunks selados com cifra autenticada.

## 1. Preparar o Discord

1. Cria uma aplicação dedicada no [Discord Developer Portal](https://discord.com/developers/applications) e configura o bot. Em **Bot**, ativa **Message Content Intent** e gera o token. Guarda-o localmente.
2. No servidor de teste do grupo, cria o canal de texto `#arquivo` e atribui o papel `admin-arquivo` aos membros que vão testar os comandos de administração.
3. Convida o bot através da instalação no servidor com os scopes `bot` e `applications.commands`. Seleciona **View Channels**, **Send Messages** e **Read Message History**.
4. No Discord, ativa **Modo de programador** nas definições avançadas. Clica com o botão direito no ícone do servidor de teste e copia o ID.

## 2. Compilar e arrancar

Com JDK 17 ou superior e Maven instalados, abre o PowerShell dentro da pasta `tpf-lider` e executa:

```powershell
java -version
mvn -version
mvn clean test

$env:DISCORD_GUILD_ID = Read-Host "ID do servidor de teste"
$env:DISCORD_BOT_TOKEN = (Get-Credential -UserName "bot" -Message "Cola o token do bot no campo Palavra-passe").GetNetworkCredential().Password
mvn exec:java
```

O arranque confirma no terminal que os comandos foram registados. A variável do token é definida apenas para esta sessão do PowerShell. Depois de parar o bot com Ctrl+C, podes limpar a variável com `Remove-Item Env:DISCORD_BOT_TOKEN`.

Se estiveres a usar o JDK e o Maven extraídos temporariamente durante a compilação neste computador, prepara a sessão com estas linhas antes de executar os comandos anteriores:

```powershell
$env:JAVA_HOME = (Get-ChildItem -LiteralPath "$env:TEMP/e2-jdk17" -Directory | Select-Object -First 1).FullName
$env:Path = "$env:JAVA_HOME/bin;$env:TEMP/e2-maven/apache-maven-3.9.11/bin;$env:Path"
```

## 3. Validar no servidor de teste

Confirma que o bot aparece online. Escreve `/` no Discord e experimenta os sete comandos com uma conta que tenha o papel `admin-arquivo`:

| Comando de teste | Resultado esperado |
| --- | --- |
| `/registar texto:Olá` | Confirmação com o LSN atribuído |
| `/estado` | Nenhum |
| `/config r n:3` | `n=3` |
| `/recuperar de:10 ate:20` | `de=10, ate=20` |
| `/replica-token` | Nenhum |
| `/replica-remover id:A` | `id=A` |
| `/rajada n:10 intervaloms:100` | Agendamento e confirmação de 10 registos |

O Discord exige nomes de opções em minúsculas. A opção `intervaloMs` do enunciado aparece no Discord como `intervaloms`.

- Tenta introduzir `abc` no campo `n` de `/config r`. O Discord rejeita texto num campo inteiro. Envia `n:0` para verificar também a validação feita pelo bot.
- Envia `/recuperar de:20 ate:10` e confirma que o bot recusa o intervalo.
- Com um membro sem `admin-arquivo`, testa `/config`, `/replica-token`, `/replica-remover` e `/rajada`. Cada comando deve devolver uma mensagem de recusa.
- Envia uma mensagem em `#arquivo` e outra noutro canal. Só a primeira deve receber resposta. Se houver outro bot disponível, confirma também que as mensagens dele são ignoradas.
- Confirma que o token está apenas na sessão local e que o `.gitignore` exclui ficheiros locais de segredos, incluindo `master.key`.

## 4. Validar o E2-02

Usa uma pasta de teste local e uma única instância do líder por pasta.

1. Executa `/registar texto:Olá` e anota o LSN. Na pasta do bot, verifica a linha em `registos.log` com `Get-Content .\registos.log -Tail 1`.
2. Reinicia o bot e envia texto em `#arquivo`. O LSN deve ser o anterior mais um. Envia outra mensagem em `#general` e confirma que não aparece uma linha adicional.
3. Com um chunk vazio, envia um só registo e aguarda cerca de 5 segundos. Confirma o par `.bin` e `.hdr` em `selados/` e o evento `SELADO` em `eventos-lider.txt`.
4. Com o cargo `admin-arquivo`, executa `/rajada n:500 intervaloms:0`. Confirma que são produzidos vários chunks e que os intervalos de LSN são consecutivos. Vê os eventos com `Get-Content .\eventos-lider.txt -Tail 12`.
5. Para verificar o nome de um `.bin`, compara-o com o resultado de `Get-FileHash .\selados\<chunkId>.bin -Algorithm SHA256`. Os testes de `mvn test` verificam ainda a abertura e a rejeição de alterações.

## 5. Formatos e leitura do código

- **`Registo` e `Arquivo.registar`.** O líder usa os IDs Discord do autor e do canal. Cada linha UTF-8 de `registos.log` guarda `lsn;instante;autor;canal;textoBase64`, seguida de LF. O instante usa ISO 8601 em UTC e o texto é codificado em Base64 a partir de UTF-8. O método sincronizado atribui o LSN e chama `force(true)` antes de confirmar. Base64 é uma codificação de texto e o log local é legível após descodificação.
- **`Arquivo.retomar`.** Valida os chunks publicados e a sequência do log. Usa o último LSN do log e reconstrói a cauda que ainda não foi selada. Uma linha final sem LF é removida como escrita incompleta. Se a escrita falhar, consulta o terminal antes de reiniciar. Um registo pode ter sido gravado mesmo quando a resposta Discord não chega.
- **`Chunk`.** O formato binário tem um inteiro de 4 bytes com o comprimento total dos dados, seguido de um ou mais pares de comprimento de 4 bytes e linha de registo UTF-8 sem LF. Os inteiros usam ordem big-endian. O padding é zero e completa o chunk até um múltiplo de `PAD_BYTES`. A leitura respeita o comprimento total para ignorar o padding. O limite de tamanho conta os comprimentos e os dados antes do padding. O registo que atinge ou ultrapassa o limite fica no chunk que é selado. O temporizador começa no primeiro registo.
- **`ChunkCrypto`.** Deriva a DEK de 32 bytes da chave mestra e do salt com HKDF-SHA256. AES-256-GCM cifra o chunk com padding e autentica o cabeçalho. `open` verifica o SHA-256, a versão e a autenticação GCM, por esta ordem.
- **Ficheiros selados.** O `.bin` contém os bytes cifrados seguidos da tag de 16 bytes. O SHA-256 hexadecimal minúsculo do `.bin` é o `chunkId`, usado também no nome do `.hdr` correspondente. O `.hdr` é UTF-8 e guarda `keyVersion;saltBase64;nonceBase64` seguido de LF. A versão é 1, com salt de 32 bytes e nonce de 12 bytes. A AAD é `tpf-v1` em ASCII, a versão de 4 bytes big-endian, o salt e o nonce. O tamanho múltiplo de `PAD_BYTES` aplica-se ao chunk antes de cifrar. O `.bin` inclui mais 16 bytes da tag.
- **Eventos.** Cada linha de `eventos-lider.txt` segue `instante;evento;detalhe`. `REGISTO` indica o LSN. `SELADO` indica o ID do chunk, primeiro e último LSN e tamanho do `.bin`.

Os parâmetros podem ser ajustados com `-Dtpf.chunkMaxBytes=65536`, `-Dtpf.padBytes=4096` e `-Dtpf.selagemMs=5000`. Guarda a `master.key` com os dados locais do líder para conseguires voltar a abrir os chunks.

[Enunciado do Épico 2](https://github.com/cunhaestgv/SD-2026-2027/issues/19) · [Sprint E2-02](https://github.com/cunhaestgv/SD-2026-2027/issues/21)
