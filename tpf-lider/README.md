# tpf-lider — Sprint E2-01

Base do bot Discord em Java 17 e JDA. Neste sprint, o bot valida os comandos e dá respostas provisórias, incluindo a mensagens em `#arquivo`.

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
mvn clean compile

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

| Comando de teste | Argumentos esperados na resposta provisória |
| --- | --- |
| `/registar texto:Olá` | `texto=Olá` |
| `/estado` | Nenhum |
| `/config r n:3` | `n=3` |
| `/recuperar de:10 ate:20` | `de=10, ate=20` |
| `/replica-token` | Nenhum |
| `/replica-remover id:A` | `id=A` |
| `/rajada n:10 intervaloms:100` | `n=10, intervaloMs=100` |

O Discord exige nomes de opções em minúsculas. A opção `intervaloMs` do enunciado aparece no Discord como `intervaloms`.

- Tenta introduzir `abc` no campo `n` de `/config r`. O Discord rejeita texto num campo inteiro. Envia `n:0` para verificar também a validação feita pelo bot.
- Envia `/recuperar de:20 ate:10` e confirma que o bot recusa o intervalo.
- Com um membro sem `admin-arquivo`, testa `/config`, `/replica-token`, `/replica-remover` e `/rajada`. Cada comando deve devolver uma mensagem de recusa.
- Envia uma mensagem em `#arquivo` e outra noutro canal. Só a primeira deve receber resposta. Se houver outro bot disponível, confirma também que as mensagens dele são ignoradas.
- Confirma que o token está apenas na sessão local e que o `.gitignore` exclui ficheiros locais de segredos, incluindo `master.key`.

[Enunciado do Épico 2](https://github.com/cunhaestgv/SD-2026-2027/issues/19) · [Sprint E2-01](https://github.com/cunhaestgv/SD-2026-2027/issues/20)
