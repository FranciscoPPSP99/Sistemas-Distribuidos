## Passo 13: Falhas provocadas

| Falha introduzida | Lado | Exceção | Momento | O que permitiu concluir |
|---|---|---|---|---|
| Cliente sem servidor | Cliente | `ConnectException` (`IO: Connection refused`) | Ligação (`new Socket`) | Ninguém estava à escuta no porto 7896. Não diz porquê (servidor desligado, porto errado, etc.) |
| `Place` sem `Serializable` | Cliente (causa) e servidor | Cliente: `NotSerializableException`. Servidor: `WriteAbortedException` | Escrita no cliente (`writeObject`); leitura no servidor (`readObject`) | Basta uma classe do grafo não ser serializável para a escrita toda falhar. As duas mensagens nomeiam `tcp01.Place` |
| `serialVersionUID` diferente | Servidor | `InvalidClassException` | Leitura (`readObject`) | A classe existe, mas a versão é incompatível. O cliente só vê um `EOF` e não descobre a causa |
| `Person` noutro pacote no servidor | Servidor | `ClassNotFoundException` (cliente: `EOFException`) | Leitura (`readObject`) | Os bytes chegaram intactos, mas o Java identifica a classe pelo nome completo (pacote incluído) e não a encontrou |

## Passo 14: Construções usadas

| Construção | Onde a usei | O que ficou garantido | O que continua por garantir |
|---|---|---|---|
| `ServerSocket` / `accept()` | `TCPServer.main`, dentro do `while (true)` | Aceitar vários clientes no mesmo porto, cada um com o seu `Socket` | Que o cliente envie algo depois de ligado, e que o `accept()` não fique preso por trabalho feito na thread principal |
| `Connection extends Thread` | Classe `Connection`, criada no `accept()`; a thread arranca no `start()` | Cada cliente é atendido em paralelo, sem bloquear os outros | Que aguente muitos clientes (uma thread por cliente) e que o construtor não prenda a thread principal |
| `implements Serializable` | Classes `Person` e `Place`, nos dois projetos | O Java converte estes objetos em bytes e reconstrói-os | Que o outro lado tenha a classe (nome e pacote), que o UID seja compatível, que todo o grafo seja serializável, e que os dados façam sentido |
| `ObjectOutputStream` / `ObjectInputStream` | Output no `TCPClient`; Input na `Connection` | Enviar e reconstruir um objeto inteiro (com dependências) numa só chamada | Que o outro lado tenha a classe, que o tipo seja o esperado (cast), que outra linguagem o leia, e que a criação do Input não bloqueie |
| `serialVersionUID` | `Person` e `Place`, com valor `1L`, nos dois projetos | Versões com UID diferente são rejeitadas (`InvalidClassException`) | Que os valores sejam iguais nos dois lados sem tu os manteres, e que UID igual signifique estrutura compatível (um atributo novo passa sem erro) |
| Referência para `Place` | Atributo `place` na `Person` | O `writeObject` envia o `Place` automaticamente junto com a `Person` | Que o `Place` seja serializável, que dê para escolher o que não enviar, e que o volume de dados seja pequeno |