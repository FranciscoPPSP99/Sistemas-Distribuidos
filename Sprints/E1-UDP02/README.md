# Sprint E1-UDP02 — Ordenação e Retenção de Mensagens UDP

Implementação de um servidor UDP que guarda temporariamente mensagens recebidas fora de ordem e as entrega em cascata quando faltas são recebidas.

Como funciona
- O formato das mensagens é `seq,payload`, por exemplo `1,ola`.
- O servidor guarda mensagens cuja sequência não é a próxima esperada numa estrutura temporária (`buffer`).
- Quando chega a próxima sequência, o servidor entrega-a e verifica o `buffer` para entregar em cascata as mensagens consecutivas.
- Se a mensagem não for entregue, o servidor responde `waitingfor,<L+1>`, em que `L` é a última mensagem entregue em ordem.
- Mensagens malformadas recebem `invalidmessage` e não alteram o estado do servidor.

Ficheiros
- `src/UDPServer.java` — servidor UDP com buffer para mensagens fora de ordem.
- `src/UDPClient.java` — cliente com modos automático, aleatório e manual.

Compilar
```bash
javac src/UDPServer.java src/UDPClient.java
```

Executar servidor (porta por omissão 9876):
```bash
java -cp src UDPServer [porta]
```

Executar cliente:
```bash
java -cp src UDPClient <host> <porta> <total> [shuffle|manual]
```

Exemplos
- Enviar 10 mensagens em ordem:
```bash
java -cp src UDPClient localhost 9876 10
```
- Enviar 10 mensagens em ordem aleatória (simula out-of-order):
```bash
java -cp src UDPClient localhost 9876 10 shuffle
```
- Escolher manualmente cada sequência e payload:
```bash
java -cp src UDPClient localhost 9876 5 manual
```
No modo manual, introduza `1`, `3`, `4`, `2`, `3` como sequências para reproduzir o teste da ficha.

Validação
- Observe que o servidor imprime `DELIVERED <seq> -> <payload>` quando entrega mensagens por ordem.
- O cliente retransmite mensagens quando recebe `waitingfor,<n>` do servidor.
- No modo manual, o cliente mostra cada resposta e não retransmite automaticamente; assim, a ordem de teste é preservada.
- O estado impresso pelo servidor inclui `L`, o buffer, as mensagens entregues nesse passo e a lista completa de mensagens entregues.

Resultados da Verificação
| Mensagem recebida | Resposta | L depois | Buffer depois | Entregue neste passo |
|---|---|---:|---|---|
| `1,ola` | `1,ola` | 1 | vazio | `ola` |
| `3,mundo` | `waitingfor,2` | 1 | `{3=mundo}` | `nada` |
| `4,tudo bem` |`waitingfor,2`| 1 | {3=mundo, 4=tudo bem} | `nada`|
| `2,cruel` | `2,cruel` | 4 | vazio |`cruel, mundo, tudo bem` |
| `3,mundo` | `waitingfor,5` | 4 | {3=mundo} | `nada` |

Notas
- Esta implementação é um exemplo pedagógico; para produção: tratar timeouts, limpeza de buffer, confirmação/ACK, e volumes maiores de dados.
