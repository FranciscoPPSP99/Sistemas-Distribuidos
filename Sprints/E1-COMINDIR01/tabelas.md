# REP01: Tabelas da tarefa

## Ponto 14: Falhas introduzidas

| Falha introduzida | Mecanismo | Lado onde surgiu | Exceção obtida (ou «nenhuma») | Os ficheiros divergiram? | O que permitiu (ou não) concluir |
|---|---|---|---|---|---|
| Arrancar o líder e registar três registos antes de arrancar as réplicas | Multicast | Nenhum | Nenhuma | Sim: as réplicas nunca têm os 3 primeiros registos e retêm os seguintes. | Falta de desacoplamento no tempo: quem não está à escuta perde a mensagem e ninguém é avisado. |
| Uma réplica com endereço de grupo `230.0.0.2` | Multicast | Nenhum | Nenhuma | Sim: a réplica não recebe nada e o ficheiro fica vazio. | O emissor não sabe quem recebe. Um grupo errado não dá erro, só silêncio. |
| `buffer` da réplica com 10 bytes em vez de 1000 | Multicast | Réplica | Nenhuma (o datagrama é truncado e o registo é rejeitado com `REJEITADO`) | Sim: nenhum registo é aplicado. | O UDP corta em silêncio o que não cabe no buffer. A validação do registo evita escrever lixo. |
| Parar uma réplica a meio de `rajada 1000` e voltar a arrancá-la | Multicast | Nenhum | Nenhuma | Sim: fica retida no primeiro buraco, deixa de escrever e acumula em memória. | O número de sequência deteta o buraco mas não o recupera. Sem retransmissão, a réplica fica retida para sempre. |
| Daemon IPFS do líder parado | PubSub | Líder (no `publish`) | `IOException` (ligação recusada ao porto 5001) | Sim: o líder já escreveu o registo no seu ficheiro, as réplicas não o recebem. | Aqui a falha é visível no líder, ao contrário do multicast. A ordem «escrever e depois propagar» deixa o líder à frente das réplicas. |
| `Pubsub.Enabled` a `false` no nó das réplicas | PubSub | Réplica (no `subscribe`) | `IOException` («sub falhou (...)») | Sim: nada chega às réplicas. | O PubSub é uma funcionalidade experimental que tem de ser ativada. Falha cedo e de forma visível. |
| Uma réplica a subscrever o tópico `rep01-temp` | PubSub | Nenhum | Nenhuma | Sim: a réplica não recebe nada. | Um tópico com o nome errado não dá erro, só silêncio, como o grupo errado no multicast. |
| Dois nós sem ligação entre si (sem `swarm connect`, com `ipfs bootstrap rm --all` e `Discovery.MDNS.Enabled` a `false` nos dois) | PubSub | Nenhum | Nenhuma | Sim: as réplicas ligadas ao outro nó não recebem nada. | O *gossip* só funciona se os nós estiverem ligados. Com os nós isolados, cada um é uma rede à parte e ninguém é avisado. |

## Ponto 15: Construções usadas

| Construção | Onde a usou | O que ficou a ser garantido | O que continua a não ser garantido |
|---|---|---|---|
| `MulticastSocket` / `joinGroup()` | `MulticastReplica` | Um único envio do líder chega a todas as réplicas que estão no grupo. Várias réplicas podem usar o mesmo porto. | Entrega, ordem e ausência de duplicados (é UDP). Quem entra no grupo depois do envio perde a mensagem. |
| `setTimeToLive(1)` | `MulticastLeader` | O datagrama não sai da rede local. | Que chegue a alguém da rede local. |
| `append` local antes da propagação | `MulticastLeader` e `PubSubLeader` | O líder nunca propaga um registo que não tenha no seu ficheiro, por isso as réplicas nunca ficam à frente dele. | Que as réplicas o recebam. Se o líder falhar entre o `append` e o envio, o líder fica com um registo que as réplicas nunca terão. |
| Número de sequência (`seq`) | `SensorRecord` e réplicas | Detetar registos perdidos, duplicados e fora de ordem. O ficheiro só cresce pela ordem do líder. | Recuperar registos perdidos (a réplica fica retida e a memória cresce) e saber que a réplica já tem tudo. |
| Representação textual (`toLine` / `fromLine`) | `SensorRecord` | Um formato legível por qualquer linguagem, com validação de 4 campos e dos tipos. | Que o registo venha mesmo do líder (qualquer programa pode enviar uma linha válida). |
| Tópico `rep01-temperaturas` | `PubSubLeader` e `PubSubReplica` | Desacoplamento no espaço: o líder não conhece as réplicas, e quem subscreve o tópico recebe. | Entrega a quem não estava subscrito, ordem e persistência. Um tópico com o nome errado dá silêncio, sem erro. |
| Daemon IPFS e API HTTP | `IpfsPubSub` | O programa Java só fala HTTP com o daemon local, e o daemon trata do *gossip* e remove duplicados entre nós. | Confirmação de entrega. Se o daemon parar, falha o programa ligado a ele. |
| Codificação multibase | `IpfsPubSub` (`toMultibase` / `fromMultibase`) | O Kubo aceita o nome do tópico e o conteúdo da mensagem com qualquer texto UTF-8. | Integridade e autenticidade (é só codificação, não cifra). |

## Ponto 16: Comparação dos mecanismos

| Métrica | Multicast | PubSub, um nó | PubSub, dois nós |
|---|---|---|---|
| Tempo de envio da rajada no líder (ms) | 1220 | 3210 | 3241 |
| Latência da rajada até à réplica mais lenta (ms) | 1139 | 2895 | 2945 |
| Débito no líder (registos/s) | ≈ 820 | ≈ 311 | ≈ 309 |
| Registos perdidos, por réplica | 0 | 0 | 0 |
| Duplicados detetados e registos retidos em memória no fim | 0 e 0 | 0 e 0 | 0 e 0 |
| Componentes que tiveram de estar a correr além da JVM | Nenhum | 1 daemon Kubo | 2 daemons Kubo (repositórios e portos distintos) |
| Passos de configuração até ao primeiro registo replicado | 0 | 4 | ≈ 14 |
| O que aconteceu à réplica que arrancou atrasada | A réplica 2 não recebeu os registos enviados antes de arrancar. Ficou retida no primeiro buraco e deixou de escrever no ficheiro. Os registos seguintes ficaram em memória (`EM ESPERA`) e o ficheiro divergiu do do líder. Nenhum programa deu erro. | Igual: o PubSub não guarda mensagens, por isso a réplica 2 não recebeu o que foi publicado antes de subscrever. Ficou retida no mesmo ponto. | Igual ao anterior. |
| Funcionaria entre duas redes diferentes sem alterar os *routers*? | Não: o TTL é 1 e a maior parte dos *routers* não encaminha multicast. | Sim: o *gossip* viaja por ligações normais entre nós, que atravessam a Internet. | Sim, pelo mesmo motivo. |

Débito = 1000 registos / tempo de envio médio no líder.
