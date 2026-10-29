# Decisões de arquitetura

## Fronteiras e persistência

O domínio não importa NestJS, MikroORM ou SDK da AWS. Factories criam estado novo e validam transições; `rehydrate` reconstrói estado persistido. HTTP e SQS chamam `Processor.process`, com o mesmo contrato de negócio.

Escolhi MikroORM 6.6.17. A carteira usa `EntitySchema`, Identity Map por transação e `LockMode.PESSIMISTIC_WRITE`. Cada operação cria um fork do EntityManager e usa `transactional()`. As consultas SQL de ledger, transações, inbox e outbox usam o contexto dessa mesma transação. SQL explícito facilita revisar `ON CONFLICT`, índices parciais, `SKIP LOCKED` e constraints sem depender de comportamento implícito do ORM.

Os registros de persistência ficam separados das entidades de domínio. `numeric(18,2)` é lido como string e convertido para `Money.rehydrate`; o domínio nunca recebe um tipo monetário do ORM. Em operações financeiras não existe conversão para `number`. Números JavaScript são usados apenas para contadores, versão, duração, paginação e métricas.

## Dinheiro

`Money` usa decimal.js com precisão de 50 dígitos. Os contratos aceitam somente strings com duas casas, sem sinal, espaços, notação científica ou zeros redundantes à esquerda. Operações internas podem representar diferenças negativas para reconciliação. O saldo tem limite de `9999999999999999.99`, compatível com `numeric(18,2)`.

Não há arredondamento no fluxo: valores com mais de duas casas são rejeitados. A configuração half-even é explícita, mas nenhuma operação aceita perda de escala. A API trabalha com BRL; o value object mantém a moeda e recusa aritmética entre moedas distintas.

`LOSS` exige `0.00`. As outras operações exigem valor positivo. Esta interpretação evita eventos de mudança de saldo para movimentações de valor zero.

## Concorrência

A unidade de serialização é uma linha de `wallets`. O lock é adquirido antes de decidir idempotência e aplicar a movimentação. Instâncias diferentes disputam o mesmo lock no PostgreSQL; carteiras diferentes usam linhas distintas. Não há lock global nem dependência de estado em memória.

Escolhi lock pessimista por ser simples de inspecionar e por evitar ciclos de cálculo especulativo em carteiras disputadas. A transação tem `lock_timeout` de cinco segundos. HTTP devolve erro transitório e a fila tenta novamente quando o lock não fica disponível. A versão começa em 1 e só incrementa quando o saldo muda.

Constraints e triggers verificam no banco:

- Uma carteira por jogador/moeda; saldo não negativo.
- Chave de idempotência globalmente única e identidade externa única por provedor.
- Uma reversão processada por referência e tipo.
- Um lançamento por carteira/transação e por versão.
- Aritmética de cada lançamento, moeda, valor e direção compatíveis com a transação.
- Ledger append-only, inclusive bloqueio de `TRUNCATE`.
- Transações terminais imutáveis e sem exclusão; payload de transações pendentes imutável.
- Correspondência entre transação financeira processada e lançamento.
- Soma do ledger igual ao saldo, cadeia de saldos consistente e última versão igual à carteira.

As verificações entre tabelas são deferred até o commit, pois carteira, transação e ledger são escritos separadamente dentro da mesma transação. Elas reforçam as invariantes mesmo se um caminho de aplicação esquecer parte da escrita.

O custo dessa defesa é somar o histórico da carteira no commit. É uma escolha deliberada para um desafio de correção. Uma carteira com histórico muito grande exigiria medir esse custo e substituir a soma por uma verificação incremental igualmente protegida. Não há afirmação de throughput sem medição.

## Idempotência

O header HTTP e `data.idempotencyKey` no SQS são as fontes da chave. O padrão recomendado é `providerId:externalTransactionId`. A chave não é derivada silenciosamente pelo serviço.

O hash é SHA-256 de JSON canônico: campos do contrato de negócio validados, chaves de objetos ordenadas lexicograficamente por código UTF-16, arrays preservados e campos `undefined` omitidos. Chave de idempotência, correlação e metadados de transporte ficam fora do hash de negócio. Os identificadores textuais são normalizados com trim pela validação.

O índice de identidade externa impede uma segunda execução usando uma chave nova. Reutilizar a mesma chave com outro payload ou a identidade externa com outra chave é conflito. `INSERT ... ON CONFLICT DO NOTHING` seguido de releitura resolve a disputa entre carteiras diferentes sem reutilizar uma transação SQL abortada por violação de unicidade.

O resultado é persistido junto com o efeito financeiro. Replay lê esse resultado, inclusive o saldo daquele processamento, e não o saldo atual da carteira. Uma operação em `PENDING_REFERENCE` pode posteriormente alcançar um estado terminal; a consulta e novos replays passam a refletir esse resultado final.

A inbox é única por `(consumer_name,message_id)` e guarda hash incluindo chave e negócio, além do identificador da transação. Reusar o mesmo `messageId` para outro conteúdo aborta toda a transação. Deduplicação SQS é apenas uma otimização.

## Regras e transições

`PENDING` pode virar `PENDING_REFERENCE`, `PROCESSED`, `REJECTED` ou `FAILED`. `PENDING_REFERENCE` pode permanecer pendente ou alcançar um desses estados terminais. Estado terminal não muda.

`WIN` pode ser independente ou referenciar uma BET. `LOSS` pode informar uma BET apenas como vínculo, sem alteração de saldo. `BET` não aceita referência. Referências devem ter o mesmo provedor, jogador, carteira, moeda e rodada. `gameId` não integra a regra de correspondência do enunciado.

`REFUND` reverte uma BET integralmente. `ROLLBACK` inverte BET, WIN ou REFUND. Não existe reversão parcial, rollback de rollback ou de OPENING. O limite de uma reversão é por tipo: uma BET pode ter um REFUND e um ROLLBACK, conforme a regra expressa do enunciado. Uma política que proibisse ambos precisaria ser uma decisão adicional de produto.

Se a referência estiver ausente ou ainda pendente, a operação é persistida como `PENDING_REFERENCE`. O worker relê a referência em cada tentativa, com lock na carteira e releitura da própria transação. Dois workers podem selecionar o mesmo candidato, mas apenas um processa o estado elegível.

Há 12 tentativas posteriores ao aceite, com espera inicial de 1 segundo, dobrando até 60 segundos. O prazo acumulado é de aproximadamente sete minutos, mais tempo de execução/indisponibilidade. Limite e intervalo são configuráveis. Uma referência terminal rejeitada falha imediatamente; referência ainda ausente no limite gera `REFERENCE_NOT_FOUND`.

| Código                        | Significado                                           |
| ----------------------------- | ----------------------------------------------------- |
| `INSUFFICIENT_FUNDS`          | BET sem saldo                                         |
| `REVERSAL_INSUFFICIENT_FUNDS` | ROLLBACK de crédito produziria saldo negativo         |
| `CURRENCY_MISMATCH`           | Moeda incompatível                                    |
| `REFERENCE_NOT_FOUND`         | Referência indisponível após o limite                 |
| `REFERENCE_REJECTED`          | Referência terminou sem ser processada                |
| `REFERENCE_MISMATCH`          | Identidade ou rodada incompatível                     |
| `INVALID_REFERENCE_KIND`      | Tipo de referência não permitido                      |
| `REVERSAL_AMOUNT_MISMATCH`    | Tentativa de reversão parcial ou valor diferente      |
| `ALREADY_REVERSED`            | Referência já revertida pelo mesmo tipo               |
| `WALLET_NOT_FOUND`            | Carteira da operação inexistente                      |
| `PLAYER_MISMATCH`             | Jogador não é dono da carteira                        |
| `BALANCE_LIMIT_EXCEEDED`      | Crédito ultrapassaria a capacidade do saldo           |
| `INFRASTRUCTURE_PERMANENT`    | Reservado ao estado FAILED em recuperação operacional |

`FAILED` não é usado como destino automático de falhas transitórias: sem banco disponível não seria possível gravá-lo com segurança. Mensagens que esgotam tentativas ficam auditáveis na DLQ; reenvio operacional preserva os identificadores originais.

## Eventos e recuperação

Inbox, transação, saldo, ledger e outbox fazem parte do mesmo commit. O publisher seleciona lotes de dez eventos com `FOR UPDATE SKIP LOCKED`. Mantém o lock durante o envio e marca publicação depois da resposta do SQS. Falha de envio agenda backoff de até 60 segundos, sem descartar eventos confirmados.

Esta escolha mantém uma transação aberta durante I/O externo, com timeout do SDK e lote limitado. Uma alternativa para volumes maiores seria claim com lease e recuperação de claims expirados, adicionando estados e complexidade operacional.

Se o processo cair depois do commit financeiro, outro publisher encontra a outbox pendente. Se cair depois do envio ao broker e antes do commit do publisher, o evento pode repetir, com o mesmo `eventId`. O consumidor downstream deve persistir a deduplicação desse identificador. Não se promete exactly-once no transporte.

A fila de eventos é separada da fila de comandos. O consumidor não recebe de volta seus próprios eventos. O publisher não promete ordenação global nem ordenação estrita entre publishers da mesma carteira; `walletVersion` permite ao consumidor detectar ordem e duplicação de mudanças de saldo.

No SIGTERM, o processo interrompe long polling, deixa de selecionar trabalho e aguarda operações em andamento. Mensagens recebidas e ainda não iniciadas voltam à visibilidade. Operações iniciadas renovam a visibilidade a cada dez segundos. O Compose permite 60 segundos para encerramento.

## Observabilidade e reconciliação

Logs JSON carregam contexto de processamento e códigos estáveis, sem payloads financeiros completos. As métricas usam labels de baixa cardinalidade, sem walletId ou transactionId como label. Cada instância expõe seu próprio `/metrics`.

Reconciliação lê carteira e soma do ledger em uma única instrução SQL, usando o mesmo snapshot MVCC. Diferença não é corrigida: retorna `consistent: false`, gera log e incrementa contador. Liveness não depende da infraestrutura; readiness consulta banco e filas.

## Autenticação

Não implementei IdP neste escopo, conforme a opção explícita do enunciado. `ProviderIdentityGuard` é o ponto de extensão e permite as requisições hoje. Health checks ficam fora dele.

Em uma integração real, o guard validaria issuer, audience, expiração e assinatura via JWKS de um IdP OIDC, por exemplo Keycloak. Um adapter mapearia claims para a identidade do provedor e verificaria sua correspondência com `providerId`. Não haveria tabela própria de senhas. O canal SQS continuaria interno, mantendo as validações de domínio da identidade informada.

## Estratégia dos testes

Os unitários cobrem o domínio sem dependências de infraestrutura. A integração usa PostgreSQL e LocalStack reais em containers e três processos Bun independentes para HTTP. Os publishers concorrentes também executam em processos separados. Cada execução tem banco e filas exclusivos, com verificação de nomes antes da limpeza.

Falhas de escrita são provocadas por triggers temporárias no banco de testes: uma falha ao inserir inbox ou outbox precisa desfazer também o débito e o ledger. Timeouts de lock usam uma transação concorrente segurando a carteira, exercitando retry HTTP/SQS e esgotamento de tentativas. Nos testes de queda, o processo realmente encerra depois do commit ou depois da publicação; a retomada usa os registros persistidos e o redelivery real do broker.

A soma do ledger e a correspondência entre transação e lançamento são verificadas após cada caso de integração. A suíte cobre encerramento do worker durante long polling. O teste de encerramento chama `Workers.stop()` diretamente, sem afirmar cobertura do sinal SIGTERM do sistema operacional no Windows. Não há benchmark de carga, dashboard ou tracing distribuído neste escopo.
