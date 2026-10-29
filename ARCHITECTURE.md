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

A unidade de serialização financeira é uma linha de `wallets`. Primeiro a operação consulta a idempotência persistida e, quando é nova, reserva a identidade pelos índices únicos da transação. Somente a execução que criou essa reserva adquire o lock da carteira para aplicar a movimentação. Replays esperam o commit da identidade concorrente ou leem seu resultado já confirmado, sem disputar o lock da carteira entre si. Instâncias diferentes continuam serializando movimentações distintas da mesma carteira no PostgreSQL; carteiras diferentes usam linhas distintas. Não há lock global nem dependência de estado em memória.

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

| Código                        | Significado                                      |
| ----------------------------- | ------------------------------------------------ |
| `INSUFFICIENT_FUNDS`          | BET sem saldo                                    |
| `REVERSAL_INSUFFICIENT_FUNDS` | ROLLBACK de crédito produziria saldo negativo    |
| `CURRENCY_MISMATCH`           | Moeda incompatível                               |
| `REFERENCE_NOT_FOUND`         | Referência indisponível após o limite            |
| `REFERENCE_REJECTED`          | Referência terminou sem ser processada           |
| `REFERENCE_MISMATCH`          | Identidade ou rodada incompatível                |
| `INVALID_REFERENCE_KIND`      | Tipo de referência não permitido                 |
| `REVERSAL_AMOUNT_MISMATCH`    | Tentativa de reversão parcial ou valor diferente |
| `ALREADY_REVERSED`            | Referência já revertida pelo mesmo tipo          |
| `WALLET_NOT_FOUND`            | Carteira da operação inexistente                 |
| `PLAYER_MISMATCH`             | Jogador não é dono da carteira                   |
| `BALANCE_LIMIT_EXCEEDED`      | Crédito ultrapassaria a capacidade do saldo      |
| `INFRASTRUCTURE_PERMANENT`    | Erro técnico não recuperável sem intervenção     |
| `RETRY_EXHAUSTED`             | Limite de tentativas técnicas atingido           |

Falhas de conexão, timeout de lock, deadlock e serialização continuam transitórias. Erros SQL de sintaxe, permissão, integridade ou recurso não suportado são permanentes. Uma falha técnica permanente, ou a quinta tentativa sem sucesso, chama `recordPermanentFailure`: persiste `FAILED`, inbox, evento `WagerTransactionFailed` e intenção de entrega à DLQ atomicamente. Não há lançamento nem mudança de saldo. O índice de identidade externa decide disputas entre esse caminho e o processamento financeiro; um resultado já confirmado nunca é sobrescrito por uma falha posterior de transporte.

A outbox tem destinos `events` e `dlq`. Intenções DLQ guardam o corpo original e a identidade da entrega; um índice parcial impede intenções duplicadas para a mesma entrega. O ack do comando depende do commit dessa intenção, sem depender de uma chamada imediata ao SQS. O publisher publica depois e mantém retry persistente. Reverter essa migration exige publicar primeiro intenções DLQ pendentes.

Se o banco permanecer indisponível até o redrive nativo, não é possível gravar `FAILED` naquele momento. O envelope continua auditável na DLQ. Após o retorno do banco, o comando `dlq:audit` persiste a falha para mensagens válidas ainda sem resultado, sem substituir operações processadas. Payloads inválidos e conflitos não recebem transações financeiras artificiais.

A auditoria arquiva o corpo original em `dlq_archive` antes de remover a entrega do SQS. Esse ack permite avançar na cabeça do grupo FIFO em execuções posteriores; devolver sempre o mesmo lote à visibilidade impediria alcançar a 11ª mensagem. O arquivo tem identidade única por fila/MessageId, hash SHA-256 do corpo exato e proteção contra UPDATE, DELETE e TRUNCATE. Resultados `AUDITED`, `INVALID_PAYLOAD` e `CONFLICT` preservam os envelopes e a classificação para análise operacional.

O registro financeiro e o arquivo usam commits separados, mas o ack depende da conclusão de ambos. Uma queda entre os commits mantém a mensagem na DLQ; a retomada reutiliza a transação existente e completa o arquivo. Uma queda depois do arquivo e antes do ack reutiliza o registro de arquivo, sem duplicar histórico nem efeito financeiro. Erros técnicos interrompem o lote com exit code 1 e devolvem à visibilidade as mensagens ainda não confirmadas. Somente erros esperados de payload/conflito são classificados e arquivados como tal. Reverter a migration de arquivo remove esse histórico e exige exportação prévia quando houver dados.

## Eventos e recuperação

Inbox, transação, saldo, ledger e outbox fazem parte do mesmo commit. O publisher seleciona lotes de dez eventos com `FOR UPDATE SKIP LOCKED`. Mantém o lock durante o envio e marca publicação depois da resposta do SQS. Falha de envio agenda backoff de até 60 segundos, sem descartar eventos confirmados.

Esta escolha mantém uma transação aberta durante I/O externo, com timeout do SDK e lote limitado. Uma alternativa para volumes maiores seria claim com lease e recuperação de claims expirados, adicionando estados e complexidade operacional.

Se o processo cair depois do commit financeiro, outro publisher encontra a outbox pendente. Se cair depois do envio ao broker e antes do commit do publisher, o evento pode repetir, com o mesmo `eventId`. O consumidor downstream deve persistir a deduplicação desse identificador. Não se promete exactly-once no transporte.

A fila de eventos é separada da fila de comandos. O consumidor não recebe de volta seus próprios eventos. O publisher não promete ordenação global nem ordenação estrita entre publishers da mesma carteira; `walletVersion` permite ao consumidor detectar ordem e duplicação de mudanças de saldo.

No SIGTERM, o processo interrompe long polling, deixa de selecionar trabalho e aguarda operações em andamento. Mensagens recebidas e ainda não iniciadas voltam à visibilidade. Operações iniciadas renovam a visibilidade a cada dez segundos. O Compose permite 60 segundos para encerramento.

## Observabilidade e reconciliação

Logs JSON carregam contexto de processamento e códigos estáveis, sem payloads financeiros completos. Correlação HTTP é atribuída uma vez e devolvida no header. Transações persistem correlação e causa para referências retomadas depois. Retry, DLQ, publicação e shutdown carregam identificadores disponíveis; campos desconhecidos ficam explicitamente nulos. Logs de publicação recuperam o provedor pela transação, e logs do driver são normalizados sem expor SQL ou parâmetros. As métricas usam labels de baixa cardinalidade, sem walletId ou transactionId como label. Cada instância expõe seu próprio `/metrics`.

Reconciliação lê carteira e soma do ledger em uma única instrução SQL, usando o mesmo snapshot MVCC. Diferença não é corrigida: retorna `consistent: false`, gera log e incrementa contador. Liveness não depende da infraestrutura; readiness consulta banco e filas.

## Autenticação

Não implementei IdP neste escopo, conforme a opção explícita do enunciado. `ProviderIdentityGuard` é o ponto de extensão e permite as requisições hoje. Health checks ficam fora dele.

Em uma integração real, o guard validaria issuer, audience, expiração e assinatura via JWKS de um IdP OIDC, por exemplo Keycloak. Um adapter mapearia claims para a identidade do provedor e verificaria sua correspondência com `providerId`. Não haveria tabela própria de senhas. O canal SQS continuaria interno, mantendo as validações de domínio da identidade informada.

## Estratégia dos testes

Os unitários cobrem o domínio sem dependências de infraestrutura. A integração usa PostgreSQL e LocalStack reais em containers e três processos Bun independentes para HTTP. Os publishers concorrentes também executam em processos separados. Cada execução tem banco e filas exclusivos, com verificação de nomes antes da limpeza.

Falhas de escrita são provocadas por triggers temporárias no banco de testes: uma falha ao inserir inbox ou outbox precisa desfazer também o débito e o ledger. Timeouts de lock usam uma transação concorrente segurando a carteira, exercitando retry HTTP/SQS e esgotamento de tentativas. Nos testes de queda, o processo realmente encerra depois do commit ou depois da publicação; a retomada usa os registros persistidos e o redelivery real do broker.

A soma do ledger e a correspondência entre transação e lançamento são verificadas após cada caso de integração e resiliência. Além do encerramento durante long polling, uma stack Compose separada executa três containers Linux e recebe SIGTERM com mensagem em andamento: o processamento iniciado termina, mensagens restantes voltam à visibilidade e outra instância assume após a retomada. O teste também para e reinicia PostgreSQL preservando seu volume, e pausa SQS para comprovar persistência, backoff e publicação da outbox após recuperação. Portas publicadas são redescobertas após reinícios. Não há benchmark de carga, dashboard ou tracing distribuído neste escopo.
