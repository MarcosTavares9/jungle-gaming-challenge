# Jungle Gaming: processador de apostas

Serviço NestJS que recebe operações por HTTP e SQS, mantém saldo e ledger no PostgreSQL e publica eventos por transactional outbox. O enunciado original está em [CHALLENGE.md](CHALLENGE.md).

## Executar localmente

Requisitos: Bun 1.4.4 e Docker Compose com containers Linux.

```sh
bun install --frozen-lockfile
docker compose up -d --wait postgres localstack
bun run db:up
bun run queues:setup
bun run start
```

Os valores padrão atendem ao ambiente local; `.env.example` lista as configurações. Para alterá-las, copie o arquivo para `.env`. A API escuta na porta 3000, PostgreSQL em `127.0.0.1:15432` e LocalStack em `127.0.0.1:4566`.

Para executar a aplicação em containers, incluindo três instâncias:

```sh
docker compose up --build -d --scale app=3
```

O serviço `setup` aplica migrations e cria as filas antes de liberar a aplicação. As instâncias recebem as portas 3000, 3001 e 3002. Todas executam o consumidor, o publisher e o worker de referências. `WORKERS_ENABLED=false` permite executar apenas HTTP.

```sh
bun run typecheck
bun run db:down
```

`db:down` desfaz a última migration. Reverter a migration de arquivo da DLQ remove seu histórico, e reverter a migration inicial remove os dados financeiros; exporte os registros antes de um rollback com dados. A migration de roteamento da outbox recusa rollback enquanto houver intenções DLQ não publicadas.

## Exemplo HTTP

```sh
curl -X POST http://localhost:3000/wallets \
  -H 'Content-Type: application/json' \
  -d '{"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","initialBalance":{"amount":"100.00","currency":"BRL"}}'
```

Use o `id` retornado como `walletId`:

```sh
curl -X POST http://localhost:3000/wagering/transactions \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: provider-a:bet-1' \
  -H 'X-Correlation-Id: example-1' \
  -d '{"providerId":"provider-a","externalTransactionId":"bet-1","playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","walletId":"SUBSTITUIR","roundId":"round-1","gameId":"game-1","kind":"BET","money":{"amount":"25.00","currency":"BRL"}}'
```

Reenviar a mesma requisição retorna o mesmo resultado financeiro com `idempotentReplay: true`. Alterar o payload mantendo a chave retorna `409`. Para `REFUND` e `ROLLBACK`, informe `referenceExternalTransactionId`.

| Método | Rota                                                                  | Função                                                            |
| ------ | --------------------------------------------------------------------- | ----------------------------------------------------------------- |
| POST   | `/wallets`                                                            | Abre carteira, com lançamento de abertura se o saldo for positivo |
| GET    | `/wallets/:walletId`                                                  | Saldo e versão atuais                                             |
| GET    | `/wallets/:walletId/ledger?limit=50&cursor=...`                       | Histórico em ordem de versão, cursor opaco                        |
| POST   | `/wagering/transactions`                                              | Processa uma operação                                             |
| GET    | `/wagering/transactions/:transactionId`                               | Estado atual e resultado persistido                               |
| GET    | `/providers/:providerId/wagering/transactions/:externalTransactionId` | Consulta pelo identificador do provedor                           |
| POST   | `/wallets/:walletId/reconciliation`                                   | Compara saldo materializado e soma do ledger                      |
| GET    | `/health/live`                                                        | Processo vivo                                                     |
| GET    | `/health/ready`                                                       | Banco e três filas alcançáveis                                    |
| GET    | `/metrics`                                                            | Métricas Prometheus desta instância                               |

### Respostas

- `201`: carteira criada (e resposta da reconciliação, conforme padrão de POST do NestJS).
- `200`: operação processada ou replay de operação processada.
- `202`: referência ainda indisponível; acompanhe pela consulta de transação.
- `400`: contrato inválido, incluindo header de idempotência ausente.
- `404`: recurso não encontrado em consulta.
- `409`: chave/payload conflitantes, identificador externo reutilizado ou carteira duplicada.
- `422`: rejeição de negócio, persistida com `failureCode`.
- `500`: resultado terminal `FAILED`, com `failureCode` persistido e replay estável.
- `503`: indisponibilidade de infraestrutura; permite reenvio com a mesma chave.

## SQS

Filas de entrada e DLQ: `wager-transactions.fifo` e `wager-transactions-dlq.fifo`. Eventos de saída: `wager-events.fifo`.

O envelope de entrada segue a seção 10 do enunciado. `data.idempotencyKey` é obrigatório; o `messageId` do envelope é a identidade persistente da inbox. Ao enviar ao SQS, use `MessageGroupId=walletId` e um `MessageDeduplicationId`. O banco mantém as garantias mesmo quando esses identificadores de transporte mudam.

O consumidor confirma mensagens somente depois do commit. Rejeições de negócio são confirmadas. Payloads inválidos e conflitos permanentes vão para a DLQ. Falhas transitórias usam backoff e limite de cinco recebimentos. Falhas permanentes de infraestrutura e esgotamento das tentativas geram `FAILED`, inbox, evento e intenção DLQ na mesma transação, sem alterar saldo. Um publisher entrega o corpo original à DLQ, com retry persistente se o SQS estiver indisponível.

Se o PostgreSQL estiver indisponível até o redrive nativo do SQS, a mensagem fica na DLQ. Após recuperar o banco e aplicar as migrations, `bun run dlq:audit` processa até dez mensagens: persiste `FAILED` para operações válidas sem resultado anterior, preserva resultados já concluídos e grava o corpo original em `dlq_archive` antes de confirmar a remoção no SQS. Repita o comando para avançar sobre os próximos lotes, inclusive dentro do mesmo grupo FIFO.

O arquivo é append-only e identifica cada entrega por fila e `message_id`, com SHA-256 do corpo original. Payloads inválidos e conflitos também são arquivados, com resultado `INVALID_PAYLOAD` ou `CONFLICT`, sem substituir transações financeiras. O histórico continua disponível no PostgreSQL para análise manual. Falha técnica no banco, arquivo ou SQS retorna código de saída 1 e mantém as mensagens não confirmadas disponíveis para nova tentativa; dados arquivados e transações já confirmadas são reaproveitados de forma idempotente.

Eventos levam `eventId`, `aggregateId`, `correlationId`, `causationId`, `occurredAt`, `eventType`, `version` e `data`. Consumidores devem deduplicar por `eventId`: uma queda entre publicação e confirmação da outbox pode repetir o evento.

## Organização

```text
src/domain/          Money, Wallet, transações, ledger, inbox/outbox e eventos
src/application/     processamento compartilhado, contratos e consultas
src/infrastructure/  MikroORM, migrations, SQS, workers e métricas
src/http/            controllers, filtro de erros e extensão de autenticação
scripts/             migrations e provisionamento das filas
test-console/        painel local para demonstração das validações
tests/unit/          invariantes de domínio e contratos
tests/integration/   PostgreSQL/SQS reais, HTTP e concorrência entre processos
tests/fixtures/      processos auxiliares dos testes
tests/resilience/    três containers, indisponibilidade real e SIGTERM em Linux
```

As decisões e interpretações do domínio estão em [ARCHITECTURE.md](ARCHITECTURE.md).

## Testes

```sh
bun run test               # suíte completa
bun run test:unit          # domínio e contratos, sem Docker
bun run test:integration   # integração, concorrência e recuperação de falhas
bun run test:concurrency   # recorte dos cenários obrigatórios de paralelismo
bun run test:resilience    # Compose isolado com três aplicações e falhas reais
bun run validate           # formatação, tipos e todas as suítes
bun run typecheck
bun run format:check
```

### Test Console

Para apresentar e executar as validações por uma interface local:

```sh
bun run test:console
```

Abra `http://127.0.0.1:4173`. O painel oferece comandos predefinidos para qualidade estática, unidade, integração, concorrência, resiliência e validação completa, com logs e duração. Ele escuta somente no loopback, mantém o histórico apenas em memória e bloqueia execuções simultâneas para que as suítes não disputem os mesmos recursos.

O objetivo do painel é tornar as garantias do projeto demonstráveis sem esconder a execução real. Cada botão inicia o mesmo comando disponível no terminal e mantém a saída completa visível. Acima dos logs, uma topologia resume o cenário ativo e destaca aplicação, SQS, PostgreSQL, outbox, wallet ou asserções conforme eventos reconhecidos aparecem na saída. Logs estruturados do worker, como processamento iniciado, resultado financeiro, retry e DLQ, alimentam essa visualização diretamente. Linhas sem mapeamento permanecem no console e não geram estado visual inferido.

Na suíte de concorrência, o painel representa os 50 envios atravessando três processos e mostra o resultado esperado de um débito com 49 replays idempotentes. O resultado visual não substitui a evidência: aprovação, falha, duração e números vêm do processo executado. As animações pausam quando a aba fica oculta e respeitam a preferência de redução de movimento do sistema.

O runner não aceita comandos enviados pelo navegador. A API local recebe somente identificadores de suítes presentes em uma lista fechada, limita uma execução por vez, restringe o acesso a `127.0.0.1` e mantém a saída em memória com limite de tamanho. Assim, a interface serve como apoio de demonstração e diagnóstico sem expor um terminal genérico.

A integração inicia PostgreSQL e LocalStack do Compose, cria um banco `jungle_test_<uuid>` e três filas com prefixo exclusivo, aplica migrations e sobe três processos HTTP. Ao terminar, encerra os processos e remove somente o banco e as filas criados naquela execução. O banco `jungle` e as filas da aplicação são preservados. O usuário PostgreSQL local precisa de permissão para criar bancos; a configuração do Compose já atende. Endereços externos são recusados pelo runner.

A resiliência usa o mesmo `compose.yaml` com `compose.resilience.yaml`, projeto de nome exclusivo, portas dinâmicas e volume PostgreSQL próprio. Sobe três containers da aplicação, para e reinicia o PostgreSQL, pausa e retoma o LocalStack e envia SIGTERM a um consumidor com mensagem em andamento. Containers, rede e volume desse projeto são removidos ao terminar. A infraestrutura de desenvolvimento não é interrompida. É necessário Docker Compose 2.24.4 ou superior para o override das portas.

Os testes cobrem valores monetários exatos, regras de transição, constraints e imutabilidade no banco, rollback de falhas tardias em inbox/outbox, idempotência, 50 repetições paralelas, disputa de saldo, independência entre carteiras, referências fora de ordem, reconciliação concorrente, retry, DLQ, publishers concorrentes e recuperação após reinício. Depois de cada teste de integração, saldo e ledger de todas as carteiras são reconciliados e a correspondência entre transação e lançamento é verificada.

Use `bun run test` para executar as três etapas em sequência. `bun test` diretamente executa os unitários e marca integração e resiliência como ignoradas, pois não provisiona os ambientes isolados. O teste de redelivery aguarda o visibility timeout real de 30 segundos do SQS. A duração total varia com a máquina e inclui build, startup, indisponibilidade e recuperação dos serviços.

`SQS_QUEUE_PREFIX` é vazio por padrão e preserva os nomes do enunciado. Os pontos de falha `TEST_CRASH_AFTER_COMMIT=true` (saída 86) e `TEST_CRASH_AFTER_PUBLISH=true` (saída 87) existem exclusivamente para os cenários locais de recuperação. Eles encerram o processo após o efeito externo e antes de sua confirmação, sem substituir PostgreSQL ou SQS por mocks.
