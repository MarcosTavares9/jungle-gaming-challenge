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

`db:down` desfaz a migration e remove os dados do desafio. Use somente em banco descartável.

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
- `503`: indisponibilidade de infraestrutura; permite reenvio com a mesma chave.

## SQS

Filas de entrada e DLQ: `wager-transactions.fifo` e `wager-transactions-dlq.fifo`. Eventos de saída: `wager-events.fifo`.

O envelope de entrada segue a seção 10 do enunciado. `data.idempotencyKey` é obrigatório; o `messageId` do envelope é a identidade persistente da inbox. Ao enviar ao SQS, use `MessageGroupId=walletId` e um `MessageDeduplicationId`. O banco mantém as garantias mesmo quando esses identificadores de transporte mudam.

O consumidor confirma mensagens somente depois do commit. Rejeições de negócio são confirmadas. Payloads inválidos e conflitos permanentes vão para a DLQ. Falhas transitórias usam backoff e limite de cinco recebimentos. A DLQ mantém o envelope original para diagnóstico e reenvio operacional.

Eventos levam `eventId`, `aggregateId`, `correlationId`, `causationId`, `occurredAt`, `eventType`, `version` e `data`. Consumidores devem deduplicar por `eventId`: uma queda entre publicação e confirmação da outbox pode repetir o evento.

## Organização

```text
src/domain/          Money, Wallet, transações, ledger, inbox/outbox e eventos
src/application/     processamento compartilhado, contratos e consultas
src/infrastructure/  MikroORM, migrations, SQS, workers e métricas
src/http/            controllers, filtro de erros e extensão de autenticação
scripts/             migrations e provisionamento das filas
tests/unit/          invariantes de domínio e contratos
tests/integration/   PostgreSQL/SQS reais, HTTP e concorrência entre processos
tests/fixtures/      processos auxiliares dos testes
```

As decisões e interpretações do domínio estão em [ARCHITECTURE.md](ARCHITECTURE.md).

## Testes

```sh
bun run test               # suíte completa
bun run test:unit          # domínio e contratos, sem Docker
bun run test:integration   # integração, concorrência e recuperação de falhas
bun run typecheck
bun run format:check
```

A integração inicia PostgreSQL e LocalStack do Compose, cria um banco `jungle_test_<uuid>` e três filas com prefixo exclusivo, aplica migrations e sobe três processos HTTP. Ao terminar, encerra os processos e remove somente o banco e as filas criados naquela execução. O banco `jungle` e as filas da aplicação são preservados. O usuário PostgreSQL local precisa de permissão para criar bancos; a configuração do Compose já atende. Endereços externos são recusados pelo runner.

Os testes cobrem valores monetários exatos, regras de transição, constraints e imutabilidade no banco, rollback de falhas tardias em inbox/outbox, idempotência, 50 repetições paralelas, disputa de saldo, independência entre carteiras, referências fora de ordem, reconciliação concorrente, retry, DLQ, publishers concorrentes e recuperação após reinício. Depois de cada teste de integração, saldo e ledger de todas as carteiras são reconciliados e a correspondência entre transação e lançamento é verificada.

Use `bun run test` para incluir a integração. `bun test` diretamente executa os unitários e marca a integração como ignorada, pois não provisiona o ambiente isolado. O teste de redelivery aguarda o visibility timeout real do SQS; a suíte de integração leva aproximadamente um minuto, além do download inicial das imagens.

`SQS_QUEUE_PREFIX` é vazio por padrão e preserva os nomes do enunciado. Os pontos de falha `TEST_CRASH_AFTER_COMMIT=true` (saída 86) e `TEST_CRASH_AFTER_PUBLISH=true` (saída 87) existem exclusivamente para os cenários locais de recuperação. Eles encerram o processo após o efeito externo e antes de sua confirmação, sem substituir PostgreSQL ou SQS por mocks.
