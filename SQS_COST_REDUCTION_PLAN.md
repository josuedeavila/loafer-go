# Plano: Redução de custos SQS em alto throughput

## 0. Contexto e diagnóstico

O SQS cobra por chamada de API, independentemente do tamanho do lote. Hoje:

| Operação | Comportamento atual | Local |
|---|---|---|
| `ReceiveMessage` | `MaxNumberOfMessages=10` (máx. SQS), `WaitTimeSeconds=10` (long polling) — já otimizado | `aws/sqs/route.go:96-105`, `aws/sqs/config.go:19-20` |
| `DeleteMessage` (commit) | 1 chamada por mensagem, sem batch | `aws/sqs/route.go:121-137` |
| `ChangeMessageVisibility` | 1 goroutine + ticker por mensagem recebida, chamadas individuais | `aws/sqs/route.go:114, 169-235` |
| Pollers | 1 goroutine por rota (não amplifica custo) | `manager.go:69-114` |
| Interface `SQSClient` | Só declara `DeleteMessage`/`ChangeMessageVisibility` singulares, sem variantes `*Batch` | `interfaces.go:24-32` |

**Maior driver de custo em alto throughput:** `DeleteMessage` e `ChangeMessageVisibility` são feitos individualmente. Em 1M mensagens/dia isso é ~1M chamadas de delete quando poderiam ser ~100k chamadas de `DeleteMessageBatch` (lotes de até 10) — redução de até 90% nessas duas operações.

> Nota de execução: este repositório é um fork, então as mudanças abaixo são aplicadas diretamente aos arquivos existentes (`interfaces.go`, `aws/sqs/route.go`, `aws/sqs/config.go`, `manager.go`, `fake/`) na branch `feature/v3-sqs-batching`, sem criar um módulo `v3` separado. A comparação de performance será feita depois contra o upstream `github.com/justcodes/loafer-go/v2` publicado.

## 1. Mudanças de interface (breaking change)

`interfaces.go:24-32` — adicionar ao `SQSClient`:

```go
DeleteMessageBatch(ctx context.Context, params *sqs.DeleteMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error)
ChangeMessageVisibilityBatch(ctx context.Context, params *sqs.ChangeMessageVisibilityBatchInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityBatchOutput, error)
```

Isso amplia a interface pública — qualquer implementação custom de `SQSClient` (fora do SDK da AWS) precisa ganhar esses dois métodos. Como o módulo já é `v2` (`go.mod:1`), decidir se justifica `v3` ou se o breaking change é tolerado dentro do `v2` (registrar no CHANGELOG/README).

Mocks: `fake/SQSClient.go` é gerado via `mockery` (`.mockery.yml`) — regenerar, não editar à mão.

Se optarmos por flush explícito no shutdown (item 5), o `Router` (`interfaces.go:12-21`) também ganha um método novo, ex. `Close(ctx context.Context) error` — mesma ressalva de breaking change para implementações custom de `Router`.

## 2. Novo componente: batcher genérico

Novo arquivo `aws/sqs/batcher.go`, reutilizável para as duas operações (delete e visibility têm a mesma forma: N entradas com `Id` + `ReceiptHandle` [+ `VisibilityTimeout` no caso de visibility], resposta com `Successful`/`Failed`).

```go
type batchEntry struct {
    id            string
    receiptHandle string
    timeout       *int32 // nil para delete, valor para visibility
    result        chan error
}

type batcher struct {
    mu        sync.Mutex
    pending   []batchEntry
    batchSize int
    interval  time.Duration
    timer     *time.Timer
    flushFn   func(ctx context.Context, entries []batchEntry) // chama DeleteMessageBatch ou ChangeMessageVisibilityBatch
}
```

Comportamento:

- `enqueue(ctx, entry) error`: adiciona à fila; se atingir `batchSize` (10, hard cap da SQS) dispara flush imediato; senão arma/reseta um timer de `interval` (linger time) para flush por tempo. **Bloqueia** até receber o resultado no `entry.result` — preserva o contrato atual de `Commit` retornar erro real, ao custo de até `interval` de latência extra no ack quando o lote não enche por tamanho.
- `flush`: separa em lotes de até 10, chama a API, distribui `nil`/erro para cada `result` channel conforme `Successful`/`Failed` (`BatchResultErrorEntry.Id` mapeia de volta pelo `Id` gerado no enqueue — usar contador atômico por rota).
- Concorrência: múltiplos workers (`workerPoolSize`, default 5) chamam `Commit` em paralelo — o batcher deve ser uma instância **por rota**, não por worker, para agregar entre workers.

## 3. Mudanças em `aws/sqs/route.go`

### Delete (`Commit`, linhas 121-137)

```go
func (r *route) Commit(ctx context.Context, m loafergo.Message) error {
    if m.BackedOff() { return nil }
    defer m.Dispatch()
    return r.deleteBatcher.enqueue(ctx, m.Identifier(), nil)
}
```

`r.deleteBatcher` inicializado em `Configure` (linhas 79-92), pois só ali temos `r.queueURL`.

### Visibility (linhas 169-235)

Ponto mais delicado: hoje cada mensagem tem seu próprio ticker independente (`sleepTime := visibilityTimeout - 10`), então os vencimentos de extensão de mensagens diferentes não coincidem naturalmente no tempo. Duas opções:

- **Opção simples (baixo esforço, ganho parcial):** manter o ticker por mensagem, trocando a chamada individual `r.sqs.ChangeMessageVisibility` (linha 215) por `r.visibilityBatcher.enqueue(...)`. Só agrupa chamadas que coincidirem na mesma janela `interval` — ganho depende de quantas mensagens vencem ao mesmo tempo.
- **Opção completa (maior esforço, maior ganho em cargas com processamento longo):** substituir os goroutines+ticker por mensagem por um scheduler único por rota, com uma min-heap ordenada por "próximo vencimento". A cada tick (ex. 1s), agrupa mensagens vencidas em lotes de até 10 e chama `ChangeMessageVisibilityBatch`. Mais complexo: precisa lidar com `Backoff()` (que hoje interrompe o ticker via `m.backoffChannel`, linhas 185-187) e `extensionLimit` por mensagem dentro da estrutura compartilhada.

Recomendação: tratar como **Fase 2 separada** — o ganho de custo em alto throughput vem majoritariamente do delete batching, já que toda mensagem passa por `Commit`, mas só uma fração passa por extensão de visibilidade.

## 4. Novas opções em `aws/sqs/config.go`

```go
defaultDeleteBatchSize         = int32(10)  // hard cap SQS
defaultDeleteBatchInterval     = 200 * time.Millisecond
defaultVisibilityBatchSize     = int32(10)
defaultVisibilityBatchInterval = 500 * time.Millisecond
```

Mais `RouteWithDeleteBatchSize`, `RouteWithDeleteBatchInterval` (e equivalentes para visibility), seguindo o padrão dos `RouteWithX` existentes (`config.go:98-123`). Validar `batchSize <= 10` (SQS rejeita lotes maiores).

## 5. `manager.go` — flush no shutdown

Em `runRoute` (linhas 69-114), no `case <-ctx.Done()` (linha 88), chamar `r.Close(ctx)` antes de retornar, para dar flush no que estiver pendente no batcher. Não é estritamente necessário para correção — mensagens não deletadas voltam a ficar visíveis após o `visibilityTimeout` expirar (semântica at-least-once preservada) — mas evita segurar acks desnecessariamente num shutdown gracioso.

## 6. Testes

- `aws/sqs/batcher_test.go` (`package sqs`, testando internals): flush por tamanho, flush por tempo, falha parcial (`Failed` mapeado pro `Id` certo), concorrência (múltiplas goroutines chamando `enqueue`, rodar com `-race`), drain no shutdown.
- Atualizar `aws/sqs/route_test.go`: `Commit` deve continuar retornando erro real quando uma entrada falha no `DeleteMessageBatch`.
- Atualizar mock `fake/SQSClient.go` (regenerado via `mockery`) para os dois métodos novos.
- Bench opcional em `manager_benchmark_test.go` comparando nº de chamadas simuladas antes/depois.

## 7. Ordem de implementação

1. **Fase 1 — Delete batching** (maior ROI, menor risco, isolado no `Commit`).
2. **Fase 2 — Visibility batching**, com o scheduler completo (min-heap por rota), não só a opção simples do ticker por mensagem.
3. **Fase 3 — Shutdown flush** (`Router.Close`).
4. **Fase 4 — Docs**: atualizar `README.md`, comentário de exemplo em `NewRoute` (`route.go:44-57`), e CHANGELOG (se existir), avisando sobre a mudança de interface.

Fases 1 e 2 serão implementadas juntas neste ciclo de trabalho.

## 8. Decisões tomadas

- **Breaking change na interface aceito dentro do `v2`** — `SQSClient` e `Router` podem ganhar métodos novos sem exigir bump para `v3`. Registrar no CHANGELOG/README mesmo assim, pois implementações custom dessas interfaces precisam ser atualizadas.
- **Delete batching é opt-in**, via config/flag explícita (ex. `RouteWithDeleteBatching(true)` ou similar) — comportamento atual (delete síncrono por mensagem) continua sendo o padrão quando a opção não é habilitada.
- **Visibility batching (Fase 2) entra neste ciclo junto com a Fase 1**, com o scheduler completo (min-heap por rota agrupando vencimentos de várias mensagens em lotes de até 10), não apenas a opção simples de reaproveitar o ticker por mensagem.
