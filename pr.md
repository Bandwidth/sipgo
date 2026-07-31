# Non-blocking server transaction cleanup

`Server.handleRequest` calls `tx.TerminateGracefully()` once the user handler returns. On UDP, if a
final response was sent, that parks the calling goroutine on `<-tx.Done()` until the transaction
terminates: 64×T1, so 32s at default timers.

That goroutine is the one sipgo spawned to handle the inbound message in the first place — one per
datagram, at `transaction_layer.go:140`. The handler has already returned by the time it reaches the
park, so it spends those 32s doing nothing.

This PR adds `ServerTx.Cleanup()`: same leak-safety checks, no wait. `server.go`'s single call site
switches to it. `TerminateGracefully` stays as a deprecated wrapper (`Cleanup()` then `<-tx.Done()`)
so no external caller breaks.

The transaction is not touched. It stays in `txl.serverTransactions` with every timer armed and
terminates at exactly the same moment it did before. Only the goroutine goes away.

## Why the wait isn't needed

Retransmissions never ran on the parked goroutine anyway. Each inbound datagram gets its own
goroutine (`transaction_layer.go:140`), which looks the transaction up in `serverTransactions` and
calls `tx.Receive`. Since `Cleanup()` doesn't remove the transaction from that map, retransmission
handling is byte-for-byte what it was before.

`<-tx.Done()` is the last statement in the function. Nothing reads its value, and the only caller
returns immediately after. So no behavior can depend on it — the reception just decides when the
goroutine dies.

Retransmission and termination are the FSM's job, and every state you can be in after a final
response already has an `AfterFunc` armed for it:

| State after final response               | RFC          | Timer, armed in                                                  | What drives it                                            |
|------------------------------------------|--------------|------------------------------------------------------------------|-----------------------------------------------------------|
| `inviteStateCompleted` (3xx-6xx, no ACK) | 3261 §17.2.1 | `timer_g` fsm.go:188, `timer_h` fsm.go:204, `actRespondComplete` | retransmits the final, then terminates                    |
| `inviteStateConfirmed` (ACK in)          | 3261 §17.2.1 | `timer_i` fsm.go:286, `actConfirm`                               | absorbs duplicate ACKs                                    |
| `inviteStateAccepted` (2xx)              | 6026 §7.1    | `timer_l` fsm.go:219, `actRespondAccept`                         | absorbs retransmitted INVITEs; TU owns 2xx retransmission |
| `stateCompleted` (non-INVITE)            | 3261 §17.2.2 | `timer_j` fsm.go:243, `actFinal`                                 | replays the final on each retransmitted request           |

Those all run on their own goroutines, never the parked one. And since `Cleanup()` doesn't call
`delete()` on this path, the transaction stays in the map, so retransmissions still match it and
still reach the FSM.

Re #277: agreed that the transaction has to be held open past its final response. It still is. The
park was holding a goroutine open, not the transaction.

`OnTerminate`'s own docstring already says it's the "alternative to tx.Done where you avoid creating
more goroutines". This just applies that to the server's own call site.

## What Cleanup Terminates

Both early exits from the original function, unchanged:

- **Reliable transport.** No retransmission to protect.
- **No final response sent.** Nothing buffered to replay, so leaving it alive would leak the
  transaction for 64×T1. This is why `Cleanup()` has to be called and not just dropped.

## Tests

`TerminateGracefully` had no test coverage. Added 7 in `sip/transaction_server_tx_cleanup_test.go`,
one per row above, each checking that `Cleanup()` returns immediately *and* that the RFC behavior
still happens after it does:

| Test                                   | Checks                                                                                         |
|----------------------------------------|------------------------------------------------------------------------------------------------|
| `...NonInviteReplaysFinal`             | retransmitted OPTIONS still gets the buffered 200 (2 on the wire), then Timer J terminates     |
| `...InviteCompletedRetransmitsFinal`   | Timer G still retransmits the 486 (≥3 seen); a later ACK still confirms and Timer I terminates |
| `...InviteCompletedTerminatesOnTimerH` | no ACK ever arrives, Timer H still reaps it                                                    |
| `...InviteAcceptedSurvivesUntilTimerL` | survives to Timer L; retransmitted INVITE absorbed, not answered (still 1 200 on the wire)     |
| `...TerminatesWithoutFinalResponse`    | provisional-only handler terminates immediately                                                |
| `...TerminatesOnReliableTransport`     | TCP terminates immediately                                                                     |
| `...TerminateGracefullyStillBlocks`    | the deprecated wrapper still blocks                                                            |

Timers are shortened via `SetTimers` and restored after, so the 64×T1 windows run in milliseconds.
Timer G writes come from a runtime timer goroutine, so the test writer is mutex-guarded.

## Numbers

Setup: a stateless UDP proxy on 4 vCPU. Per call INVITE → 183 → 200 → BYE, so ~3 server transactions,
SDP rewritten on offer and answer through an external media relay, dialog state in Redis. Both runs
are the same commit on the same host, differing only in the sipgo version.

|                                             | before       | after |
|---------------------------------------------|--------------|-------|
| live goroutines at 200 CPS                  | 14,164       | 1,407 |
| ...of those parked in `TerminateGracefully` | 13,326 (94%) | 0     |

That's ~14,000 stacks at 2-8 KiB each, every one pinning its request, response and handler locals for
32s, and all of them walked by the GC on every cycle.

Latency was unchanged in our runs.

## Not fixed here

`ackSendAsync` (`transaction_server_tx.go:147`) spawns `go tx.ackSend(r)` when nothing is reading
`tx.Acks()`, and that goroutine parks on `tx.done`. Anything handling ACK through `OnRequest(ACK)`
instead of `tx.Acks()`, which is normal for a proxy, leaks one per non-2xx ACK. In our test we measured 823 before
this change and 774 after, so `Cleanup()` doesn't help: the park is on `tx.done`, which still closes
at Timer I/L.

Options look like dropping the ACK when nobody's listening, buffering the channel by one, or only
spawning when a reader is registered. Happy to do it as a separate PR if you have a preference.

## Pre-existing test failures

Both are on unmodified `main`, not from this PR:

- the `sipgo` package won't build under `go test` — `server_integration_test.go:36` embeds
  `testdata/certs/client.crt`, which isn't in the repo
- `TestServerTransactionRespondRejectsCRLF` panics on a nil deref

The `sip` package goes 117 → 124 passing with those two unchanged. Can file issues separately.
