package sip

import (
	"bytes"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/emiago/sipgo/fakes"
	"github.com/stretchr/testify/require"
)

// These tests cover ServerTx.Cleanup, and with it the claim that the SIP timers alone
// own retransmission and termination of a server transaction after the user handler has
// returned. Each unreliable-transport case asserts both halves: that Cleanup returns
// without waiting for the transaction, and that the behaviour RFC 3261 requires during
// the remaining timer window still happens.

const cleanupTestFromAddr = "127.0.0.2:5060"

// syncBuffer is an io.Writer that is safe for concurrent use. Timer G retransmissions
// are written from a runtime timer goroutine while the test reads the buffer.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuffer) count(substr string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return strings.Count(s.buf.String(), substr)
}

// setFastTimers shrinks the SIP timers so that 64*T1 windows are testable, and restores
// the package defaults when the test finishes.
func setFastTimers(t *testing.T) {
	t.Helper()
	timer1xx := Timer_1xx
	// Long enough that the automatic 100 Trying never interferes with a test that
	// responds immediately.
	Timer_1xx = time.Minute
	SetTimers(20*time.Millisecond, 40*time.Millisecond, 40*time.Millisecond)
	t.Cleanup(func() {
		Timer_1xx = timer1xx
		SetTimers(500*time.Millisecond, 4*time.Second, 5*time.Second)
	})
}

func newCleanupTestTx(t *testing.T, req *Request) (*ServerTx, *syncBuffer) {
	t.Helper()
	outgoing := &syncBuffer{}
	conn := &UDPConnection{
		PacketConn: &fakes.UDPConn{
			Reader:  bytes.NewBuffer([]byte{}),
			Writers: map[string]io.Writer{cleanupTestFromAddr: outgoing},
		},
	}
	tx := NewServerTx("cleanup-test", req, conn, slog.Default())
	require.NoError(t, tx.Init())
	t.Cleanup(tx.Terminate)
	return tx, outgoing
}

// requireReturnsImmediately fails if f takes anywhere near a 64*T1 timer window.
func requireReturnsImmediately(t *testing.T, f func()) {
	t.Helper()
	start := time.Now()
	f()
	require.Less(t, time.Since(start), Timer_J/4, "Cleanup must not wait for the transaction to terminate")
}

func requireNotTerminated(t *testing.T, tx *ServerTx) {
	t.Helper()
	select {
	case <-tx.Done():
		t.Fatal("transaction terminated early; its timer window must stay open")
	default:
	}
}

// --- non-INVITE: Timer J replays the final response (RFC 3261 §17.2.2) ---

func TestServerTxCleanupNonInviteReplaysFinal(t *testing.T) {
	setFastTimers(t)

	req := testCreateRequest(t, "OPTIONS", "sip:example.com", "UDP", cleanupTestFromAddr)
	tx, outgoing := newCleanupTestTx(t, req)

	require.NoError(t, tx.Receive(req))
	require.NoError(t, tx.Respond(NewResponseFromRequest(req, StatusOK, "OK", nil)))
	require.NoError(t, compareFunctions(tx.currentFsmState(), tx.stateCompleted))

	requireReturnsImmediately(t, tx.Cleanup)
	requireNotTerminated(t, tx)
	require.NoError(t, compareFunctions(tx.currentFsmState(), tx.stateCompleted))

	// A retransmitted OPTIONS inside the Timer J window still gets the buffered final.
	require.NoError(t, tx.Receive(req))
	require.Equal(t, 2, outgoing.count("SIP/2.0 200 OK"))

	require.Eventually(t, func() bool {
		return compareFunctions(tx.currentFsmState(), tx.stateTerminated) == nil
	}, 4*Timer_J, Timer_J/10, "Timer J must terminate the transaction on its own")
}

// --- INVITE 3xx-6xx: Timer G retransmits, Timer H terminates (RFC 3261 §17.2.1) ---

func TestServerTxCleanupInviteCompletedRetransmitsFinal(t *testing.T) {
	setFastTimers(t)

	req, _, _ := testCreateInvite(t, "sip:127.0.0.99:5060", "UDP", cleanupTestFromAddr)
	tx, outgoing := newCleanupTestTx(t, req)

	require.NoError(t, tx.Respond(NewResponseFromRequest(req, StatusBusyHere, "Busy Here", nil)))
	require.NoError(t, compareFunctions(tx.currentFsmState(), tx.inviteStateCompleted))

	requireReturnsImmediately(t, tx.Cleanup)
	requireNotTerminated(t, tx)

	// Timer G keeps retransmitting the final response with nothing parked on Done().
	require.Eventually(t, func() bool {
		return outgoing.count("SIP/2.0 486 Busy Here") >= 3
	}, 4*Timer_H, Timer_G, "Timer G must retransmit the final response after Cleanup returned")

	// And an ACK arriving afterwards is still absorbed into Confirmed.
	ack := NewRequest(ACK, req.Recipient)
	ack.AppendHeader(HeaderClone(req.Via()))
	ack.AppendHeader(HeaderClone(req.From()))
	ack.AppendHeader(HeaderClone(req.To()))
	ack.AppendHeader(HeaderClone(req.CallID()))
	require.NoError(t, tx.Receive(ack))

	require.Eventually(t, func() bool {
		return compareFunctions(tx.currentFsmState(), tx.inviteStateTerminated) == nil
	}, 20*Timer_I, Timer_I/10, "Timer I must terminate the confirmed transaction")
}

func TestServerTxCleanupInviteCompletedTerminatesOnTimerH(t *testing.T) {
	setFastTimers(t)

	req, _, _ := testCreateInvite(t, "sip:127.0.0.99:5060", "UDP", cleanupTestFromAddr)
	tx, _ := newCleanupTestTx(t, req)

	require.NoError(t, tx.Respond(NewResponseFromRequest(req, StatusBusyHere, "Busy Here", nil)))
	requireReturnsImmediately(t, tx.Cleanup)

	// No ACK ever arrives: Timer H is the only thing that can reap this transaction.
	select {
	case <-tx.Done():
	case <-time.After(4 * Timer_H):
		t.Fatal("Timer H did not terminate the transaction")
	}
}

// --- INVITE 2xx: Timer L keeps the transaction alive (RFC 6026 §7.1) ---

func TestServerTxCleanupInviteAcceptedSurvivesUntilTimerL(t *testing.T) {
	setFastTimers(t)

	req, _, _ := testCreateInvite(t, "sip:127.0.0.99:5060", "UDP", cleanupTestFromAddr)
	tx, outgoing := newCleanupTestTx(t, req)

	require.NoError(t, tx.Respond(NewResponseFromRequest(req, StatusOK, "OK", nil)))
	require.NoError(t, compareFunctions(tx.currentFsmState(), tx.inviteStateAccepted))

	requireReturnsImmediately(t, tx.Cleanup)
	requireNotTerminated(t, tx)

	// A retransmitted INVITE is absorbed, not answered: the TU owns 2xx retransmission.
	require.NoError(t, tx.Receive(req))
	require.NoError(t, compareFunctions(tx.currentFsmState(), tx.inviteStateAccepted))
	require.Equal(t, 1, outgoing.count("SIP/2.0 200 OK"))

	select {
	case <-tx.Done():
	case <-time.After(4 * Timer_L):
		t.Fatal("Timer L did not terminate the transaction")
	}
}

// --- the two early-exit paths Cleanup must keep: leak safety ---

func TestServerTxCleanupTerminatesWithoutFinalResponse(t *testing.T) {
	setFastTimers(t)

	req, _, _ := testCreateInvite(t, "sip:127.0.0.99:5060", "UDP", cleanupTestFromAddr)
	tx, _ := newCleanupTestTx(t, req)

	// Handler returned after a provisional only: nothing is buffered to retransmit, so
	// leaving the transaction alive would leak it for 64*T1.
	require.NoError(t, tx.Respond(NewResponseFromRequest(req, StatusTrying, "Trying", nil)))
	requireReturnsImmediately(t, tx.Cleanup)

	<-tx.Done()
	require.Equal(t, ErrTransactionTerminated, tx.Err())
}

func TestServerTxCleanupTerminatesOnReliableTransport(t *testing.T) {
	setFastTimers(t)

	req := testCreateRequest(t, "OPTIONS", "sip:example.com", "TCP", cleanupTestFromAddr)
	tx, _ := newCleanupTestTx(t, req)

	require.NoError(t, tx.Receive(req))
	require.NoError(t, tx.Respond(NewResponseFromRequest(req, StatusOK, "OK", nil)))
	requireReturnsImmediately(t, tx.Cleanup)

	<-tx.Done()
	require.Equal(t, ErrTransactionTerminated, tx.Err())
}

// --- the deprecated wrapper keeps its blocking contract ---

func TestServerTransactionTerminateGracefullyStillBlocks(t *testing.T) {
	setFastTimers(t)

	req := testCreateRequest(t, "OPTIONS", "sip:example.com", "UDP", cleanupTestFromAddr)
	tx, _ := newCleanupTestTx(t, req)

	require.NoError(t, tx.Receive(req))
	require.NoError(t, tx.Respond(NewResponseFromRequest(req, StatusOK, "OK", nil)))

	start := time.Now()
	tx.TerminateGracefully()
	require.GreaterOrEqual(t, time.Since(start), Timer_J/2)
	require.Equal(t, ErrTransactionTerminated, tx.Err())
}
