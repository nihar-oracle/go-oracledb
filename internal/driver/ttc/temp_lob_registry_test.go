/*
** Copyright (c) 2026 Oracle and/or its affiliates.
**
** The Universal Permissive License (UPL), Version 1.0
**
** Subject to the condition set forth below, permission is hereby granted to any
** person obtaining a copy of this software, associated documentation and/or data
** (collectively the "Software"), free of charge and under any and all copyright
** rights in the Software, and any and all patent rights owned or freely
** licensable by each licensor hereunder covering either (i) the unmodified
** Software as contributed to or provided by such licensor, or (ii) the Larger
** Works (as defined below), to deal in both
**
** (a) the Software, and
** (b) any piece of software and/or hardware listed in the lrgrwrks.txt file if
** one is included with the Software (each a "Larger Work" to which the Software
** is contributed by such licensors),
**
** without restriction, including without limitation the rights to copy, create
** derivative works of, display, perform, and distribute the Software and make,
** use, sell, offer for sale, import, export, have made, and have sold the
** Software and the Larger Work(s), and to sublicense the foregoing rights on
** either these or other terms.
**
** This license is subject to the following condition:
** The above copyright notice and either this complete permission notice or at
** a minimum a reference to the UPL must be included in all copies or
** substantial portions of the Software.
**
** THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
** IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
** FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
** AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
** LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
** OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
** SOFTWARE.
 */

package ttc

import (
	"bytes"
	"context"
	"testing"

	"github.com/oracle/go-oracledb/v26/internal/driver/common"
)

type tempLobOrdinaryFunction struct{}

func (*tempLobOrdinaryFunction) GetMsgCode() common.MessageType                     { return TTIFUN }
func (*tempLobOrdinaryFunction) GetFuncCode() common.FunctionType                   { return ping }
func (*tempLobOrdinaryFunction) MarshalTo(context.Context, common.Marshaller) error { return nil }

type tempLobEventRecorder struct{ events []eventType }

func (recorder *tempLobEventRecorder) notify(event eventType) {
	recorder.events = append(recorder.events, event)
}

func pendingTemporaryLobCount(shelf *ttiShelf[common.MessageType]) int {
	shelf.lobState.temporary.mu.Lock()
	defer shelf.lobState.temporary.mu.Unlock()
	return len(shelf.lobState.temporary.entries)
}

// newReferenceCountedTempLocator returns a structurally complete test locator
// whose stable ten-byte LOB ID is deterministic. Mutable flags are outside the
// identity comparison performed by tempLobRegistry.
func newReferenceCountedTempLocator(seed byte) *locator {
	data := make(common.B1Array, kolbLobIDOffset+kolbLobIDLength)
	data[koll2FlagOffset] = kolblInitializedFlag
	data[koll4FlagOffset] = kolblTemporaryFlagByte
	for index := 0; index < kolbLobIDLength; index++ {
		data[kolbLobIDOffset+index] = seed + byte(index)
	}
	return newLocator(data, 1)
}

func TestTempLobRegistry_ReferenceCountDefersFreeUntilLastAlias(t *testing.T) {
	t.Parallel()

	shelf := newShelf[common.MessageType]()
	streamer := NewMessageStreamer(shelf)
	shelf.RegisterMessageStreamer(streamer)
	first := newReferenceCountedTempLocator(1)
	second := newLocator(append(common.B1Array(nil), first.locatorBytes...), 1)
	firstReference, err := shelf.retainTemporaryLobReference(first)
	if err != nil {
		t.Fatalf("retain first: %v", err)
	}
	secondReference, err := shelf.retainTemporaryLobReference(second)
	if err != nil {
		t.Fatalf("retain second: %v", err)
	}

	if err := releaseTemporaryLobReference(shelf, firstReference); err != nil {
		t.Fatalf("release first: %v", err)
	}
	if first.isTemporaryLocator() {
		t.Fatal("released local alias still appears temporary")
	}
	if !second.isTemporaryLocator() {
		t.Fatal("live alias was modified by another alias release")
	}

	if err := releaseTemporaryLobReference(shelf, secondReference); err != nil {
		t.Fatalf("release second: %v", err)
	}
	if streamer.outgoingMessages.Len() != 0 {
		t.Fatal("last alias sent a TTC message instead of queuing its free")
	}
	if second.isTemporaryLocator() {
		t.Fatal("last released alias still appears temporary")
	}
	if err := releaseTemporaryLobReference(shelf, secondReference); err != nil {
		t.Fatalf("idempotent second release: %v", err)
	}
}

func TestTempLobRegistry_LastReleasePiggybacksBeforeNextFunction(t *testing.T) {
	t.Parallel()

	shelf := newShelf[common.MessageType]()
	streamer := NewMessageStreamer(shelf)
	shelf.RegisterMessageStreamer(streamer)
	loc := newReferenceCountedTempLocator(17)
	reference, err := shelf.retainTemporaryLobReference(loc)
	if err != nil {
		t.Fatalf("retain: %v", err)
	}
	// Simulate TTIRPA refreshing mutable locator metadata after retain. The free
	// must use the last wrapper's current bytes, not its original snapshot.
	loc.locatorBytes[kolbLobIDOffset-1] = 0xA5
	wantLocator := append(common.B1Array(nil), loc.locatorBytes...)
	if err := releaseTemporaryLobReference(shelf, reference); err != nil {
		t.Fatalf("release: %v", err)
	}
	if streamer.outgoingMessages.Len() != 0 {
		t.Fatal("release performed an immediate TTC operation on a piggyback-capable session")
	}

	ordinary := &tTIOall{}
	if err := streamer.Push(context.Background(), ordinary); err != nil {
		t.Fatalf("Push ordinary function: %v", err)
	}
	if got := streamer.outgoingMessages.Len(); got != 2 {
		t.Fatalf("outgoing message count = %d, want piggyback plus ordinary function", got)
	}
	first := streamer.outgoingMessages.Front().Value.(common.Message[common.MessageType])
	piggyback, ok := first.(*tTIlob)
	if !ok {
		t.Fatalf("first message type = %T, want *tTIlob", first)
	}
	if piggyback.GetMsgCode() != TTIPFN || piggyback.lobPayloadDefinition.operation != kplobArrayTmpFree {
		t.Fatalf("piggyback code/operation = %v/%v", piggyback.GetMsgCode(), piggyback.lobPayloadDefinition.operation)
	}
	if !bytes.Equal(piggyback.lobPayloadDefinition.sourceLocator.locatorBytes, wantLocator) {
		t.Fatalf("piggyback locator = % X, want % X", piggyback.lobPayloadDefinition.sourceLocator.locatorBytes, wantLocator)
	}
	if streamer.outgoingMessages.Back().Value != ordinary {
		t.Fatal("ordinary TTC function was not queued after the LOB-free piggyback")
	}
}

func TestTempLobRegistry_ArrayPiggybackMarshalsWithOrdinaryFunction(t *testing.T) {
	t.Parallel()

	shelf := newShelf[common.MessageType]()
	buffer := NewArrayDataBuffer(4096)
	marshaller := NewMarshalEngine(buffer, common.BIG_ENDIAN, [5]byte{Native, Universal, Universal, Universal, Universal})
	shelf.RegisterMarshaller(marshaller)
	streamer := NewMessageStreamer(shelf)
	shelf.RegisterMessageStreamer(streamer)
	loc := newReferenceCountedTempLocator(71)
	reference, err := shelf.retainTemporaryLobReference(loc)
	if err != nil {
		t.Fatalf("retain: %v", err)
	}
	if err := releaseTemporaryLobReference(shelf, reference); err != nil {
		t.Fatalf("release: %v", err)
	}
	if err := streamer.Push(context.Background(), &tempLobOrdinaryFunction{}); err != nil {
		t.Fatalf("Push: %v", err)
	}
	shelf.lobState.temporary.mu.Lock()
	pendingBeforeFlush := len(shelf.lobState.temporary.entries)
	shelf.lobState.temporary.mu.Unlock()
	if pendingBeforeFlush != 1 {
		t.Fatalf("pending entries before Flush = %d, want 1", pendingBeforeFlush)
	}
	if err := streamer.Flush(context.Background()); err != nil {
		t.Fatalf("Flush piggyback plus ordinary function: %v", err)
	}
	shelf.lobState.temporary.mu.Lock()
	pendingAfterFlush := len(shelf.lobState.temporary.entries)
	shelf.lobState.temporary.mu.Unlock()
	if pendingAfterFlush != 0 {
		t.Fatalf("pending entries after Flush = %d, want 0", pendingAfterFlush)
	}
	if buffer.currentWritePosition == 0 {
		t.Fatal("piggyback and ordinary function produced no TTC bytes")
	}
}

func TestTempLobRegistry_PiggybackBatchesPendingLocators(t *testing.T) {
	t.Parallel()

	shelf := newShelf[common.MessageType]()
	streamer := NewMessageStreamer(shelf)
	shelf.RegisterMessageStreamer(streamer)
	first := newReferenceCountedTempLocator(111)
	second := newReferenceCountedTempLocator(112)
	for _, loc := range []*locator{first, second} {
		reference, err := shelf.retainTemporaryLobReference(loc)
		if err != nil {
			t.Fatalf("retain: %v", err)
		}
		if err := releaseTemporaryLobReference(shelf, reference); err != nil {
			t.Fatalf("release: %v", err)
		}
	}

	ordinary := &tempLobOrdinaryFunction{}
	if err := streamer.Push(context.Background(), ordinary); err != nil {
		t.Fatalf("Push ordinary function: %v", err)
	}
	if streamer.outgoingMessages.Len() != 2 {
		t.Fatalf("outgoing message count = %d, want one piggyback plus ordinary function", streamer.outgoingMessages.Len())
	}
	piggyback, ok := streamer.outgoingMessages.Front().Value.(*tTIlob)
	if !ok {
		t.Fatalf("first outgoing message = %T, want *tTIlob", streamer.outgoingMessages.Front().Value)
	}
	if got, want := len(piggyback.lobPayloadDefinition.sourceLocator.locatorBytes), len(first.locatorBytes)+len(second.locatorBytes); got != want {
		t.Fatalf("array-free locator bytes = %d, want %d for both pending locators", got, want)
	}
}

func TestTempLobRegistry_PiggybackMarshallingFailureRestoresPendingBatch(t *testing.T) {
	shelf := newShelf[common.MessageType]()
	shelf.RegisterMarshaller(NewMarshalEngine(NewArrayDataBuffer(0), common.BIG_ENDIAN, [5]byte{Native, Universal, Universal, Universal, Universal}))
	streamer := NewMessageStreamer(shelf)
	shelf.RegisterMessageStreamer(streamer)
	loc := newReferenceCountedTempLocator(121)
	reference, err := shelf.retainTemporaryLobReference(loc)
	if err != nil {
		t.Fatalf("retain: %v", err)
	}
	if err := releaseTemporaryLobReference(shelf, reference); err != nil {
		t.Fatalf("release: %v", err)
	}
	if err := streamer.Push(context.Background(), &tempLobOrdinaryFunction{}); err != nil {
		t.Fatalf("Push: %v", err)
	}
	if err := streamer.Flush(context.Background()); err == nil {
		t.Fatal("Flush succeeded with a zero-capacity marshalling buffer")
	}
	if got := pendingTemporaryLobCount(shelf); got != 1 {
		t.Fatalf("pending entries after pre-transport marshal failure = %d, want 1", got)
	}
	shelf.lobState.temporary.mu.Lock()
	entry := shelf.lobState.temporary.entries[tempLobID(loc.locatorBytes[kolbLobIDOffset:kolbLobIDOffset+kolbLobIDLength])]
	queued := entry != nil && entry.queued
	shelf.lobState.temporary.mu.Unlock()
	if queued {
		t.Fatal("pre-transport marshal failure left the free batch reserved")
	}
	streamer.Drain(context.Background(), common.OUT)
	shelf.RegisterMarshaller(NewMarshalEngine(NewArrayDataBuffer(4096), common.BIG_ENDIAN, [5]byte{Native, Universal, Universal, Universal, Universal}))
	if err := streamer.Push(context.Background(), &tempLobOrdinaryFunction{}); err != nil {
		t.Fatalf("Push after safe restore: %v", err)
	}
	if got := streamer.outgoingMessages.Len(); got != 2 {
		t.Fatalf("outgoing messages after safe restore = %d, want piggyback plus ordinary function", got)
	}
}

func TestTempLobRegistry_PiggybackTransportFailureInvalidatesAndDiscards(t *testing.T) {
	shelf := newShelf[common.MessageType]()
	buffer := NewArrayDataBuffer(4096)
	buffer.returnFlushError = true
	shelf.RegisterMarshaller(NewMarshalEngine(buffer, common.BIG_ENDIAN, [5]byte{Native, Universal, Universal, Universal, Universal}))
	recorder := &tempLobEventRecorder{}
	shelf.getEventService().register(recorder, streamerStaleEvent)
	streamer := NewMessageStreamer(shelf)
	shelf.RegisterMessageStreamer(streamer)
	loc := newReferenceCountedTempLocator(122)
	reference, err := shelf.retainTemporaryLobReference(loc)
	if err != nil {
		t.Fatalf("retain: %v", err)
	}
	if err := releaseTemporaryLobReference(shelf, reference); err != nil {
		t.Fatalf("release: %v", err)
	}
	if err := streamer.Push(context.Background(), &tempLobOrdinaryFunction{}); err != nil {
		t.Fatalf("Push: %v", err)
	}
	if err := streamer.Flush(context.Background()); err == nil {
		t.Fatal("Flush succeeded despite transport failure")
	}
	if got := pendingTemporaryLobCount(shelf); got != 0 {
		t.Fatalf("pending entries after ambiguous transport failure = %d, want 0", got)
	}
	if len(recorder.events) != 1 || recorder.events[0] != streamerStaleEvent {
		t.Fatalf("events = %v, want [streamerStaleEvent]", recorder.events)
	}
	streamer.Drain(context.Background(), common.OUT)
	if err := streamer.Push(context.Background(), &tempLobOrdinaryFunction{}); err != nil {
		t.Fatalf("Push after transport failure: %v", err)
	}
	if got := streamer.outgoingMessages.Len(); got != 1 {
		t.Fatalf("outgoing messages after ambiguous failure = %d, want ordinary function only", got)
	}
}

func TestTempLobRegistry_PiggybackIsNotRestoredAfterMainResponseFailure(t *testing.T) {
	shelf := newShelf[common.MessageType]()
	buffer := NewArrayDataBuffer(4096)
	shelf.RegisterMarshaller(NewMarshalEngine(buffer, common.BIG_ENDIAN, [5]byte{Native, Universal, Universal, Universal, Universal}))
	streamer := NewMessageStreamer(shelf)
	shelf.RegisterMessageStreamer(streamer)
	loc := newReferenceCountedTempLocator(123)
	reference, err := shelf.retainTemporaryLobReference(loc)
	if err != nil {
		t.Fatalf("retain: %v", err)
	}
	if err := releaseTemporaryLobReference(shelf, reference); err != nil {
		t.Fatalf("release: %v", err)
	}
	if err := streamer.Push(context.Background(), &tempLobOrdinaryFunction{}); err != nil {
		t.Fatalf("Push: %v", err)
	}
	if err := streamer.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := pendingTemporaryLobCount(shelf); got != 0 {
		t.Fatalf("pending entries after successful transport flush = %d, want 0", got)
	}
	buffer.currentReadPosition = buffer.currentWritePosition
	if _, err := streamer.Pull(context.Background(), TTIRPA); err == nil {
		t.Fatal("Pull succeeded without a main-RPC response")
	}
	if err := streamer.Push(context.Background(), &tempLobOrdinaryFunction{}); err != nil {
		t.Fatalf("Push after response failure: %v", err)
	}
	if got := streamer.outgoingMessages.Len(); got != 1 {
		t.Fatalf("outgoing messages after response failure = %d, want ordinary function only", got)
	}
}

func TestTempLobRegistry_PiggybackRejectsPayloadBeyondOLOBOPSLengthLimit(t *testing.T) {
	shelf := newShelf[common.MessageType]()
	streamer := NewMessageStreamer(shelf)
	shelf.RegisterMessageStreamer(streamer)
	loc := newReferenceCountedTempLocator(124)
	reference, err := shelf.retainTemporaryLobReference(loc)
	if err != nil {
		t.Fatalf("retain: %v", err)
	}
	if err := releaseTemporaryLobReference(shelf, reference); err != nil {
		t.Fatalf("release: %v", err)
	}
	shelf.lobState.temporary.maxPendingBytes = len(loc.locatorBytes) - 1
	if err := streamer.Push(context.Background(), &tempLobOrdinaryFunction{}); err == nil {
		t.Fatal("Push accepted an array-free payload beyond its OLOBOPS limit")
	}
	if got := pendingTemporaryLobCount(shelf); got != 1 {
		t.Fatalf("pending entries after size rejection = %d, want 1", got)
	}
	if got := streamer.outgoingMessages.Len(); got != 0 {
		t.Fatalf("outgoing messages after size rejection = %d, want 0", got)
	}
}

func TestTempLobRegistry_PiggybackPreservesExistingPiggybackOrdering(t *testing.T) {
	shelf := newShelf[common.MessageType]()
	streamer := NewMessageStreamer(shelf)
	shelf.RegisterMessageStreamer(streamer)
	otherPiggyback := newTTIlobPiggyback().(*tTIlob)
	if err := streamer.Push(context.Background(), otherPiggyback); err != nil {
		t.Fatalf("Push existing piggyback: %v", err)
	}
	loc := newReferenceCountedTempLocator(125)
	reference, err := shelf.retainTemporaryLobReference(loc)
	if err != nil {
		t.Fatalf("retain: %v", err)
	}
	if err := releaseTemporaryLobReference(shelf, reference); err != nil {
		t.Fatalf("release: %v", err)
	}
	ordinary := &tempLobOrdinaryFunction{}
	if err := streamer.Push(context.Background(), ordinary); err != nil {
		t.Fatalf("Push ordinary function: %v", err)
	}
	if got := streamer.outgoingMessages.Len(); got != 3 {
		t.Fatalf("outgoing messages = %d, want existing piggyback, temp-free piggyback, ordinary function", got)
	}
	if got := streamer.outgoingMessages.Front().Value; got != otherPiggyback {
		t.Fatalf("first outgoing message = %T, want existing piggyback", got)
	}
	tempFree := streamer.outgoingMessages.Front().Next().Value.(*tTIlob)
	if tempFree.lobPayloadDefinition.operation != kplobArrayTmpFree {
		t.Fatalf("second piggyback operation = %v, want %v", tempFree.lobPayloadDefinition.operation, kplobArrayTmpFree)
	}
	if got := streamer.outgoingMessages.Back().Value; got != ordinary {
		t.Fatal("ordinary function was not queued after temporary-LOB piggyback")
	}
}

func TestTempLobRegistry_LogoffDiscardsPendingFree(t *testing.T) {
	t.Parallel()

	shelf := newShelf[common.MessageType]()
	streamer := NewMessageStreamer(shelf)
	shelf.RegisterMessageStreamer(streamer)
	loc := newReferenceCountedTempLocator(89)
	reference, err := shelf.retainTemporaryLobReference(loc)
	if err != nil {
		t.Fatalf("retain: %v", err)
	}
	if err := releaseTemporaryLobReference(shelf, reference); err != nil {
		t.Fatalf("release: %v", err)
	}

	logoff := newLogOff()
	if err := streamer.Push(context.Background(), logoff); err != nil {
		t.Fatalf("Push LOGOFF: %v", err)
	}
	if got := streamer.outgoingMessages.Len(); got != 1 {
		t.Fatalf("outgoing message count = %d, want LOGOFF without a LOB-free piggyback", got)
	}
	if streamer.outgoingMessages.Front().Value != logoff {
		t.Fatal("LOGOFF was not the only queued message")
	}
	shelf.lobState.temporary.mu.Lock()
	remaining := len(shelf.lobState.temporary.entries)
	shelf.lobState.temporary.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("registry retains %d entries after LOGOFF", remaining)
	}
}

func TestTempLobRegistry_UsesStableLobIDNotMutableFlags(t *testing.T) {
	t.Parallel()

	registry := newTempLobRegistry()
	first := newReferenceCountedTempLocator(31)
	second := newLocator(append(common.B1Array(nil), first.locatorBytes...), 1)
	second.locatorBytes[koll1FlagOffset] |= kolblAbstractLocatorFlag
	firstReference, err := registry.retain(first)
	if err != nil {
		t.Fatalf("retain first: %v", err)
	}
	secondReference, err := registry.retain(second)
	if err != nil {
		t.Fatalf("retain second: %v", err)
	}
	if err := registry.release(firstReference); err != nil {
		t.Fatalf("first release returned error: %v", err)
	}
	if err := registry.release(secondReference); err != nil {
		t.Fatalf("second release returned error: %v", err)
	}
	batch, err := registry.reservePending()
	if err != nil {
		t.Fatalf("reserve pending: %v", err)
	}
	if batch == nil || len(batch.locators) == 0 {
		t.Fatal("last release did not queue a piggyback free")
	}
	registry.completePending(batch)
}

func TestTempLobRegistry_ReferenceRejectsLocatorWithoutCompleteID(t *testing.T) {
	t.Parallel()

	data := make(common.B1Array, koll4FlagOffset+1)
	data[koll2FlagOffset] = kolblInitializedFlag
	data[koll4FlagOffset] = kolblTemporaryFlagByte
	if _, err := newTempLobRegistry().retain(newLocator(data, 1)); err == nil {
		t.Fatal("retain accepted a temporary locator without its complete ten-byte LOB ID")
	}
}

func TestTempLobRegistry_ReleaseReferenceAcceptsNil(t *testing.T) {
	t.Parallel()

	if err := releaseTemporaryLobReference(newShelf[common.MessageType](), nil); err != nil {
		t.Fatalf("nil reference release returned error: %v", err)
	}
}
