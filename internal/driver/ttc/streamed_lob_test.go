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
	"errors"
	"testing"
	"time"

	driverCommon "github.com/oracle/go-oracledb/v26/internal/driver/common"
	internallob "github.com/oracle/go-oracledb/v26/internal/lob"
	oracleErrors "github.com/oracle/go-oracledb/v26/oracle/errors"
)

// lobEventRecorder records event delivery for tests in this package.
type lobEventRecorder struct {
	events []eventType
}

// notify implements EventListener.
func (listener *lobEventRecorder) notify(event eventType) {
	listener.events = append(listener.events, event)
}

type testLobManager struct {
	readFn   func(context.Context, internallob.Kind, *locator, driverCommon.UB8) ([]byte, driverCommon.UB8, error)
	lengthFn func(context.Context, internallob.Kind, *locator) (driverCommon.UB8, error)
}

func (manager *testLobManager) read(ctx context.Context, kind internallob.Kind, loc *locator, amount driverCommon.UB8) ([]byte, driverCommon.UB8, error) {
	return manager.readFn(ctx, kind, loc, amount)
}

func (manager *testLobManager) length(ctx context.Context, kind internallob.Kind, loc *locator) (driverCommon.UB8, error) {
	if manager.lengthFn == nil {
		return 0, nil
	}
	return manager.lengthFn(ctx, kind, loc)
}

func (*testLobManager) chunkSize(context.Context, internallob.Kind, *locator) (driverCommon.UB8, error) {
	return 0, nil
}

// recordingLobRPC emulates offset-based server reads without involving the TTC
// marshaller. It lets lifecycle tests verify offset and refill decisions.
type recordingLobRPC struct {
	data    []byte
	offsets []driverCommon.UB8
	reads   int
}

// read returns at most amount bytes beginning at the locator's 1-based offset.
func (rpc *recordingLobRPC) read(_ context.Context, loc *locator, amount driverCommon.UB8) ([]byte, driverCommon.UB8, error) {
	rpc.reads++
	rpc.offsets = append(rpc.offsets, loc.offset)
	start := int(loc.offset - 1)
	if start >= len(rpc.data) {
		return nil, 0, nil
	}
	end := start + int(amount)
	if end > len(rpc.data) {
		end = len(rpc.data)
	}
	payload := append([]byte(nil), rpc.data[start:end]...)
	return payload, driverCommon.UB8(len(payload)), nil
}

// length returns the emulated server length.
func (rpc *recordingLobRPC) length(context.Context, *locator) (driverCommon.UB8, error) {
	return driverCommon.UB8(len(rpc.data)), nil
}

// newLobTestRows constructs the minimum live owner needed by a private value.
func newLobTestRows() *ttcRows {
	rows := newTTCRows(nil)
	rows.shelf = newShelf[driverCommon.MessageType]()
	rows.setContext(context.Background())
	return rows
}

func mustRegisterLob(t *testing.T, rows *ttcRows, value *streamedLob) {
	t.Helper()
	registered, err := rows.registerLob(value)
	if err != nil {
		t.Fatalf("registerLob returned error: %v", err)
	}
	if !registered {
		t.Fatal("registerLob rejected live Rows")
	}
}

func TestStreamedLob_BoundedClobReadAmountUsesCharacterChunk(t *testing.T) {
	t.Parallel()

	wantMaximum := driverCommon.UB8(internallob.DefaultCharacterLobChunkChars)
	if got := boundedClobReadAmount(wantMaximum * 2); got != wantMaximum {
		t.Fatalf("bounded amount = %d, want %d", got, wantMaximum)
	}
	if got := boundedClobReadAmount(3); got != 3 {
		t.Fatalf("small bounded amount = %d, want 3", got)
	}
}

func newTestStreamedBlob(rows *ttcRows, manager lobOperationManager, length driverCommon.UB8, prefix []byte) *streamedLob {
	return &streamedLob{
		owner:       rows,
		blob:        &blobExecutor{},
		manager:     manager,
		locator:     newLocator(driverCommon.B1Array("locator"), 1),
		lengthValue: length,
		prefix:      prefix,
		nextOffset:  driverCommon.UB8(len(prefix) + 1),
	}
}

func TestStreamedLob_ValueConsumesPrefixThenUsesBoundedOffsets(t *testing.T) {
	t.Parallel()
	rows := newLobTestRows()
	rpc := &recordingLobRPC{data: []byte("abcdef")}
	manager := &testLobManager{readFn: func(ctx context.Context, _ internallob.Kind, loc *locator, amount driverCommon.UB8) ([]byte, driverCommon.UB8, error) {
		return rpc.read(ctx, loc, amount)
	}}
	value := newTestStreamedBlob(rows, manager, 6, []byte("ab"))
	mustRegisterLob(t, rows, value)

	first := make([]byte, 1)
	if n, err := value.Read(first); n != 1 || err != nil || string(first) != "a" {
		t.Fatalf("first Read = (%d, %v, %q), want (1, nil, a)", n, err, first)
	}
	if rpc.reads != 0 {
		t.Fatalf("prefix Read performed %d RPCs, want 0", rpc.reads)
	}
	var rest bytes.Buffer
	if _, err := value.WriteTo(&rest); err != nil || rest.String() != "bcdef" {
		t.Fatalf("WriteTo = (%q, %v), want (bcdef, nil)", rest.String(), err)
	}
	if len(rpc.offsets) != 1 || rpc.offsets[0] != 3 {
		t.Fatalf("RPC offsets = %v, want [3]", rpc.offsets)
	}
	if len(rows.lifecycle._lobs) != 0 {
		t.Fatalf("outstanding LOB count = %d after EOF, want 0", len(rows.lifecycle._lobs))
	}
}

func TestStreamedLob_ReadCancellationBeforeRPCInvalidatesValue(t *testing.T) {
	t.Parallel()
	rows := newLobTestRows()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rows.setContext(ctx)
	called := false
	value := newTestStreamedBlob(rows, &testLobManager{readFn: func(context.Context, internallob.Kind, *locator, driverCommon.UB8) ([]byte, driverCommon.UB8, error) {
		called = true
		return nil, 0, nil
	}}, 1, nil)
	mustRegisterLob(t, rows, value)
	if _, err := value.Read(make([]byte, 1)); err == nil {
		t.Fatal("Read unexpectedly succeeded")
	} else {
		requireErrorCode(t, err, oracleErrors.LobValueInvalidated)
	}
	if called {
		t.Fatal("canceled Read performed an RPC")
	}
}

func TestStreamedLob_RowsCloseInvalidatesLob(t *testing.T) {
	t.Parallel()
	rows := newLobTestRows()
	called := false
	value := newTestStreamedBlob(rows, &testLobManager{readFn: func(context.Context, internallob.Kind, *locator, driverCommon.UB8) ([]byte, driverCommon.UB8, error) {
		called = true
		return nil, 0, nil
	}}, 7, nil)
	mustRegisterLob(t, rows, value)
	if err := rows.Close(); err != nil {
		t.Fatalf("Rows.Close returned error: %v", err)
	}
	if _, err := value.Read(make([]byte, 1)); err == nil {
		t.Fatal("Read after Rows.Close unexpectedly succeeded")
	} else {
		requireErrorCode(t, err, oracleErrors.LobValueInvalidated)
	}
	if called {
		t.Fatal("Read after Rows.Close performed an RPC")
	}
}

func TestStreamedLob_RowsCloseDoesNotWaitForStalledLobRead(t *testing.T) {
	t.Parallel()
	rows := newLobTestRows()
	entered, release := make(chan struct{}), make(chan struct{})
	value := newTestStreamedBlob(rows, &testLobManager{readFn: func(context.Context, internallob.Kind, *locator, driverCommon.UB8) ([]byte, driverCommon.UB8, error) {
		close(entered)
		<-release
		return nil, 0, errors.New("transport stopped")
	}}, 1, nil)
	mustRegisterLob(t, rows, value)
	readDone := make(chan error, 1)
	go func() { _, err := value.Read(make([]byte, 1)); readDone <- err }()
	<-entered
	closeDone := make(chan error, 1)
	go func() { closeDone <- rows.Close() }()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Rows.Close returned error: %v", err)
		}
	case <-time.After(time.Second):
		close(release)
		t.Fatal("Rows.Close blocked behind an in-flight LOB read")
	}
	close(release)
	if err := <-readDone; err == nil {
		t.Fatal("in-flight Read unexpectedly succeeded")
	}
}

func TestStreamedLob_RPCFailuresApplySessionSafetyPolicy(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		err    error
		closes bool
	}{
		{"unsafe transport", errors.New("transport failed"), true},
		{"completed Oracle response", &completedLobResponseError{err: errors.New("server rejected locator operation")}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			rows := newLobTestRows()
			recorder := &lobEventRecorder{}
			rows.shelf.getEventService().register(recorder, streamerStaleEvent)
			value := newTestStreamedBlob(rows, &testLobManager{readFn: func(context.Context, internallob.Kind, *locator, driverCommon.UB8) ([]byte, driverCommon.UB8, error) {
				return nil, 0, test.err
			}}, 1, nil)
			mustRegisterLob(t, rows, value)
			if _, err := value.Read(make([]byte, 1)); err == nil {
				t.Fatal("Read returned nil error")
			}
			if rows.isClosed() != test.closes {
				t.Fatalf("Rows closed = %t, want %t", rows.isClosed(), test.closes)
			}
			if len(recorder.events) != map[bool]int{true: 1, false: 0}[test.closes] {
				t.Fatalf("stale events = %v", recorder.events)
			}
		})
	}
}

func TestStreamedLob_CloseReleasesRowsOwnershipAndIsIdempotent(t *testing.T) {
	t.Parallel()
	rows := newLobTestRows()
	value := newTestStreamedBlob(rows, &testLobManager{readFn: func(context.Context, internallob.Kind, *locator, driverCommon.UB8) ([]byte, driverCommon.UB8, error) {
		return nil, 0, nil
	}}, 1, nil)
	mustRegisterLob(t, rows, value)
	if err := value.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
	if err := value.Close(); err != nil {
		t.Fatalf("second Close returned error: %v", err)
	}
	if _, err := value.Read(make([]byte, 1)); err == nil {
		t.Fatal("Read after Close unexpectedly succeeded")
	} else {
		requireErrorCode(t, err, oracleErrors.LobValueClosed)
	}
	if len(rows.lifecycle._lobs) != 0 {
		t.Fatalf("outstanding LOB count = %d after Close, want 0", len(rows.lifecycle._lobs))
	}
}

func TestStreamedLob_ClobPrefixConversionUsesCorrectLogicalUnits(t *testing.T) {
	t.Parallel()

	session := driverCommon.NewSessionContext()
	session.SetSessionCharacterSets(al32Utf8CharSet, al16Utf16CharSet)
	executor := newClobExecutor(newShelf[driverCommon.MessageType]().Shelf, session)
	loc := newLocator(make(driverCommon.B1Array, koll4FlagOffset+1), 1)

	payload, logical, err := executor.decodeReadPayload(loc, false, []byte("A🙂"))
	if err != nil {
		t.Fatalf("CLOB decodePrefix returned error: %v", err)
	}
	if !bytes.Equal(payload, []byte("A🙂")) || logical != 3 {
		t.Fatalf("CLOB prefix = (%q, %d), want (A🙂, 3 UTF-16 units)", payload, logical)
	}

	payload, logical, err = executor.decodeReadPayload(loc, true, []byte{0xD8, 0x3D, 0xDE, 0x42})
	if err != nil {
		t.Fatalf("NCLOB decodePrefix returned error: %v", err)
	}
	if !bytes.Equal(payload, []byte("🙂")) || logical != 2 {
		t.Fatalf("NCLOB prefix = (%q, %d), want (🙂, 2 UTF-16 units)", payload, logical)
	}
}

func TestStreamedLob_QueryRowCopiesLobPrefixLocatorAndMetadata(t *testing.T) {
	t.Parallel()

	locatorBytes := make(driverCommon.B1Array, koll4FlagOffset+1)
	locatorBytes[koll4FlagOffset] = kolblTemporaryFlagByte
	metadata := &lobColumnContext{
		charsetForm:       FormNChar,
		charsetID:         al16Utf16CharSet,
		locatorByteLength: 99,
		totalLobLength:    6,
		prefetchChunkSize: 32,
		lobLocator:        locatorBytes,
	}
	rxd := &tTIrxd{
		row:           []driverCommon.B1Array{driverCommon.B1Array("prefix")},
		lobColContext: []*lobColumnContext{metadata},
	}
	state := &queryRunState{rows: newTTCRows(nil)}
	state.handleRXDRow(rxd)

	// Mutate every source category after ownership transfer.
	rxd.row[0][0] = 'X'
	metadata.charsetID = 0
	metadata.lobLocator[koll4FlagOffset] = 0

	copied := state.rows.lobColContext[0][0]
	if string(state.rows.rowData[0][0]) != "prefix" {
		t.Fatalf("copied prefix = %q, want prefix", state.rows.rowData[0][0])
	}
	if copied.charsetForm != FormNChar || copied.charsetID != al16Utf16CharSet || copied.locatorByteLength != 99 || copied.totalLobLength != 6 || copied.prefetchChunkSize != 32 {
		t.Fatalf("copied metadata changed: %+v", copied)
	}
	if !copied.temporary || copied.lobLocator[koll4FlagOffset]&kolblTemporaryFlagByte == 0 {
		t.Fatalf("temporary locator flags were not copied: %+v", copied)
	}
}
