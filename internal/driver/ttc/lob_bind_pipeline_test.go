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
	"database/sql/driver"
	"errors"
	"io"
	"testing"

	"github.com/oracle/go-oracledb/v26/internal/common"
	driverCommon "github.com/oracle/go-oracledb/v26/internal/driver/common"
	internallob "github.com/oracle/go-oracledb/v26/internal/lob"
	oracleErrors "github.com/oracle/go-oracledb/v26/oracle/errors"
)

// recordingBlobBindWriter records bounded chunks and locator offsets.
type recordingBlobBindWriter struct {
	chunks  [][]byte
	offsets []driverCommon.UB8
	short   bool
}

// failIfReadReader records accidental source consumption during bind preflight.
type failIfReadReader struct{ read bool }

// Read marks the source consumed and fails immediately.
func (reader *failIfReadReader) Read([]byte) (int, error) {
	reader.read = true
	return 0, errors.New("reader must not be consumed")
}

// write implements lobBinaryWriter.
func (writer *recordingBlobBindWriter) write(_ context.Context, loc *locator, payload driverCommon.B1Array) (driverCommon.UB8, error) {
	writer.offsets = append(writer.offsets, loc.offset)
	writer.chunks = append(writer.chunks, append([]byte(nil), payload...))
	if writer.short && len(payload) > 0 {
		return driverCommon.UB8(len(payload) - 1), nil
	}
	return driverCommon.UB8(len(payload)), nil
}

// recordingClobBindWriter records rune chunks and Oracle logical offsets.
type recordingClobBindWriter struct {
	chunks  []string
	offsets []driverCommon.UB8
	short   bool
}

// logicalAmount implements lobCharacterWriter.
func (*recordingClobBindWriter) logicalAmount(runes []rune) (driverCommon.UB8, error) {
	return driverCommon.UB8(lobCharacterUnits(runes)), nil
}

// write implements lobCharacterWriter.
func (writer *recordingClobBindWriter) write(_ context.Context, loc *locator, isNClob bool, runes []rune, _ int) (driverCommon.UB8, error) {
	writer.offsets = append(writer.offsets, loc.offset)
	writer.chunks = append(writer.chunks, string(runes))
	amount, _ := writer.logicalAmount(runes)
	if writer.short && amount > 0 {
		amount--
	}
	return amount, nil
}

func TestLobBindPipeline_StreamBlobInputIsBoundedAndHonorsDeclaredSize(t *testing.T) {
	t.Parallel()

	payload := bytes.Repeat([]byte("x"), internallob.DefaultBlobLobChunkBytes+7)
	source := bytes.NewBuffer(append(append([]byte(nil), payload...), 'z'))
	writer := &recordingBlobBindWriter{}
	loc := newLocator(driverCommon.B1Array("locator"), 1)
	input := internallob.NewInput(source, internallob.BLOB, int64(len(payload)))

	if _, err := streamBlobInput(context.Background(), writer.write, loc, input); err != nil {
		t.Fatalf("streamBlobInput returned error: %v", err)
	}
	if len(writer.chunks) != 2 || len(writer.chunks[0]) != internallob.DefaultBlobLobChunkBytes || len(writer.chunks[1]) != 7 {
		t.Fatalf("chunk lengths = [%d %d], want [%d 7]", len(writer.chunks[0]), len(writer.chunks[1]), internallob.DefaultBlobLobChunkBytes)
	}
	if len(writer.offsets) != 2 || writer.offsets[0] != 1 || writer.offsets[1] != driverCommon.UB8(internallob.DefaultBlobLobChunkBytes+1) {
		t.Fatalf("offsets = %v, want [1 %d]", writer.offsets, internallob.DefaultBlobLobChunkBytes+1)
	}
	remaining, err := io.ReadAll(source)
	if err != nil || string(remaining) != "z" {
		t.Fatalf("bytes after declared size = %q, %v; want z", remaining, err)
	}
}

func TestLobBindPipeline_BindBlobUsesSingleZeroCopyWrite(t *testing.T) {
	t.Parallel()
	payload := bytes.Repeat([]byte{0xAB}, 2*internallob.DefaultBlobLobChunkBytes+7)
	loc := newLocator(driverCommon.B1Array("locator"), 1)
	calls := 0
	write := func(_ context.Context, _ *locator, data driverCommon.B1Array) (driverCommon.UB8, error) {
		calls++
		if len(data) != len(payload) || &data[0] != &payload[0] {
			t.Fatal("BindBlob payload was copied or split before the TTC write")
		}
		return driverCommon.UB8(len(data)), nil
	}

	if _, err := streamBlobInput(context.Background(), write, loc, internallob.NewBlobInput(payload)); err != nil {
		t.Fatalf("streamBlobInput returned error: %v", err)
	}
	if calls != 1 {
		t.Fatalf("TTC write calls = %d, want 1", calls)
	}
}

// TestLobBindPipeline_NormalizeLobBindInputsConvertsMarkersToInputs verifies that every
// public LOB marker enters the existing streamed-LOB bind path rather than a
// LOB-specific executor or OAC registry path.
func TestLobBindPipeline_NormalizeLobBindInputsConvertsMarkersToInputs(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		value   any
		kind    internallob.Kind
		payload []byte
	}{
		{name: "BLOB", value: internallob.BindBlob{1, 2, 3}, kind: internallob.BLOB, payload: []byte{1, 2, 3}},
		{name: "CLOB", value: internallob.BindClob("CLOB text"), kind: internallob.CLOB, payload: []byte("CLOB text")},
		{name: "NCLOB", value: internallob.BindNClob("NCLOB text"), kind: internallob.NCLOB, payload: []byte("NCLOB text")},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			args := []driver.NamedValue{
				{Ordinal: 1, Value: testCase.value},
				{Ordinal: 2, Value: int64(7)},
			}

			normalized := normalizeLobBindInputs(args)
			if &normalized[0] == &args[0] {
				t.Fatal("normalization returned the original argument slice")
			}
			input, ok := normalized[0].Value.(internallob.Input)
			if !ok {
				t.Fatalf("normalized LOB type = %T, want internallob.Input", normalized[0].Value)
			}
			if input.Kind() != testCase.kind || input.Size() != int64(len(testCase.payload)) {
				t.Fatalf("normalized LOB metadata = (%v, %d), want (%v, %d)", input.Kind(), input.Size(), testCase.kind, len(testCase.payload))
			}
			if blob, ok := testCase.value.(internallob.BindBlob); ok {
				retained, retainedOK := input.BlobBytes()
				if !retainedOK || len(retained) != len(blob) || &retained[0] != &blob[0] {
					t.Fatal("normalized BindBlob did not retain the original bytes")
				}
			}
			if got, err := io.ReadAll(input.Reader()); err != nil || !bytes.Equal(got, testCase.payload) {
				t.Fatalf("normalized LOB reader = %v, %v; want %v, nil", got, err, testCase.payload)
			}
			if _, ok := normalized[1].Value.(int64); !ok {
				t.Fatalf("non-LOB argument type = %T, want int64", normalized[1].Value)
			}
		})
	}
}

// TestLobBindPipeline_NormalizeLobBindInputsLeavesOrdinaryValuesUntouched verifies ordinary
// binds avoid an unnecessary argument-slice allocation.
func TestLobBindPipeline_NormalizeLobBindInputsLeavesOrdinaryValuesUntouched(t *testing.T) {
	t.Parallel()

	args := []driver.NamedValue{{Ordinal: 1, Value: int64(7)}}
	normalized := normalizeLobBindInputs(args)
	if &normalized[0] != &args[0] {
		t.Fatal("ordinary bind arguments were unnecessarily copied")
	}
}

// TestLobBindPipeline_CheckNamedValueAcceptsLOBMarkers verifies that database/sql does not
// flatten an explicit LOB marker before bind normalization.
func TestLobBindPipeline_CheckNamedValueAcceptsLOBMarkers(t *testing.T) {
	t.Parallel()

	for _, value := range []any{internallob.BindBlob{1, 2, 3}, internallob.BindClob("CLOB"), internallob.BindNClob("NCLOB")} {
		if err := checkNamedValue(&driver.NamedValue{Ordinal: 1, Value: value}); err != nil {
			t.Fatalf("checkNamedValue(%T) returned error: %v", value, err)
		}
	}
}

func TestLobBindPipeline_PrepareStreamedLobBindsValidatesAllInputsBeforeReading(t *testing.T) {
	t.Parallel()

	firstSource := &failIfReadReader{}
	args := []driver.NamedValue{
		{Ordinal: 1, Value: internallob.NewInput(firstSource, internallob.BLOB, 1)},
		{Ordinal: 2, Value: internallob.NewInput(nil, internallob.CLOB, 0)},
	}
	_, _, err := prepareStreamedLobBinds(
		context.Background(),
		newShelf[driverCommon.MessageType](),
		newTestSessionContext(),
		args,
	)
	if err == nil {
		t.Fatal("prepareStreamedLobBinds unexpectedly succeeded")
	} else {
		requireErrorCode(t, err, oracleErrors.InvalidLobInput)
	}
	if firstSource.read {
		t.Fatal("first LOB source was consumed before all inputs were validated")
	}
}

func TestLobBindPipeline_IsTerminalOracleError(t *testing.T) {
	t.Parallel()

	serverErr := common.NewOracleError(oracleErrors.ErrorCode("ORA-00001"), nil)
	if !isTerminalOracleError(serverErr) {
		t.Fatal("direct terminal ORA error was not recognized")
	}
	wrapped := common.NewOracleError(oracleErrors.RunExecError, serverErr, "execute")
	if isTerminalOracleError(wrapped) {
		t.Fatal("wrapped error was incorrectly accepted as a direct terminal response")
	}
	if isTerminalOracleError(common.NewOracleError(oracleErrors.StreamerReadError, io.ErrUnexpectedEOF)) {
		t.Fatal("transport EOF was classified as a synchronized TTC exchange")
	}
}

func TestLobBindPipeline_StreamBlobInputRejectsShortSourceAndAcknowledgement(t *testing.T) {
	t.Parallel()

	loc := newLocator(driverCommon.B1Array("locator"), 1)
	disposition, err := streamBlobInput(
		context.Background(),
		(&recordingBlobBindWriter{}).write,
		loc,
		internallob.NewInput(bytes.NewReader([]byte("abc")), internallob.BLOB, 4),
	)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("short source error = %v, want ErrUnexpectedEOF", err)
	}
	requireErrorCode(t, err, oracleErrors.InvalidLobInput)
	if disposition != lobCleanupFreeNow {
		t.Fatalf("short source disposition = %v, want free now", disposition)
	}

	disposition, err = streamBlobInput(
		context.Background(),
		(&recordingBlobBindWriter{short: true}).write,
		loc,
		internallob.NewInput(bytes.NewReader([]byte("abc")), internallob.BLOB, 3),
	)
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short acknowledgement error = %v, want ErrShortWrite", err)
	}
	requireErrorCode(t, err, oracleErrors.InvalidLobInput)
	if disposition != lobCleanupFreeNow {
		t.Fatalf("short acknowledgement disposition = %v, want free now", disposition)
	}
}

func TestLobBindPipeline_StreamBlobInputAbandonsCleanupAfterAmbiguousWriteFailure(t *testing.T) {
	t.Parallel()

	disposition, err := streamBlobInput(
		context.Background(),
		func(context.Context, *locator, driverCommon.B1Array) (driverCommon.UB8, error) {
			return 0, errors.New("write transport failure")
		},
		newLocator(driverCommon.B1Array("locator"), 1),
		internallob.NewInput(bytes.NewReader([]byte("abc")), internallob.BLOB, 3),
	)
	if err == nil {
		t.Fatal("ambiguous write unexpectedly succeeded")
	}
	if disposition != lobCleanupAbandon {
		t.Fatalf("ambiguous write disposition = %v, want abandon", disposition)
	}
}

func TestLobBindPipeline_StreamCLOBAndNCLOBInputUseUCS2AmountsAndOffsets(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		kind internallob.Kind
	}{
		{name: "CLOB", kind: internallob.CLOB},
		{name: "NCLOB", kind: internallob.NCLOB},
	} {
		t.Run(tc.name, func(t *testing.T) {
			writer := &recordingClobBindWriter{}
			loc := newLocator(driverCommon.B1Array("locator"), 1)
			payload := []byte("A🙂")
			source := bytes.NewBuffer(append(append([]byte(nil), payload...), 'z'))
			input := internallob.NewInput(source, tc.kind, int64(len(payload)))
			if _, err := streamClobInput(context.Background(), func(ctx context.Context, loc *locator, isNClob bool, runes []rune) (driverCommon.UB8, error) {
				return writer.write(ctx, loc, isNClob, runes, len(runes))
			}, writer.logicalAmount, loc, input, tc.kind == internallob.NCLOB); err != nil {
				t.Fatalf("streamClobInput returned error: %v", err)
			}
			if len(writer.chunks) != 1 || writer.chunks[0] != "A🙂" {
				t.Fatalf("chunks = %q, want A🙂", writer.chunks)
			}
			if len(writer.offsets) != 1 || writer.offsets[0] != 1 || loc.offset != 4 {
				t.Fatalf("offsets = %v final=%d, want [1] final=4", writer.offsets, loc.offset)
			}
			if _, err := writer.write(context.Background(), loc, tc.kind == internallob.NCLOB, []rune("B"), 1); err != nil {
				t.Fatalf("following write returned error: %v", err)
			}
			if len(writer.offsets) != 2 || writer.offsets[1] != 4 {
				t.Fatalf("following write offset = %v, want [1 4]", writer.offsets)
			}
			remaining, err := io.ReadAll(source)
			if err != nil || string(remaining) != "z" {
				t.Fatalf("bytes after declared size = %q, %v; want z", remaining, err)
			}
		})
	}
}

func TestLobBindPipeline_StreamClobInputRejectsMalformedUTF8(t *testing.T) {
	t.Parallel()

	_, err := streamClobInput(
		context.Background(),
		func(ctx context.Context, loc *locator, isNClob bool, runes []rune) (driverCommon.UB8, error) {
			return (&recordingClobBindWriter{}).write(ctx, loc, isNClob, runes, len(runes))
		},
		(&recordingClobBindWriter{}).logicalAmount,
		newLocator(driverCommon.B1Array("locator"), 1),
		internallob.NewInput(bytes.NewReader([]byte{0xf0, 0x9f}), internallob.CLOB, 2),
		false,
	)
	if err == nil {
		t.Fatal("malformed UTF-8 input unexpectedly succeeded")
	} else {
		requireErrorCode(t, err, oracleErrors.InvalidLobInput)
	}
}

func TestLobBindPipeline_EncodeLobLocatorBindUsesLobOAC(t *testing.T) {
	t.Parallel()

	encoded, marshalled, err := encodeLobLocatorBind(lobLocatorBind{
		locator:     driverCommon.B1Array("locator"),
		kind:        internallob.NCLOB,
		charsetForm: FormNChar,
		charsetID:   al16Utf16CharSet,
	})
	if err != nil {
		t.Fatalf("encodeLobLocatorBind returned error: %v", err)
	}
	oac := marshalled.(*tTIoac)
	if string(encoded) != "locator" || DtyType(oac.dataType) != DtyClob || oac.characterSetForm != FormNChar || oac.characterSetID != al16Utf16CharSet {
		t.Fatalf("encoded=%q oac=%+v, want NCLOB locator metadata", encoded, oac)
	}
}

func TestLobBindPipeline_EnqueueTemporaryLobFreeDefersAndInvalidatesLocator(t *testing.T) {
	t.Parallel()

	shelf := newShelf[driverCommon.MessageType]()
	streamer := NewMessageStreamer(shelf)
	shelf.RegisterMessageStreamer(streamer)
	loc := newReferenceCountedTempLocator(101)
	loc.locatorBytes[koll1FlagOffset] |= kolblAbstractLocatorFlag
	loc.locatorBytes[koll4FlagOffset] |= kolblOpenFlagByte | kolblReadWriteFlagByte

	if err := shelf.enqueueTemporaryLobFree(loc); err != nil {
		t.Fatalf("enqueueTemporaryLobFree returned error: %v", err)
	}
	if streamer.outgoingMessages.Len() != 0 {
		t.Fatal("enqueueTemporaryLobFree sent a standalone TTC request")
	}
	if loc.locatorBytes[koll1FlagOffset]&kolblAbstractLocatorFlag != 0 ||
		loc.locatorBytes[koll2FlagOffset]&kolblInitializedFlag != 0 ||
		loc.locatorBytes[koll4FlagOffset]&(kolblTemporaryFlagByte|kolblOpenFlagByte|kolblReadWriteFlagByte) != 0 {
		t.Fatalf("locator flags were not cleared: %v", loc.locatorBytes)
	}
	if err := shelf.enqueueTemporaryLobFree(loc); err == nil {
		t.Fatal("second enqueue accepted an already-freed locator")
	}
	if err := streamer.Push(context.Background(), &tempLobOrdinaryFunction{}); err != nil {
		t.Fatalf("Push ordinary function: %v", err)
	}
	if streamer.outgoingMessages.Len() != 2 {
		t.Fatalf("outgoing message count = %d, want piggyback plus ordinary function", streamer.outgoingMessages.Len())
	}
}

func TestLobBindPipeline_CanceledLobExchangeUsesBreakResetAndRestoresStream(t *testing.T) {
	t.Parallel()

	shelf := newShelf[driverCommon.MessageType]()
	cancelCalled := make(chan struct{}, 1)
	shelf.registerCancelExecution(func(context.Context) error {
		cancelCalled <- struct{}{}
		return nil
	})
	parent, cancelParent := context.WithCancel(context.Background())
	pulls := 0
	streamer := &fakeStreamer{}
	streamer.pullHook = func(ctx context.Context, _ ...driverCommon.MessageType) (driverCommon.Message[driverCommon.MessageType], error) {
		pulls++
		if pulls == 1 {
			cancelParent()
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return &mockOer{}, nil
	}
	shelf.RegisterMessageStreamer(streamer)
	operationContext, _, cleanup := shelf.cancellation.newCancelableOperationContext(parent, shelf.cancelExecution)
	defer cleanup()
	executor := newLobExecutor()
	executor.setShelf(shelf.Shelf)

	err := executor.execute(operationContext, newLobDefinitionForGetLengthOperation(newLocator(driverCommon.B1Array("locator"), 1)))
	if !isCompletedLobResponseError(err) {
		t.Fatalf("execute error = %v, want completed cancellation error", err)
	}
	if pulls != 2 {
		t.Fatalf("Pull count = %d, want canceled pull plus terminal TTIOER pull", pulls)
	}
	select {
	case <-cancelCalled:
	default:
		t.Fatal("break/reset callback was not invoked")
	}
}

// TestLobBindPipeline_CanceledLobExchangeDiscardsStreamWhenRecoveryFails verifies that a LOB
// cancellation remains unsafe unless break/reset also consumes its terminal
// response.
func TestLobBindPipeline_CanceledLobExchangeDiscardsStreamWhenRecoveryFails(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name        string
		breakErr    error
		terminalErr error
	}{
		{name: "break reset failure", breakErr: errors.New("break/reset failed")},
		{name: "terminal pull failure", terminalErr: errors.New("terminal response unavailable")},
	} {
		t.Run(test.name, func(t *testing.T) {
			shelf := newShelf[driverCommon.MessageType]()
			shelf.registerCancelExecution(func(context.Context) error { return test.breakErr })
			parent, cancelParent := context.WithCancel(context.Background())
			streamer := &fakeStreamer{}
			pulls := 0
			streamer.pullHook = func(ctx context.Context, _ ...driverCommon.MessageType) (driverCommon.Message[driverCommon.MessageType], error) {
				pulls++
				if pulls == 1 {
					cancelParent()
					<-ctx.Done()
					return nil, ctx.Err()
				}
				return nil, test.terminalErr
			}
			shelf.RegisterMessageStreamer(streamer)
			operationContext, _, cleanup := shelf.cancellation.newCancelableOperationContext(parent, shelf.cancelExecution)
			defer cleanup()
			executor := newLobExecutor()
			executor.setShelf(shelf.Shelf)

			err := executor.execute(operationContext, newLobDefinitionForGetLengthOperation(newLocator(driverCommon.B1Array("locator"), 1)))
			if isCompletedLobResponseError(err) {
				t.Fatalf("execute error = %v, unexpectedly marked as completed", err)
			}
			if !requiresLobSessionDiscard(err) {
				t.Fatalf("execute error = %v, want session discard", err)
			}
			if test.breakErr != nil && pulls != 1 {
				t.Fatalf("Pull count = %d, want 1 when break/reset fails", pulls)
			}
			if test.terminalErr != nil && pulls != 2 {
				t.Fatalf("Pull count = %d, want canceled pull plus terminal pull", pulls)
			}
		})
	}
}
