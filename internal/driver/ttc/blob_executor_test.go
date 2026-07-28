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
	"strings"
	"testing"

	"github.com/oracle/go-driver/driver/common"
)

// TestBlobExecutor_CreateTemporaryLob verifies the BLOB-specific temporary
// locator metadata, returned locator, and complete OLOBOPS wire request.
func TestBlobExecutor_CreateTemporaryLob(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const duration common.UB4 = 10

	// Step 1: verify the definition values required specifically for a BLOB.
	def := NewLobDefinitionForTemporaryCreate(
		kolllTempWithSignature,
		blobFormOfUse,
		common.UB8(DtyBlob),
		duration,
		true,
		blobCharsetIDPlaceholder,
	)
	if def.operation != kplobTmpCreate {
		t.Fatalf("operation = %v, want %v", def.operation, kplobTmpCreate)
	}
	if def.sourceLocator.offset != common.UB8(blobFormOfUse) {
		t.Fatalf("form-of-use = %d, want %d", def.sourceLocator.offset, blobFormOfUse)
	}
	if def.destinationLocator.offset != common.UB8(DtyBlob) {
		t.Fatalf("LOB type = %d, want DtyBlob (%d)", def.destinationLocator.offset, DtyBlob)
	}
	if def.charsetID != blobCharsetIDPlaceholder {
		t.Fatalf("charset ID = %d, want BLOB protocol placeholder %d", def.charsetID, blobCharsetIDPlaceholder)
	}
	if def.destinationLength != common.SB4(duration) || def.lobAmt != common.UB8(duration) {
		t.Fatalf("duration fields = (%d, %d), want (%d, %d)", def.destinationLength, def.lobAmt, duration, duration)
	}
	if len(def.lobscn) != 1 || def.lobscn[0] != 1 {
		t.Fatalf("cache metadata = %v, want [1]", def.lobscn)
	}
	wantHeader := []byte{0x00, byte(kolllTempWithSignature - kolbLocatorLengthHeaderBytes)}
	if !bytes.Equal(def.sourceLocator.locatorBytes[:2], wantHeader) {
		t.Fatalf("locator header = % X, want % X", def.sourceLocator.locatorBytes[:2], wantHeader)
	}

	// Step 2: stage a minimal successful TTIRPA containing a complete temporary
	// locator, charset placeholder, returned duration, and non-NULL status.
	shelf, _, dbuf := newLobTestShelf(4096)
	wantLocator := make(common.B1Array, kolllTempWithSignature)
	copy(wantLocator[:2], wantHeader)
	wantLocator[2] = 0xA5
	response := append(common.B1Array{byte(TTIRPA)}, wantLocator...)
	response = append(response,
		0x01, byte(blobCharsetIDPlaceholder),
		0x01, byte(duration),
		0x00,
	)
	if err := dbuf.WriteBytesWithContext(ctx, response); err != nil {
		t.Fatalf("stage temporary BLOB response: %v", err)
	}
	marshalWritePosition := dbuf.currentWritePosition

	// Step 3: execute the production API and verify the returned locator.
	gotLocator, err := NewBlobExecutor(shelf).CreateTemporaryLob(ctx, true, duration)
	if err != nil {
		t.Fatalf("CreateTemporaryLob() error = %v", err)
	}
	if !bytes.Equal(gotLocator, wantLocator) {
		t.Fatalf("locator mismatch:\n got: % X\nwant: % X", gotLocator, wantLocator)
	}

	// Step 4: compare every emitted byte with the fixed BLOB OLOBOPS request.
	wantMarshal := makeLobPayloadFromDump(blobTempLocatorMarshalGoldenPayload)
	gotMarshal := dbuf.bytes[marshalWritePosition:dbuf.currentWritePosition]
	if !bytes.Equal(gotMarshal, wantMarshal) {
		t.Fatalf("marshal payload mismatch:\n got: % X\nwant: % X", gotMarshal, wantMarshal)
	}
}

// TestBlobExecutor_Write verifies that Write emits the BLOB OLOBOPS request
// followed by an exact TTILOBD copy of a small binary payload.
func TestBlobExecutor_Write(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	payload := common.B1Array{0x00, 0xFF, 0x10}
	locatorBytes := newTestLocator(false)
	locatorBytes[koll1FlagOffset] = kolblBlobFlag

	// Step 1: stage a minimal TTIRPA response containing the returned locator
	// and byte count. The test shelf supplies the terminal successful OER.
	shelf, _, dbuf := newLobTestShelf(1024)
	response := append(common.B1Array{byte(TTIRPA)}, locatorBytes...)
	response = append(response, 0x01, byte(len(payload)))
	if err := dbuf.WriteBytesWithContext(ctx, response); err != nil {
		t.Fatalf("stage BLOB write response: %v", err)
	}
	marshalWritePosition := dbuf.currentWritePosition

	// Step 2: execute Write with bytes that cannot be mistaken for text.
	written, err := NewBlobExecutor(shelf).Write(
		ctx,
		newLocator(append(common.B1Array(nil), locatorBytes...), 1),
		payload,
	)
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if written != common.UB8(len(payload)) {
		t.Fatalf("Write() = %d bytes, want %d", written, len(payload))
	}

	// Step 3: compare the complete OLOBOPS and TTILOBD request. This checks the
	// write opcode, locator, offset, byte amount, CLR length, and payload bytes.
	wantMarshal := makeLobPayloadFromDump(blobWriteMarshalGoldenPayload)
	gotMarshal := dbuf.bytes[marshalWritePosition:dbuf.currentWritePosition]
	if !bytes.Equal(gotMarshal, wantMarshal) {
		t.Fatalf("marshal payload mismatch:\n got: % X\nwant: % X", gotMarshal, wantMarshal)
	}
}

// TestBlobExecutor_Read verifies that a real TTILOBD response copies raw
// binary bytes into the caller-provided buffer without character conversion
// and that Read emits the exact BLOB OLOBOPS request.
func TestBlobExecutor_Read(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	want := common.B1Array{0x00, 0xFF, 0x10, 0x80, 0x41, 0x00, 0x7F}
	locatorBytes := newTestLocator(false)
	locatorBytes[koll1FlagOffset] = kolblBlobFlag

	// Step 1: build a minimal response containing one short binary TTILOBD
	// frame followed by TTIRPA locator and amount fields.
	response := make(common.B1Array, 0, 4+len(want)+len(locatorBytes))
	response = append(response, byte(TTILOBD), byte(len(want)))
	response = append(response, want...)
	response = append(response, byte(TTIRPA))
	response = append(response, locatorBytes...)
	response = append(response, 0x01, byte(len(want)))

	// Step 2: stage the real TTC response in the same in-memory marshaller used
	// by the CLOB read test.
	shelf, _, dbuf := newLobTestShelf(8192)
	if err := dbuf.WriteBytesWithContext(ctx, response); err != nil {
		t.Fatalf("stage BLOB read response: %v", err)
	}
	marshalWritePosition := dbuf.currentWritePosition

	// Step 3: use a BLOB locator and execute the production read path.
	out := make(common.B1Array, len(want))
	read, err := NewBlobExecutor(shelf).Read(
		ctx,
		newLocator(append(common.B1Array(nil), locatorBytes...), 1),
		common.UB8(len(out)),
		out,
	)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	// Step 4: verify both the reported amount and every transferred byte.
	if read != common.UB8(len(want)) {
		t.Fatalf("Read() = %d bytes, want %d", read, len(want))
	}
	if !bytes.Equal(out[:read], want) {
		t.Fatalf("Read() payload mismatch:\n got: % X\nwant: % X", out[:read], want)
	}

	// Step 5: compare the complete request with the fixed read golden payload.
	wantMarshal := makeLobPayloadFromDump(blobReadMarshalGoldenPayload)
	gotMarshal := dbuf.bytes[marshalWritePosition:dbuf.currentWritePosition]
	if !bytes.Equal(gotMarshal, wantMarshal) {
		t.Fatalf("marshal payload mismatch:\n got: % X\nwant: % X", gotMarshal, wantMarshal)
	}
}

// TestBlobExecutor_Errors verifies BLOB-specific validation and transport
// failure propagation for temporary creation and binary streaming.
func TestBlobExecutor_Errors(t *testing.T) {
	ctx := context.Background()

	t.Run("rejects BFILE-only open mode", func(t *testing.T) {
		// Step 1: pass a BFILE mode to a BLOB executor.
		_, err := NewBlobExecutor(&common.Shelf[common.MessageType]{}).Open(ctx, newLocator(newTestLocator(false), 0), BfileOpenModeReadOnly)

		// Step 2: ensure the public API returns the documented validation error.
		if code := getErrorCode(err); code != string(common.InvalidLOBBuffer) || !strings.Contains(err.Error(), "unsupported BFILE open mode") {
			t.Fatalf("Open(BFILE mode) error = %v; want InvalidLOBBuffer", err)
		}
	})

	for _, tt := range []struct {
		name string
		run  func(*BlobExecutor) error
	}{
		{"temporary create", func(e *BlobExecutor) error { _, err := e.CreateTemporaryLob(ctx, false, 1); return err }},
		{"write", func(e *BlobExecutor) error {
			_, err := e.Write(ctx, newLocator(newTestLocator(false), 0), common.B1Array{1})
			return err
		}},
		{"read", func(e *BlobExecutor) error {
			_, err := e.Read(ctx, newLocator(newTestLocator(false), 0), 1, make(common.B1Array, 1))
			return err
		}},
	} {
		t.Run("propagates transport failure from "+tt.name, func(t *testing.T) {
			// Step 1: make the TTC streamer fail while flushing the operation.
			exec := newBlobExecutorWithStub(lobExecutorScenario{flushErr: errors.New("flush failed")})
			err := tt.run(exec)

			// Step 2: verify callers receive the standard LOB execution classification.
			if code := getErrorCode(err); code != string(common.LobExecError) {
				t.Fatalf("%s error code = %q, want %q (error: %v)", tt.name, code, common.LobExecError, err)
			}
		})
	}

	for _, locatorCase := range []struct {
		name      string
		configure func(common.B1Array)
		detail    string
	}{
		{"value-based", func(l common.B1Array) { l[koll1FlagOffset] = kolblValueBasedLocatorFlag }, "value-based"},
		{"read-only", func(l common.B1Array) { l[koll3FlagOffset] = kolblReadOnlyFlag }, "read-only"},
	} {
		t.Run("rejects "+locatorCase.name+" locator for write", func(t *testing.T) {
			// Step 1: mark the locator as incapable of a BLOB write.
			locatorBytes := newTestLocator(false)
			locatorCase.configure(locatorBytes)
			_, err := NewBlobExecutor(&common.Shelf[common.MessageType]{}).Write(
				ctx,
				newLocator(locatorBytes, 0),
				common.B1Array{1},
			)

			// Step 2: verify validation fails before any TTC operation is attempted.
			if code := getErrorCode(err); code != string(common.InvalidLOBBuffer) || !strings.Contains(err.Error(), locatorCase.detail) {
				t.Fatalf("Write() error = %v; want InvalidLOBBuffer for %s locator", err, locatorCase.name)
			}
		})
	}
}

// newBlobExecutorWithStub constructs a BlobExecutor backed by the shared fake
// TTC streamer so BLOB tests can deterministically control protocol outcomes.
func newBlobExecutorWithStub(s lobExecutorScenario) *BlobExecutor {
	shelf, _, _ := newLobTestShelf(8192)
	stub := &fakeStreamer{
		pushErr:   s.pushErr,
		flushErr:  s.flushErr,
		pullErr:   s.pullErr,
		locator:   s.locator,
		events:    make([]common.Message[common.MessageType], 0),
		preHooks:  make(map[common.MessageType]StreamerPreUnmarshallCallback),
		postHooks: make(map[common.MessageType]StreamerPostUnmarshallCallback),
		pullHook:  s.pullHook,
	}
	shelf.RegisterMessageStreamer(stub)
	blob := NewBlobExecutor(shelf)
	stub.executor = blob.lobExecutor

	if len(s.events) == 0 {
		msg, err := shelf.GetMessageFactory().(Factory).GetMessageForFunction(TTIRPA, oLobOps)
		if err != nil {
			panic(err)
		}
		s.events = []common.Message[common.MessageType]{msg, &mockOer{}}
	}
	stub.events = append(stub.events, s.events...)
	if s.onFlush != nil {
		stub.onFlush = func() { s.onFlush(blob.lobExecutor) }
	}

	return blob
}

// blobTempLocatorMarshalGoldenPayload is the complete OLOBOPS request for a
// cached temporary BLOB with duration 10, binary form-of-use metadata, and the
// required non-zero charset placeholder.
var blobTempLocatorMarshalGoldenPayload = []string{
	`"03 60 01 00 01 01 6C 00"`,
	`"01 0A 00 00 01 00 01 02"`,
	`"01 10 01 01 01 00 01 71"`,
	`"01 00 00 00 00 00 00 00"`,
	`"6A 00 00 00 00 00 00 00"`,
	`"00 00 00 00 00 00 00 00"`,
	`"00 00 00 00 00 00 00 00"`,
	`"00 00 00 00 00 00 00 00"`,
	`"00 00 00 00 00 00 00 00"`,
	`"00 00 00 00 00 00 00 00"`,
	`"00 00 00 00 00 00 00 00"`,
	`"00 00 00 00 00 00 00 00"`,
	`"00 00 00 00 00 00 00 00"`,
	`"00 00 00 00 00 00 00 00"`,
	`"00 00 00 00 00 00 00 00"`,
	`"00 00 00 00 00 00 00 00"`,
	`"00 00 00 00 00 00 00 00"`,
	`"00 00 00 01 01 01 01 01 0A"`,
}

// blobWriteMarshalGoldenPayload is the complete OLOBOPS plus TTILOBD request
// for a three-byte BLOB write at offset one.
var blobWriteMarshalGoldenPayload = []string{
	`"03 60 01 00 01 01 08 00"`,
	`"00 00 00 00 00 00 01 40"`,
	`"00 00 01 01 00 01 00 00"`,
	`"00 00 00 00 00 00 00 00"`,
	`"01 00 00 00 01 03 0E 03"`,
	`"00 FF 10"`,
}

// blobReadMarshalGoldenPayload is the complete OLOBOPS request for a
// seven-byte BLOB read at offset one.
var blobReadMarshalGoldenPayload = []string{
	`"03 60 01 00 01 01 08 00"`,
	`"00 00 00 00 00 00 01 02"`,
	`"00 00 01 01 00 01 00 00"`,
	`"00 00 00 00 00 00 00 00"`,
	`"01 00 00 00 01 07"`,
}
