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
	"context"
	"errors"
	"io"
	"sync"

	"github.com/oracle/go-oracledb/v26/internal/common"
	driverCommon "github.com/oracle/go-oracledb/v26/internal/driver/common"
	internallob "github.com/oracle/go-oracledb/v26/internal/lob"
	oracleErrors "github.com/oracle/go-oracledb/v26/oracle/errors"
)

// streamedLob is the private buffered read and lifecycle state for one
// locator-backed LOB decoded from a query row.
// It owns copies of the TTC locator and row prefix, and is usable only while
// its owning Rows and Statement remain open. Its mutex serializes Read,
// Size, Close, and owner invalidation, including complete TTC refills, so a
// locator can never be freed or invalidated midway through an operation.
//
// streamedLob deliberately owns no public API policy. oracle/lob.LOB exposes
// the supported application surface; blobExecutor and clobExecutor perform the
// type-specific TTC exchanges. This type coordinates their result streaming
// with row lifetime, buffered data, and temporary-LOB cleanup.
type streamedLob struct {
	// mu serializes state changes and the complete duration of one locator RPC.
	mu sync.Mutex

	// owner controls validity and operation context.
	owner *ttcRows

	// blob and clob perform type-specific TTC operations. Exactly one is set.
	blob *blobExecutor
	clob *clobExecutor
	// isNClob selects national-character conversion semantics.
	isNClob bool
	// manager owns kind dispatch and unsafe-session policy. Rows still owns the
	// cursor lifetime and supplies operation admission.
	manager lobOperationManager

	// locator owns an independent byte copy and the next 1-based server offset.
	locator *locator

	// lengthValue caches the logical size. It can bound reads only for BLOBs,
	// whose size and locator offsets are both byte counts.
	lengthValue driverCommon.UB8
	// prefix is the copied inline row payload in public byte representation.
	prefix []byte
	// prefixPos is the next unread prefix byte.
	prefixPos int
	// pending contains one converted bounded refill.
	pending []byte
	// pendingPos is the next unread refill byte.
	pendingPos int
	// nextOffset is the next 1-based Oracle byte/code-point/code-unit position.
	nextOffset driverCommon.UB8

	// eof releases Rows ownership and suppresses later RPCs.
	eof bool
	// closed records an explicit application Close.
	closed bool
	// invalidated records owner failure/close and takes precedence over closed.
	invalidated bool
}

// lobOperationManager supplies the locator operations used by a query LOB.
// lobManager is its production implementation; the narrow contract lets
// lifecycle tests provide deterministic RPC outcomes without test hooks in TTC
// executors.
type lobOperationManager interface {
	read(context.Context, internallob.Kind, *locator, driverCommon.UB8) ([]byte, driverCommon.UB8, error)
	length(context.Context, internallob.Kind, *locator) (driverCommon.UB8, error)
	chunkSize(context.Context, internallob.Kind, *locator) (driverCommon.UB8, error)
}

// newStreamedLob builds a locator-backed source from row-owned copies. It does
// not perform network I/O; the caller registers the result before exposing it.
//
// Parameters:
//   - owner: Rows that controls validity, context, and LOB ownership.
//   - dtype: TTC BLOB or CLOB datatype.
//   - prefix: inline row payload to copy and decode.
//   - metadata: locator, length, and character-set metadata from the row.
//
// Returns:
//   - *streamedLob: initialized locator-backed source.
//   - error: invalid source metadata or prefix-decoding failure.
func newStreamedLob(owner *ttcRows, dtype DtyType, prefix driverCommon.B1Array, metadata *lobColumnContext) (*streamedLob, error) {
	if owner == nil || owner.shelf == nil || metadata == nil || len(metadata.lobLocator) == 0 {
		return nil, common.NewOracleError(oracleErrors.InvalidLobSource, nil, "locator metadata")
	}
	locatorBytes := append(driverCommon.B1Array(nil), metadata.lobLocator...)
	loc := newLocator(locatorBytes, 1)
	// The minimal query API supports persistent table locators only. Temporary
	// and abstract locators require separate physical-session ownership and
	// cleanup semantics, so accepting them would reintroduce the escaped-LOB
	// lifecycle this implementation deliberately avoids.
	if metadata.temporary || loc.isTemporaryLocator() || loc.isAbstractLocator() {
		return nil, common.NewOracleError(
			oracleErrors.InvalidLobSource,
			nil,
			"temporary or abstract query LOB",
		)
	}
	value := &streamedLob{
		owner:       owner,
		manager:     newLobManager(owner.shelf, owner.sessionContext),
		locator:     loc,
		lengthValue: metadata.totalLobLength,
		nextOffset:  1,
	}

	switch dtype {
	case DtyBlob:
		value.blob = newBlobExecutor(owner.shelf.Shelf)
		value.prefix = append([]byte(nil), prefix...)
		value.nextOffset += driverCommon.UB8(len(value.prefix))
	case DtyClob:
		if owner.sessionContext == nil {
			return nil, common.NewOracleError(oracleErrors.InvalidLobSource, nil, "charset context")
		}
		isNClob := metadata.charsetForm == FormNChar
		value.clob = newClobExecutor(owner.shelf.Shelf, owner.sessionContext)
		value.isNClob = isNClob
		decodedPrefix, logical, err := value.clob.decodeReadPayload(loc, isNClob, prefix)
		if err != nil {
			return nil, err
		}
		value.prefix = decodedPrefix
		value.nextOffset += logical
	default:
		return nil, common.NewOracleError(oracleErrors.InvalidLobSource, nil, "TTC datatype")
	}
	if value.blob != nil && value.nextOffset-1 >= value.lengthValue {
		value.eof = len(value.prefix) == 0
	}
	return value, nil
}

// Kind implements internal/lob.LOBSource.
//
// Returns:
//   - internallob.Kind: BLOB, CLOB, or NCLOB selected at construction.
func (value *streamedLob) Kind() internallob.Kind {
	if value.blob != nil {
		return internallob.BLOB
	}
	if value.isNClob {
		return internallob.NCLOB
	}
	return internallob.CLOB
}

// DetachPersistentLocator verifies sessionKey owns this unread persistent locator
// and transfers it out of Rows. Detaching releases Rows ownership.
//
// Returns:
//   - []byte: independent locator-byte copy for a direct LOB handle.
//   - error: lifecycle, session-mismatch, or InvalidLOBBuffer after any read.
func (value *streamedLob) DetachPersistentLocator(sessionKey any) ([]byte, error) {
	value.mu.Lock()
	if err := value.stateErrorLocked(); err != nil {
		value.mu.Unlock()
		return nil, err
	}
	owner := value.owner
	if sessionKey == nil || owner == nil || sessionKey != owner.shelf {
		value.mu.Unlock()
		return nil, common.NewOracleError(oracleErrors.InvalidLOBBuffer, nil, "open-persistent", "LOB", "source belongs to another connection")
	}
	if value.prefixPos != 0 || value.pendingPos != 0 {
		value.mu.Unlock()
		return nil, common.NewOracleError(oracleErrors.InvalidLOBBuffer, nil, "open-persistent", "LOB", "LOB has already been read")
	}
	bytes := append([]byte(nil), value.locator.locatorBytes...)
	value.closed = true
	value.prefix = nil
	value.pending = nil
	value.mu.Unlock()
	if owner != nil {
		owner.releaseLob(value)
	}
	return bytes, nil
}

// Read implements io.Reader. It returns inline row data before issuing bounded
// locator RPCs and releases Rows ownership once the stream reaches EOF.
//
// Parameters:
//   - dst: destination for the next public BLOB or UTF-8 character bytes.
//
// Returns:
//   - int: bytes copied to dst.
//   - error: io.EOF at end of stream or a lifecycle, decode, or RPC error.
func (value *streamedLob) Read(dst []byte) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	value.mu.Lock()
	if err := value.stateErrorLocked(); err != nil {
		value.mu.Unlock()
		return 0, err
	}
	if n, finished := value.copyBufferedLocked(dst); n > 0 {
		value.mu.Unlock()
		if finished {
			value.owner.releaseLob(value)
		}
		return n, nil
	}
	if value.eof {
		value.mu.Unlock()
		return 0, io.EOF
	}

	request := value.readChunkSize()
	// Only a length expressed in locator-offset units can bound a read. CLOB
	// Size is UTF-16 units, while its locator offsets may be code points.
	if value.blob != nil {
		consumed := value.nextOffset - 1
		if consumed >= value.lengthValue {
			value.eof = true
			value.mu.Unlock()
			value.owner.releaseLob(value)
			return 0, io.EOF
		}
		remaining := value.lengthValue - consumed
		if driverCommon.UB8(request) > remaining {
			request = int(remaining)
		}
	}
	ctx, release, err := value.beginOperation()
	if err != nil {
		value.invalidated = true
		value.mu.Unlock()
		value.owner.releaseLob(value)
		return 0, common.NewOracleError(oracleErrors.LobValueInvalidated, err, "Rows owner")
	}
	value.locator.offset = value.nextOffset
	var payload []byte
	var logical driverCommon.UB8
	var readErr error
	if value.manager != nil {
		payload, logical, readErr = value.manager.read(ctx, value.Kind(), value.locator, driverCommon.UB8(request))
	} else {
		readErr = common.NewOracleError(oracleErrors.InvalidLobSource, nil, "LOB reader")
	}
	unsafeStream := readErr != nil && !isCompletedLobResponseError(readErr)
	release()
	if readErr != nil {
		value.invalidated = true
		value.mu.Unlock()
		value.owner.releaseLob(value)
		if unsafeStream {
			value.owner.invalidateAfterUnsafeLobRPC()
		}
		return 0, readErr
	}
	if len(payload) == 0 || logical == 0 {
		value.eof = true
		value.mu.Unlock()
		value.owner.releaseLob(value)
		return 0, io.EOF
	}
	value.nextOffset += logical
	value.pending = payload
	value.pendingPos = 0
	n := copyUnread(dst, value.pending, &value.pendingPos)
	finished := value.finishIfCompleteLocked()
	value.mu.Unlock()
	if finished {
		value.owner.releaseLob(value)
	}
	return n, nil
}

// Size returns the server-reported LOB length. BLOB lengths are bytes; CLOB
// and NCLOB lengths are UTF-16 units. Only BLOB lengths bound reads.
//
// Returns:
//   - int64: logical server length.
//   - error: lifecycle, range, or locator-RPC error.
func (value *streamedLob) Size() (int64, error) {
	value.mu.Lock()
	if err := value.stateErrorLocked(); err != nil {
		value.mu.Unlock()
		return 0, err
	}
	if value.blob != nil {
		length, err := checkedLobLength(value.lengthValue)
		value.mu.Unlock()
		return length, err
	}
	ctx, release, err := value.beginOperation()
	if err != nil {
		value.invalidated = true
		value.mu.Unlock()
		value.owner.releaseLob(value)
		return 0, common.NewOracleError(oracleErrors.LobValueInvalidated, err, "Rows owner")
	}
	var length driverCommon.UB8
	if value.clob != nil {
		length, err = value.manager.length(ctx, value.Kind(), value.locator)
	} else {
		err = common.NewOracleError(oracleErrors.InvalidLobSource, nil, "LOB executor")
	}
	unsafeStream := err != nil && !isCompletedLobResponseError(err)
	release()
	if err != nil {
		value.invalidated = true
		value.mu.Unlock()
		value.owner.releaseLob(value)
		if unsafeStream {
			value.owner.invalidateAfterUnsafeLobRPC()
		}
		return 0, err
	}
	checked, err := checkedLobLength(length)
	value.mu.Unlock()
	return checked, err
}

// ChunkSize reports the server storage chunk size. It does not change the
// stream's network refill size.
//
// Returns:
//   - int64: server storage chunk size in bytes.
//   - error: lifecycle, range, or locator-RPC error.
func (value *streamedLob) ChunkSize() (int64, error) {
	value.mu.Lock()
	if err := value.stateErrorLocked(); err != nil {
		value.mu.Unlock()
		return 0, err
	}
	ctx, release, err := value.beginOperation()
	if err != nil {
		value.invalidated = true
		value.mu.Unlock()
		value.owner.releaseLob(value)
		return 0, common.NewOracleError(oracleErrors.LobValueInvalidated, err, "Rows owner")
	}
	var size driverCommon.UB8
	if value.manager != nil {
		size, err = value.manager.chunkSize(ctx, value.Kind(), value.locator)
	} else {
		err = common.NewOracleError(oracleErrors.InvalidLobSource, nil, "LOB executor")
	}
	unsafeStream := err != nil && !isCompletedLobResponseError(err)
	release()
	if err != nil {
		value.invalidated = true
		value.mu.Unlock()
		value.owner.releaseLob(value)
		if unsafeStream {
			value.owner.invalidateAfterUnsafeLobRPC()
		}
		return 0, err
	}
	value.mu.Unlock()
	return checkedLobLength(size)
}

// WriteTo implements io.WriterTo by repeatedly reading into one reusable
// bounded buffer. It leaves the stream at EOF on success.
//
// Parameters:
//   - writer: destination for remaining public LOB bytes.
//
// Returns:
//   - int64: bytes written to writer.
//   - error: writer, read, or lifecycle error.
func (value *streamedLob) WriteTo(writer io.Writer) (int64, error) {
	if writer == nil {
		return 0, common.NewOracleError(oracleErrors.InvalidLOBBuffer, nil, "write-to", "lob", "nil writer")
	}
	buffer := make([]byte, value.readChunkSize())
	var total int64
	for {
		n, readErr := value.Read(buffer)
		if n > 0 {
			written, writeErr := writer.Write(buffer[:n])
			total += int64(written)
			if writeErr != nil {
				return total, writeErr
			}
			if written != n {
				return total, io.ErrShortWrite
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return total, nil
			}
			return total, readErr
		}
	}
}

// Close releases Rows ownership and discards buffered data.
//
// Returns:
//   - error: always nil.
func (value *streamedLob) Close() error {
	value.mu.Lock()
	if value.closed {
		value.mu.Unlock()
		return nil
	}
	owner := value.owner
	value.closed = true
	value.prefix = nil
	value.pending = nil
	value.mu.Unlock()
	if owner != nil {
		owner.releaseLob(value)
	}
	return nil
}

// invalidate prevents future RPCs and drops buffered data.
func (value *streamedLob) invalidate() {
	value.mu.Lock()
	value.invalidated = true
	value.prefix = nil
	value.pending = nil
	value.mu.Unlock()
}

// beginOperation serializes a locator RPC with the physical session.
//
// Returns:
//   - context.Context: exchange-scoped cancelable context.
//   - func(): required release function after the RPC.
//   - error: owner-closure or context error before an RPC begins.
func (value *streamedLob) beginOperation() (context.Context, func(), error) {
	if value.owner == nil {
		return nil, nil, common.NewOracleError(oracleErrors.LobValueInvalidated, nil, "LOB session")
	}
	return value.owner.beginLobOperation()
}

// readChunkSize returns the protocol-safe application and locator refill size.
//
// Returns:
//   - int: byte chunk for BLOB or character-unit chunk for CLOB and NCLOB.
func (value *streamedLob) readChunkSize() int {
	if value.blob != nil {
		return internallob.DefaultBlobLobChunkBytes
	}
	return internallob.DefaultCharacterLobChunkChars
}

// stateErrorLocked reports terminal state without network activity. value.mu
// must be held.
//
// Returns:
//   - error: lifecycle error, or nil when the source remains usable.
func (value *streamedLob) stateErrorLocked() error {
	if value.invalidated || value.owner == nil || value.owner.isClosed() {
		value.invalidated = true
		return common.NewOracleError(oracleErrors.LobValueInvalidated, nil, "Rows owner")
	}
	if err := value.owner.contextErr(); err != nil {
		value.invalidated = true
		return common.NewOracleError(oracleErrors.LobValueInvalidated, err, "query context")
	}
	if value.closed {
		return common.NewOracleError(oracleErrors.LobValueClosed, nil, "after Close")
	}
	return nil
}

// finishIfCompleteLocked marks BLOB EOF after buffered bytes and known length
// are consumed. value.mu must be held.
//
// Returns:
//   - bool: true when the caller must release Rows ownership.
func (value *streamedLob) finishIfCompleteLocked() bool {
	if value.eof {
		return false
	}
	if value.prefixPos < len(value.prefix) || value.pendingPos < len(value.pending) {
		return false
	}
	if value.blob != nil && value.nextOffset-1 >= value.lengthValue {
		value.eof = true
		return true
	}
	return false
}

// copyBufferedLocked copies unread prefix or refill data into dst. value.mu
// must be held.
//
// Parameters:
//   - dst: destination for unread buffered bytes.
//
// Returns:
//   - int: bytes copied from prefix or pending data.
//   - bool: true when copying completed a BLOB and releases Rows ownership.
func (value *streamedLob) copyBufferedLocked(dst []byte) (int, bool) {
	if n := copyUnread(dst, value.prefix, &value.prefixPos); n > 0 {
		return n, value.finishIfCompleteLocked()
	}
	if n := copyUnread(dst, value.pending, &value.pendingPos); n > 0 {
		return n, value.finishIfCompleteLocked()
	}
	return 0, false
}

// copyUnread copies from a buffered segment and advances its cursor.
//
// Parameters:
//   - dst: destination buffer.
//   - source: buffered source bytes.
//   - position: current source cursor to update.
//
// Returns:
//   - int: bytes copied.
func copyUnread(dst, source []byte, position *int) int {
	if *position >= len(source) {
		return 0
	}
	n := copy(dst, source[*position:])
	*position += n
	return n
}

// checkedLobLength converts the protocol's unsigned size to the public int64.
//
// Parameters:
//   - length: unsigned protocol size.
//
// Returns:
//   - int64: representable public size.
//   - error: InvalidLOBBuffer when length exceeds int64.
func checkedLobLength(length driverCommon.UB8) (int64, error) {
	const maxInt64 = int64(^uint64(0) >> 1)
	if uint64(length) > uint64(maxInt64) {
		return 0, common.NewOracleError(oracleErrors.InvalidLOBBuffer, nil, "size", "lob", "length exceeds int64")
	}
	return int64(length), nil
}
