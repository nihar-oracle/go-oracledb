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
	"log/slog"

	"github.com/oracle/go-driver/driver/common"
)

const (
	// blobFormOfUse identifies the form-of-use value used for binary LOB locators.
	// Binary LOBs do not carry character semantics, so the value remains zero.
	blobFormOfUse common.UB2 = 0

	// blobCharsetIDPlaceholder supplies the non-zero charset value required by
	// the OLOBOPS temporary-create protocol. It makes the charset pointer
	// non-null but does not apply character conversion to BLOB data.
	blobCharsetIDPlaceholder common.UB2 = 1
)

// BlobExecutor orchestrates BLOB operations on top of the shared lobExecutor.
// The executor intentionally keeps the API surface byte-oriented so higher
// layers do not perform redundant conversions when streaming binary payloads.
type BlobExecutor struct {
	*lobExecutor
}

// NewBlobExecutor constructs a BlobExecutor using the supplied message shelf.
//
// Inputs:
//   - shelf: message shelf shared across TTC executors.
//
// Outputs:
//   - *BlobExecutor: executor backed by the shared lobExecutor.
//
// Errors:
//   - None.
func NewBlobExecutor(shelf *common.Shelf[common.MessageType]) *BlobExecutor {
	base := newLobExecutor()
	base.SetShelf(shelf)

	return &BlobExecutor{lobExecutor: base}
}

// Open delegates to the shared lobExecutor while preventing unsupported BFILE
// open modes from slipping through the binary LOB path.
//
// Inputs:
//   - ctx: request-scoped context for cancellation and deadlines.
//   - lobLocator: BLOB locator to open.
//   - mode: desired access mode, either LobOpenModeReadOnly or
//     LobOpenModeReadWrite.
//
// Outputs:
//   - bool: true when the locator was opened, or false when no state change was
//     required.
//   - error: nil when the operation succeeds.
//
// Errors:
//   - Returns InvalidLOBBuffer for the BFILE-only read mode.
//   - Propagates validation, TTC, protocol, and network failures from the
//     shared lobExecutor.
//
// Behaviour:
//   - Temporary and abstract locators are opened by updating their local
//     locator flags.
//   - Persistent locators are opened through an OLOBOPS round trip.
func (b *BlobExecutor) Open(ctx context.Context, lobLocator *locator, mode LobOpenMode) (bool, error) {
	if mode == BfileOpenModeReadOnly {
		err := common.NewOracleError(common.InvalidLOBBuffer, nil, "open", "blob", "unsupported BFILE open mode")
		common.Odl.Error("BlobExecutor.Open: invalid open mode", "mode", mode, "error", err)
		return false, err
	}

	return b.lobExecutor.open(ctx, lobLocator, mode)
}

// Close closes the locator and clears client side flags via the base executor.
//
// Inputs:
//   - ctx: request-scoped context for cancellation and deadlines.
//   - lobLocator: BLOB locator to close.
//
// Outputs:
//   - error: nil when the operation succeeds.
//
// Errors:
//   - Propagates locator-state, TTC, protocol, and network failures from the
//     shared lobExecutor.
func (b *BlobExecutor) Close(ctx context.Context, lobLocator *locator) error {
	return b.lobExecutor.close(ctx, lobLocator)
}

// GetChunkSize retrieves the server reported chunk (page) size for the locator.
//
// Inputs:
//   - ctx: request-scoped context for cancellation and deadlines.
//   - lobLocator: BLOB locator whose storage chunk size is requested.
//
// Outputs:
//   - common.UB8: chunk size in bytes reported by the server.
//   - error: nil when the operation succeeds.
//
// Errors:
//   - Propagates TTC, protocol, and network failures from the shared
//     lobExecutor.
func (b *BlobExecutor) GetChunkSize(ctx context.Context, lobLocator *locator) (common.UB8, error) {
	return b.lobExecutor.GetChunkSize(ctx, lobLocator)
}

// GetLength retrieves the server reported length for the supplied locator.
//
// Inputs:
//   - ctx: request-scoped context for cancellation and deadlines.
//   - lobLocator: BLOB locator whose length is requested.
//
// Outputs:
//   - common.UB8: BLOB length in bytes reported by the server.
//   - error: nil when the operation succeeds.
//
// Errors:
//   - Propagates TTC, protocol, and network failures from the shared
//     lobExecutor.
func (b *BlobExecutor) GetLength(ctx context.Context, lobLocator *locator) (common.UB8, error) {
	return b.lobExecutor.GetLength(ctx, lobLocator)
}

// Trim truncates or extends the LOB to the provided length.
//
// Inputs:
//   - ctx: request-scoped context for cancellation and deadlines.
//   - lobLocator: BLOB locator whose length should be changed.
//   - newLength: desired BLOB length in bytes.
//
// Outputs:
//   - common.UB8: resulting length in bytes reported by the server.
//   - error: nil when the operation succeeds.
//
// Errors:
//   - Returns InvalidLOBBuffer for value-based or read-only locators.
//   - Propagates TTC, protocol, and network failures from the shared
//     lobExecutor.
func (b *BlobExecutor) Trim(ctx context.Context, lobLocator *locator, newLength common.UB8) (common.UB8, error) {
	return b.lobExecutor.Trim(ctx, lobLocator, newLength)
}

// IsOpen interrogates the locator open state using the base executor helper.
//
// Inputs:
//   - ctx: request-scoped context for cancellation and deadlines.
//   - lobLocator: BLOB locator whose open state is requested.
//
// Outputs:
//   - bool: true when the locator is open.
//   - error: nil when the operation succeeds.
//
// Errors:
//   - Propagates TTC, protocol, and network failures from the shared
//     lobExecutor.
//
// Behaviour:
//   - Temporary and abstract locator state is read locally.
//   - Persistent locator state is queried through an OLOBOPS round trip.
func (b *BlobExecutor) IsOpen(ctx context.Context, lobLocator *locator) (bool, error) {
	return b.lobExecutor.IsOpen(ctx, lobLocator)
}

// CreateTemporaryLob provisions a temporary BLOB locator.
//
// Inputs:
//   - ctx: request-scoped context for cancellation and deadlines.
//   - cache: whether Oracle should place temporary BLOB data in the buffer
//     cache.
//   - duration: server duration hint for the temporary BLOB lifecycle.
//
// Outputs:
//   - common.B1Array: locator bytes for the newly created temporary BLOB.
//   - error: nil when the operation succeeds.
//
// Errors:
//   - Propagates TTC, protocol, and network failures from the shared
//     lobExecutor.
//
// Behaviour:
//   - Temporary BLOBs use binary form-of-use and a non-zero charset placeholder
//     required by OLOBOPS. The placeholder does not give binary data character
//     semantics or trigger character conversion.
func (b *BlobExecutor) CreateTemporaryLob(ctx context.Context, cache bool, duration common.UB4) (common.B1Array, error) {
	tempSize := kolllTempWithSignature
	def := NewLobDefinitionForTemporaryCreate(
		tempSize,
		blobFormOfUse,
		common.UB8(DtyBlob),
		duration,
		cache,
		blobCharsetIDPlaceholder,
	)

	if common.Odl.Enabled(ctx, slog.LevelDebug) {
		common.Odl.Debug(
			"BlobExecutor.CreateTemporaryLob: before execute",
			"cache", cache,
			"duration", duration,
			"tempSize", tempSize,
			"operation", def.operation,
		)
	}

	if err := b.lobExecutor.execute(ctx, def); err != nil {
		common.Odl.Error("BlobExecutor.CreateTemporaryLob: execute failed",
			"error", err,
			"operation", def.operation,
		)
		return nil, err
	}

	return def.sourceLocator.locatorBytes, nil
}

// Write streams the supplied binary payload into the target locator using the
// shared lobExecutor.write helper.
//
// Inputs:
//   - ctx: request-scoped context for cancellation and deadlines.
//   - lobLocator: writable BLOB locator and byte offset for the operation.
//   - data: raw bytes to write without character-set conversion.
//
// Outputs:
//   - common.UB8: number of bytes reported written by the server.
//   - error: nil when the operation succeeds.
//
// Errors:
//   - Returns InvalidLOBBuffer for value-based or read-only locators.
//   - Propagates TTC, protocol, and network failures from the shared
//     lobExecutor.
//
// Buffer requirements:
//   - The entire data slice is sent in this operation. Its length determines
//     the OLOBOPS byte amount.
func (b *BlobExecutor) Write(
	ctx context.Context,
	lobLocator *locator,
	data common.B1Array,
) (common.UB8, error) {
	return b.lobExecutor.write(ctx, lobLocator, data, common.UB8(len(data)))
}

// Read copies raw BLOB bytes from the locator into the caller-supplied buffer
// using the shared lobExecutor.read helper.
//
// Inputs:
//   - ctx: request-scoped context for cancellation and deadlines.
//   - lobLocator: readable BLOB locator and byte offset for the operation.
//   - numBytes: maximum number of raw bytes requested from the server.
//   - outBuffer: caller-owned destination for the returned bytes.
//
// Outputs:
//   - common.UB8: number of bytes copied into outBuffer.
//   - error: nil when the operation succeeds.
//
// Errors:
//   - Propagates TTC, protocol, unmarshalling, and network failures from the
//     shared lobExecutor.
//
// Buffer requirements:
//   - len(outBuffer) should be at least numBytes. Supplying a smaller buffer can
//     truncate a long CLR response or fail unmarshalling a short CLR response.
//   - Only the returned number of bytes in outBuffer contain response data.
func (b *BlobExecutor) Read(
	ctx context.Context,
	lobLocator *locator,
	numBytes common.UB8,
	outBuffer common.B1Array,
) (common.UB8, error) {
	return b.lobExecutor.read(ctx, lobLocator, numBytes, outBuffer)
}
