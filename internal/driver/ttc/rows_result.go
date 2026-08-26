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
	"database/sql/driver"
	"io"
	"reflect"
	"sync"
	"time"

	"github.com/oracle/go-oracledb/v26/internal/common"
	driverCommon "github.com/oracle/go-oracledb/v26/internal/driver/common"
	oracleErrors "github.com/oracle/go-oracledb/v26/oracle/errors"
)

/*
lobColumnContext captures LOB-specific metadata needed to decode pre-fetched
LOB payloads.

Description:

	LOB columns carry additional metadata beyond the generic columnContext fields.
	This metadata describes the LOB character set for text-based LOBs and the
	server-provided prefetch details used when the row already includes LOB data.

Fields:
  - charsetForm: Oracle character set form for the LOB payload (for example, database or national character set).
  - charsetID: Oracle character set identifier used to decode textual LOB payloads.
  - locatorByteLength: Byte length of the locator representation in the RXD row.
  - totalLobLength: Total logical LOB length reported by the server.
  - prefetchChunkSize: Suggested chunk size for subsequent LOB reads when additional data must be fetched.
  - lobLocator: Opaque locator bytes that identify the server-side LOB and allow follow-up read operations.
*/
type lobColumnContext struct {
	charsetForm       driverCommon.UB1
	charsetID         driverCommon.UB2
	locatorByteLength driverCommon.UB4
	totalLobLength    driverCommon.UB8
	prefetchChunkSize driverCommon.UB4
	lobLocator        driverCommon.B1Array
	// Temporary is copied from the locator flags while decoding the row. It is
	// retained separately so ownership decisions never depend on mutable RXD
	// buffers.
	temporary bool
}

// columnContext aggregates the metadata required to interpret a column's value
// at runtime. The TTC protocol delivers column descriptors separately from row
// payloads, so columnContext instances are constructed on-demand while scanning
// rows.
//
// Only the fields required by the decode helpers are surfaced here. Additional
// metadata can be appended without impacting existing decode helpers because
// the struct is passed by value.
type columnContext struct {
	Index                int
	Name                 driverCommon.B1Array
	SchemaName           driverCommon.B1Array
	DBTypeName           driverCommon.B1Array
	ScanType             *reflect.Type
	Length               int64
	DataType             DtyType
	Precision            int64
	Scale                int8
	KernelPosition       int
	ColumnFlags          uint32
	CharsetForm          uint8
	CharsetID            uint16
	Nullable             bool
	lobContext           *lobColumnContext
	serverTimeZoneOffset int16
}

// rowsLifecycle groups the state that controls whether Rows and its
// locator-backed values may continue to use the physical session. It is
// protected by ttcRows.mu.
type rowsLifecycle struct {
	// closed makes Close idempotent and invalidates child locators.
	_closed bool
	// ctx follows the caller's QueryContext and cancel stops queued or in-flight
	// locator work when Rows closes.
	_ctx    context.Context
	_cancel context.CancelFunc
	// statement is the Statement which produced this result. ownsStatement is
	// true only for the direct Connection.QueryContext path.
	_statement     *Statement
	_ownsStatement bool
	// lobs owns every locator value that remains usable. Rows.Close and owner
	// invalidation end their lifetime; Rows.Next deliberately does not.
	_lobs map[*streamedLob]struct{}
	// decodingLobs records values created while decoding one row, allowing a
	// partial row-decode failure to invalidate only those values.
	_decodingLobs map[*streamedLob]struct{}
}

// ttcRows implements database/sql/driver.Rows and owns one completed query
// result. The current executor buffers row payloads, while ttcRows also retains
// the Statement/session lifetime required for future locator-backed LOB reads.
//
// A direct Connection.QueryContext result owns its internally-created
// Statement and closes it from Rows.Close. Rows produced by a prepared
// Statement hold a non-owning reference and merely detach on close so that the
// Statement can be reused. The mutex protects only lifecycle fields; row
// iteration remains governed by database/sql's single-consumer Rows contract.
//
// Implemented interfaces:
//   - driver.Rows
//   - RowsColumnTypeDatabaseTypeName
//   - RowsColumnTypeLength
//   - RowsColumnTypeNullable
//   - RowsColumnTypePrecisionScale
//   - RowsColumnTypeScanType
type ttcRows struct {
	// mu protects lifecycle and row index. It must not be held while calling
	// Statement or streamedLob methods or performing TTC cleanup.
	mu        sync.Mutex
	lifecycle rowsLifecycle

	// rowData stores one raw TTC value slice per buffered result row.
	rowData [][]driverCommon.B1Array
	// currentRowIdx is the zero-based row returned by the next call to Next.
	currentRowIdx int
	// numOfRows is the number of complete rows available in rowData.
	numOfRows int

	// columnContexts stores immutable column metadata and backs ColumnType APIs.
	columnContexts []columnContext
	// lobColContext is aligned with rowData and retains row-specific locator,
	// prefix, length, and character-set metadata.
	lobColContext [][]*lobColumnContext
	// shelf supplies codecs, localization, connection properties, and the shared
	// physical-session operation guard.
	shelf *ttiShelf[driverCommon.MessageType]
	// sessionContext supplies negotiated database and national character sets to
	// the existing clobExecutor.
	sessionContext *driverCommon.SessionContext
	// strictNullHandlingValue controls whether SQL NULL is preserved or mapped to
	// legacy type-specific zero values.
	strictNullHandlingValue bool
}

// SetShelf injects the shared TTC shelf instance used to resolve codecs and
// connection-level properties during row decoding.
func (r *ttcRows) SetShelf(shelf *ttiShelf[driverCommon.MessageType]) {
	r.shelf = shelf
	r.strictNullHandlingValue = true
	if r.shelf.Shelf.GetConnectionProperties() != nil {
		r.strictNullHandlingValue = r.shelf.Shelf.GetConnectionProperties().IsStrictNullValueHandling()
	}
}

// SetSessionContext injects immutable negotiated character-set state used by
// locator-backed CLOB and NCLOB values.
func (r *ttcRows) SetSessionContext(sessionContext *driverCommon.SessionContext) {
	r.sessionContext = sessionContext
}

// setContext gives Rows a lifecycle context derived from the original public
// QueryContext context, not the statement's exchange-only child context.
func (r *ttcRows) setContext(parent context.Context) {
	r.mu.Lock()
	oldCancel := r.lifecycle._cancel
	r.lifecycle._ctx, r.lifecycle._cancel = context.WithCancel(parent)
	closed := r.lifecycle._closed
	if closed {
		r.lifecycle._cancel()
	}
	r.mu.Unlock()
	if oldCancel != nil {
		oldCancel()
	}
}

// Columns implements driver.Rows.Columns. It returns the column names prepared
// during construction from decoded column metadata. The returned slice is a
// fresh allocation, so callers cannot mutate the stored descriptors.
func (r *ttcRows) Columns() []string {
	res := make([]string, len(r.columnContexts))
	for i, colCtx := range r.columnContexts {
		res[i] = driverCommon.B1ArrayToString(colCtx.Name)
	}
	// TODO : shall we cache this ?
	return res
}

// Next implements driver.Rows.Next. It advances the cursorId and assigns each
// column's raw []driverCommon.B1Array value as a type provided in dest. Row count is
// computed once and cached to avoid repeated len() calls.
func (r *ttcRows) Next(dest []driver.Value) error {
	r.mu.Lock()
	if r.lifecycle._closed {
		r.mu.Unlock()
		return io.EOF
	}
	if r.lifecycle._ctx != nil {
		if err := r.lifecycle._ctx.Err(); err != nil {
			r.mu.Unlock()
			return err
		}
	}
	rowIndex := r.currentRowIdx
	clear(r.lifecycle._decodingLobs)
	r.mu.Unlock()
	if rowIndex >= r.numOfRows {
		return io.EOF
	}
	rawRow := r.rowData[rowIndex]
	for i := range rawRow {
		val, err := r.decodeColumnValue(i)
		if err != nil {
			r.invalidateCurrentRowLobs()
			return r.shelf.LocalizeError(err)
		}
		dest[i] = val
	}
	r.mu.Lock()
	if r.lifecycle._closed {
		r.mu.Unlock()
		r.invalidateCurrentRowLobs()
		return io.EOF
	}
	r.currentRowIdx++
	r.mu.Unlock()
	return nil
}

// decodeColumnValue returns the decoded driver.Value for the current row's column i.
//
// Behaviour:
//   - Oracle NULLs are detected via zero-length payloads.
//   - When a decoder is available for the column's TTC datatype, it is invoked; otherwise
//     the raw protocol bytes are surfaced unchanged.
//
// Errors:
//   - Any error propagated from the registered TTC codec for the column's datatype.
func (r *ttcRows) decodeColumnValue(i int) (driver.Value, error) {
	// Fast-path locals to minimize repeated bounds checks and lookups
	colCtx := r.columnContexts[i]
	dtype := colCtx.DataType
	scale := colCtx.Scale
	data := r.rowData[r.currentRowIdx][i]
	colCtx.lobContext = r.lobColContext[r.currentRowIdx][i]
	colCtx.serverTimeZoneOffset = r.shelf.getServerTimeZoneOffset()
	// BLOB, CLOB, and NCLOB columns are always represented by a locator source.
	// The database/sql scan destination selects streaming or materialization.
	// JSON uses a LOB transport internally but retains its historical string.
	if dtype == DtyBlob || dtype == DtyClob {
		metadata := colCtx.lobContext
		if metadata == nil {
			return nil, common.NewOracleError(
				oracleErrors.InvalidLobSource,
				nil,
				"row metadata",
			)
		}
		// The current RXD protocol represents SQL NULL with a zero LOB length and
		// does not send the remaining locator fields. Do this check before
		// requiring a locator, but never treat a non-empty prefix as NULL.
		if metadata.locatorByteLength == 0 && len(data) == 0 && len(metadata.lobLocator) == 0 {
			return r.handleNull(i, dtype, scale), nil
		}
		if len(metadata.lobLocator) == 0 {
			return nil, common.NewOracleError(
				oracleErrors.InvalidLobSource,
				nil,
				"LOB locator",
			)
		}
		value, err := newStreamedLob(r, dtype, data, metadata)
		if err != nil {
			return nil, err
		}
		registered, retainErr := r.registerLob(value)
		if retainErr != nil {
			value.invalidate()
			return nil, retainErr
		}
		if !registered {
			value.invalidate()
			return nil, common.NewOracleError(oracleErrors.LobValueInvalidated, nil, "Rows decode")
		}
		return value, nil
	}
	// A non-NULL zero-length LOB has an empty prefetched payload but still
	// carries locator metadata. Decode it as an empty Go value; treating every
	// empty payload as NULL loses the database distinction.
	emptyNonNullLob := (dtype == DtyBlob || dtype == DtyClob) && colCtx.lobContext != nil && len(colCtx.lobContext.lobLocator) != 0
	// Handle Oracle SQL NULL (typically raw length zero is NULL).
	if len(data) == 0 && !emptyNonNullLob {
		return r.handleNull(i, dtype, scale), nil
	}

	decoder, err := r.shelf.GetCodecFactory().getDecoder(dtype)
	if err != nil || decoder == nil {
		// Preserve unknown types as raw bytes
		return data, nil
	}

	val, err := decoder.decodeToType(colCtx, data)
	if err != nil {
		// Preserve unknown types as raw bytes
		return nil, r.shelf.LocalizeError(err)
	}

	return val, nil
}

// registerLob records a newly decoded locator until it reaches EOF, is closed,
// or its owning Rows closes. It returns false if Rows already closed.
func (r *ttcRows) registerLob(value *streamedLob) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lifecycle._closed {
		return false, nil
	}
	if r.lifecycle._lobs == nil {
		r.lifecycle._lobs = make(map[*streamedLob]struct{})
	}
	if r.lifecycle._decodingLobs == nil {
		r.lifecycle._decodingLobs = make(map[*streamedLob]struct{})
	}
	r.lifecycle._lobs[value] = struct{}{}
	r.lifecycle._decodingLobs[value] = struct{}{}
	return true, nil
}

// releaseLob removes a completed, closed, or detached locator from Rows
// ownership. It no longer needs invalidation when Rows subsequently closes.
func (r *ttcRows) releaseLob(value *streamedLob) {
	r.mu.Lock()
	delete(r.lifecycle._lobs, value)
	delete(r.lifecycle._decodingLobs, value)
	r.mu.Unlock()
}

// takeLobsLocked detaches all outstanding locators. r.mu must be held.
func (r *ttcRows) takeLobsLocked() []*streamedLob {
	values := r.lobsSnapshotLocked()
	clear(r.lifecycle._lobs)
	clear(r.lifecycle._decodingLobs)
	return values
}

// lobsSnapshotLocked copies the outstanding locator registry without changing
// it. r.mu must be held.
func (r *ttcRows) lobsSnapshotLocked() []*streamedLob {
	values := make([]*streamedLob, 0, len(r.lifecycle._lobs))
	for value := range r.lifecycle._lobs {
		values = append(values, value)
	}
	return values
}

// invalidateCurrentRowLobs detaches and invalidates values created while
// decoding the current row. It is used for partial row-decode failures.
func (r *ttcRows) invalidateCurrentRowLobs() {
	r.mu.Lock()
	values := make([]*streamedLob, 0, len(r.lifecycle._decodingLobs))
	for value := range r.lifecycle._decodingLobs {
		values = append(values, value)
		delete(r.lifecycle._lobs, value)
	}
	clear(r.lifecycle._decodingLobs)
	r.mu.Unlock()
	for _, value := range values {
		value.invalidate()
	}
}

// beginLobOperation validates Rows, snapshots its context, and acquires the
// connection-wide TTC guard. It rechecks Rows after acquiring so Close can win
// while an operation is queued without allowing a later RPC.
func (r *ttcRows) beginLobOperation() (context.Context, func(), error) {
	r.mu.Lock()
	if r.lifecycle._closed {
		r.mu.Unlock()
		return nil, nil, common.NewOracleError(oracleErrors.LobValueInvalidated, nil, "Rows owner")
	}
	ctx := r.lifecycle._ctx
	shelf := r.shelf
	r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	release, err := shelf.synchronizer.begin(ctx)
	if err != nil {
		return nil, nil, err
	}
	r.mu.Lock()
	closed := r.lifecycle._closed
	r.mu.Unlock()
	if closed || ctx.Err() != nil {
		release()
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		return nil, nil, common.NewOracleError(oracleErrors.LobValueInvalidated, nil, "Rows setup")
	}
	operationContext, _, cleanup := shelf.cancellation.newCancelableOperationContext(ctx, shelf.cancelExecution)
	var once sync.Once
	complete := func() {
		once.Do(func() {
			cleanup()
			release()
		})
	}
	return operationContext, complete, nil
}

// isClosed reports whether the owning Rows has invalidated its child locators.
// It is safe to call while streamedLob.mu is held because Rows never holds r.mu
// while acquiring a streamedLob mutex.
func (r *ttcRows) isClosed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lifecycle._closed
}

// contextErr returns the query/Rows lifecycle error without exposing the
// mutable context field to locator-backed values.
func (r *ttcRows) contextErr() error {
	r.mu.Lock()
	ctx := r.lifecycle._ctx
	r.mu.Unlock()
	return ctx.Err()
}

// invalidateAfterUnsafeLobRPC closes Rows locally after a canceled, transport,
// or protocol-level locator exchange. It deliberately does not call into any
// streamedLob while an RPC caller may still hold that value's mutex. Posting the
// streamer-stale event marks the physical connection invalid, preventing
// database/sql from reusing a TTC stream whose terminal response was not
// proven to have been consumed.
func (r *ttcRows) invalidateAfterUnsafeLobRPC() {
	stmt, _, _, first := r.closeState(nil)
	if first {
		if stmt != nil {
			stmt.detachRows(r)
		}
	}
	if r.shelf != nil {
		r.shelf.getEventService().post(streamerStaleEvent)
	}
}

// invalidateAfterTemporaryCleanupFailure prevents reuse of a session that may
// still own a temporary locator. Even a terminal Oracle free error leaves the
// resource ownership unresolved, so physical-session teardown is the fallback.
// handleNull determines the driver.Value to surface for a NULL result-set column.
//
// Preconditions:
//   - Column nullability is checked by decodeColumnValue before this helper is invoked.
//
// Parameters:
//   - i: zero-based column index within the current row buffer.
//   - dtype: TTC datatype negotiated for the column.
//   - scale: TTC scale metadata used for numeric default evaluation.
//
// Returns:
//   - driver.Value representing the NULL substitution (for example 0, "", time.Time{}),
//     or nil when no specific default applies.
func (r *ttcRows) handleNull(i int, dtype DtyType, scale int8) driver.Value {
	if !r.strictNullHandlingValue {
		if val, ok := _defaultValueForNull(dtype, scale); ok {
			return val
		}
	}
	// Fallback to nil when the type is unrecognised; behaviour matches legacy default.
	return nil
}

// _defaultValueForNull returns the driver.Value substitution that should be used
// for a NULL column based on its negotiated TTC datatype metadata.
//
// Parameters:
//   - dtype: TTC datatype negotiated for the column being read.
//   - scale: numeric scale metadata used to refine NUMBER defaults.
//
// Returns:
//   - driver.Value providing the default representation for the NULL column.
//   - bool flag indicating whether a default was found. When false, callers
//     should surface a nil value.
//
// Errors:
//   - This helper does not return errors; defaults are mapped deterministically.
//
// Numeric defaults consider scale to decide between integer, floating-point, or
// decimal string representations.
func _defaultValueForNull(dtype DtyType, scale int8) (driver.Value, bool) {
	if resolver, ok := defaultNullValueResolvers[dtype]; ok {
		return resolver(scale)
	}

	return nil, false
}

// defaultNullValueResolver resolves the default substitution for a TTC datatype.
// Implementations may inspect the column scale (for numbers). The bool return
// indicates whether the resolver produced a value.
type defaultNullValueResolver func(scale int8) (driver.Value, bool)

// constantDefaultNullValue returns a resolver that always yields the provided
// driver.Value, regardless of scale metadata.
func constantDefaultNullValue(value driver.Value) defaultNullValueResolver {
	return func(int8) (driver.Value, bool) {
		return value, true
	}
}

// defaultNullValueResolvers defines the default substitution for TTC datatypes when
// strict null handling is disabled. Numeric types are handled separately because the
// default representation depends on scale metadata.
var defaultNullValueResolvers = map[DtyType]defaultNullValueResolver{
	DtyNum:      defaultNumericValue,
	DtyVnu:      defaultNumericValue,
	DtyInt:      defaultNumericValue,
	DtyPdn:      defaultNumericValue,
	DtyUin:      defaultNumericValue,
	DtySls:      defaultNumericValue,
	DtyIbFloat:  constantDefaultNullValue(float64(0)),
	DtyIbDouble: constantDefaultNullValue(float64(0)),
	DtyChr:      constantDefaultNullValue(""),
	DtyStr:      constantDefaultNullValue(""),
	DtyVCS:      constantDefaultNullValue(""),
	DtyAfc:      constantDefaultNullValue(""),
	DtyAvc:      constantDefaultNullValue(""),
	DtyBin:      constantDefaultNullValue(driverCommon.B1Array{}),
	DtyVbi:      constantDefaultNullValue(driverCommon.B1Array{}),
	DtyLbi:      constantDefaultNullValue(driverCommon.B1Array{}),
	DtyBlob:     constantDefaultNullValue(driverCommon.B1Array{}),
	DtyDblob:    constantDefaultNullValue(driverCommon.B1Array{}),
	DtyBol:      constantDefaultNullValue(false),
	DtyDat:      constantDefaultNullValue(time.Time{}),
	DtyEdate:    constantDefaultNullValue(time.Time{}),
	DtyStamp:    constantDefaultNullValue(time.Time{}),
	DtyEstamp:   constantDefaultNullValue(time.Time{}),
	DtyStz:      constantDefaultNullValue(time.Time{}),
	DtyEstz:     constantDefaultNullValue(time.Time{}),
	DtySitz:     constantDefaultNullValue(time.Time{}),
	DtyEsitz:    constantDefaultNullValue(time.Time{}),
	DtyTime:     constantDefaultNullValue(time.Time{}),
	DtyEtime:    constantDefaultNullValue(time.Time{}),
	DtyTtz:      constantDefaultNullValue(time.Time{}),
	DtyEttz:     constantDefaultNullValue(time.Time{}),
	DtyIym:      constantDefaultNullValue("00-00"),
	DtyEiym:     constantDefaultNullValue("00-00"),
	DtyIds:      constantDefaultNullValue("00 00:00:00.0"),
	DtyEids:     constantDefaultNullValue("00 00:00:00.0"),
}

// defaultNumericValue calculates the default driver.Value to surface for
// numeric TTC datatypes when the column value is NULL.
//
// Parameters:
//   - scale: TTC scale metadata used to discriminate between integer,
//     floating-point, and arbitrary-precision defaults.
//
// Returns:
//   - driver.Value representing the numeric default (int64, float64, or
//     decimal string).
//   - bool indicating whether the resolver produced a value. This allows the
//     method to be used directly as a defaultNullValueResolver implementation.
//
// Errors:
//   - This helper does not return errors; the mapping is deterministic.
func defaultNumericValue(scale int8) (driver.Value, bool) {
	switch scale {
	case 0:
		return int64(0), true
	case NumberScaleFloatSentinel:
		return float64(0), true
	default:
		return "0", true
	}
}

// Close implements driver.Rows.Close. It detaches reusable prepared statements
// and closes a Statement only when Connection.QueryContext transferred direct-
// query ownership to these Rows.
func (r *ttcRows) Close() error {
	common.Odl.Debug("closing rows")
	stmt, owned, _, first := r.closeState(nil)
	if !first {
		return nil
	}
	if stmt == nil {
		return nil
	}
	stmt.detachRows(r)
	if owned {
		return stmt.closeAfterRows()
	}
	return nil
}

// closeState atomically closes Rows, cancels queued/in-flight locator work,
// detaches statement ownership, and clears the outstanding LOB registry. It
// does not acquire any streamedLob mutex: owner.closed is the authoritative
// invalidation signal, which keeps Rows.Close from blocking behind a stalled
// network read. If expectedStatement is non-nil, the transition occurs only
// for that statement.
func (r *ttcRows) closeState(expectedStatement *Statement) (*Statement, bool, []*streamedLob, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lifecycle._closed || (expectedStatement != nil && r.lifecycle._statement != expectedStatement) {
		return nil, false, nil, false
	}
	r.lifecycle._closed = true
	if r.lifecycle._cancel != nil {
		r.lifecycle._cancel()
	}
	stmt := r.lifecycle._statement
	owned := r.lifecycle._ownsStatement
	values := r.takeLobsLocked()
	r.lifecycle._statement = nil
	r.lifecycle._ownsStatement = false
	// Escaped Lob values retain their small Rows owner to observe invalidation.
	// Drop buffered result payloads here so one closed Lob cannot retain the
	// complete query result in memory.
	r.rowData = nil
	r.lobColContext = nil
	return stmt, owned, values, true
}

// attachStatement records the prepared Statement which produced these Rows.
// The reference is non-owning until the direct Connection query path promotes
// it with takeStatementOwnership.
func (r *ttcRows) attachStatement(stmt *Statement) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.lifecycle._closed {
		r.lifecycle._statement = stmt
	}
}

// takeStatementOwnership transfers an internally-created direct-query
// Statement to Rows so database/sql keeps both cursor and connection alive
// until Rows.Close. It returns false if Rows was already closed or was not
// attached to stmt, allowing the connection path to close stmt rather than
// leak it.
func (r *ttcRows) takeStatementOwnership(stmt *Statement) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.lifecycle._closed && r.lifecycle._statement == stmt {
		r.lifecycle._ownsStatement = true
		return true
	}
	return false
}

// closeFromStatement marks Rows closed without calling back into Statement.
// Statement.Close uses this path to avoid a recursive close cycle.
func (r *ttcRows) closeFromStatement(stmt *Statement) error {
	_, _, _, first := r.closeState(stmt)
	if !first {
		return nil
	}
	return nil
}

// newTTCRows constructs a ttcRows instance from decoded column metadata.
// It precomputes and caches fields backing the RowsColumnType* interfaces
// for fast access during scanning and introspection.
func newTTCRows(columnContexts []columnContext) *ttcRows {
	n := len(columnContexts)
	rows := &ttcRows{
		strictNullHandlingValue: true,
		lifecycle: rowsLifecycle{
			_ctx:          context.Background(),
			_lobs:         make(map[*streamedLob]struct{}),
			_decodingLobs: make(map[*streamedLob]struct{}),
		},
	}
	if n == 0 {
		return rows
	}

	rows.columnContexts = make([]columnContext, n)
	for i := 0; i < n; i++ {
		rows.columnContexts[i] = columnContexts[i]
	}
	rows.lobColContext = make([][]*lobColumnContext, 0)

	return rows
}

// ColumnTypeDatabaseTypeName implements RowsColumnTypeDatabaseTypeName.
// It returns the database-specific type name (e.g., VARCHAR2, NUMBER).
func (r *ttcRows) ColumnTypeDatabaseTypeName(index int) string {
	// inline translation here waiting for refactor of our type registry
	switch r.columnContexts[index].DataType {
	case DtyChr:
		if r.columnContexts[index].CharsetForm == 2 {
			return "NVARCHAR2"
		}
		return "VARCHAR2"
	case DtyNum, DtyVnu:
		if r.columnContexts[index].Precision != 0 && r.columnContexts[index].Precision == -127 {
			return "FLOAT"
		}
		return "NUMBER"
	case DtyLng:
		return "LONG"
	case DtyDat:
		return "DATE"
	case DtyBin:
		return "RAW"
	case DtyLbi:
		return "LONG RAW"
	case DtyAfc:
		if r.columnContexts[index].CharsetForm == 2 {
			return "NCHAR"
		}
		return "CHAR"
	case DtyIbFloat:
		return "BINARY_FLOAT"
	case DtyIbDouble:
		return "BINARY_DOUBLE"
	case DtyCur:
		return "REFCURSOR"
	case DtyRdd, DtyBuri:
		return "ROWID"
	case DtyINty:
		return "Internal Named Type" // enough for now
	case DtyIref:
		return "Internal Named Type" // enough for now
	case DtyClob:
		if r.columnContexts[index].CharsetForm == 2 {
			return "NCLOB"
		}
		return "CLOB"
	case DtyBlob:
		return "BLOB"
	case DtyBFil:
		return "BFILE"
	case DtyJSON:
		return "JSON"
	case DtyVec:
		return "VECTOR"
	case DtyStamp:
		return "TIMESTAMP"
	case DtyStz:
		return "TIMESTAMP WITH TIME ZONE"
	case DtyIym:
		return "INTERVALYM"
	case DtyIds:
		return "INTERVALDS"
	case DtySitz:
		return "TIMESTAMP WITH LOCAL TIME ZONE"
	case DtyBol:
		return "BOOLEAN"
	default:
		common.Odl.Warn("Do not have name mapping", "type", r.columnContexts[index].DataType)
		return ""
	}
}

// ColumnTypeLength implements RowsColumnTypeLength. It returns the byte length
// for variable-length types when available.
func (r *ttcRows) ColumnTypeLength(index int) (int64, bool) {
	if index < 0 || index >= len(r.columnContexts) {
		return 0, false
	}
	if r.columnContexts[index].Length <= 0 {
		return 0, false
	}
	return r.columnContexts[index].Length, true
}

// ColumnTypeNullable implements RowsColumnTypeNullable. It returns whether the
// column may be NULL and whether the information is available.
func (r *ttcRows) ColumnTypeNullable(index int) (bool, bool) {
	return r.columnContexts[index].Nullable, true
}

// ColumnTypePrecisionScale implements RowsColumnTypePrecisionScale. It should return
// the precision and scale for decimal types. If not applicable, ok should be false.
func (r *ttcRows) ColumnTypePrecisionScale(index int) (int64, int64, bool) {
	dty := r.columnContexts[index].DataType
	if dty == DtyNum || dty == DtyVnu {
		return r.columnContexts[index].Precision, int64(r.columnContexts[index].Scale), true
	}

	return 0, 0, false
}

// ColumnTypeScanType implements RowsColumnTypeScanType. It returns the Go type
// into which database values will be scanned. Currently, []byte is used for all
// columns, matching the raw protocol representation returned by Next.
func (r *ttcRows) ColumnTypeScanType(index int) reflect.Type {
	dtype := r.columnContexts[index].DataType
	if dtype == DtyBlob || dtype == DtyClob {
		// database/sql cannot name the public lob.LOB type from this private
		// transport package. any accurately describes the driver.Value source and
		// lets sql.Scanner perform the ownership transfer.
		return reflect.TypeFor[any]()
	}
	if r.columnContexts[index].ScanType == nil {
		decoder, err := r.shelf.GetCodecFactory().getDecoder(dtype)
		if err != nil {
			common.Odl.Warn("Do not have decode mapping", "type", r.columnContexts[index].DataType)
			return reflect.TypeOf([]byte(nil))
		}
		r.columnContexts[index].ScanType = new(decoder.getScanType(r.columnContexts[index]))
	}
	return *r.columnContexts[index].ScanType
}

// ttcResult implements database/sql/driver.Result, used for DML or exec results.
type ttcResult struct {
	rowsAffected int64
	shelf        *ttiShelf[driverCommon.MessageType]
}

// RowsAffected returns the number of rows affected by the last exec.
func (r *ttcResult) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

// LastInsertId reports that Oracle does not support retrieving a last insert ID
// through this driver path and returns a shelf-localized error.
func (r *ttcResult) LastInsertId() (int64, error) {
	return 0, r.shelf.LocalizeError(common.NewOracleError(oracleErrors.UnsupportedFeature, nil, "LastInsertId"))
}
