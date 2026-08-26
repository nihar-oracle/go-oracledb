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
	"testing"

	"github.com/oracle/go-oracledb/v26/internal/driver/common"
	internallob "github.com/oracle/go-oracledb/v26/internal/lob"
	oracleErrors "github.com/oracle/go-oracledb/v26/oracle/errors"
)

// TestLobManager_OwnsTemporaryLease verifies that temporary-locator ownership
// is recorded and released through the manager, not through the shelf.
func TestLobManager_OwnsTemporaryLease(t *testing.T) {
	t.Parallel()
	shelf := newShelf[common.MessageType]()
	manager := newLobManager(shelf, newTestSessionContext())
	loc := newReferenceCountedTempLocator(71)

	lease, err := manager.retainTemporary(loc)
	if err != nil {
		t.Fatalf("retainTemporary: %v", err)
	}
	if err := manager.releaseTemporary(lease); err != nil {
		t.Fatalf("releaseTemporary: %v", err)
	}
	if loc.isTemporaryLocator() {
		t.Fatal("released locator still appears temporary")
	}
	if got := pendingTemporaryLobCount(shelf); got != 1 {
		t.Fatalf("pending temporary entries = %d, want 1", got)
	}
}

// TestLobManager_RejectsUnsupportedKind verifies the shared dispatch boundary
// rejects unsupported kinds before it reaches a type-specific executor.
func TestLobManager_RejectsUnsupportedKind(t *testing.T) {
	t.Parallel()
	manager := newLobManager(newShelf[common.MessageType](), newTestSessionContext())
	_, err := manager.createTemporary(context.Background(), internallob.Kind(99))
	var coded oracleErrors.SQLError
	if err == nil || !errors.As(err, &coded) || coded.ErrorCode() != string(oracleErrors.InvalidLobSource) {
		t.Fatalf("createTemporary unsupported kind error = %v, want %s", err, oracleErrors.InvalidLobSource)
	}
}
