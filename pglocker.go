package pglocker

import (
	"context"
	"database/sql"
	"hash/crc32"
	"time"
)

// Lock uses postgres to get a distributed lock and holds the lock until ctx is canceled. Returns an err channel that
// closes when the lock is released or receives an error indicating the lock could not be released.
// getLockTimeout is the amount of time to wait for the lock to become available before giving up.
func Lock(ctx context.Context, db *sql.DB, lockName string, getLockTimeout time.Duration) (<-chan error, error) {
	lockID := crc32.ChecksumIEEE([]byte(lockName))
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	err = getLock(ctx, conn, lockID, getLockTimeout)
	if err != nil {
		return nil, err
	}
	errs := make(chan error)
	go func() {
		<-ctx.Done()
		errs <- releaseLock(conn, lockID)
		close(errs)
	}()
	return errs, nil
}

func getLock(ctx context.Context, conn *sql.Conn, lockID uint32, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	_, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock($1)", lockID)
	return err
}

func releaseLock(conn *sql.Conn, lockID uint32) error {
	// use our own context so we can attempt to release a lock even after the calling function's context has been closed
	ctx := context.Background()
	_, err := conn.ExecContext(ctx, "SELECT pg_advisory_unlock($1)", lockID)
	return err
}
