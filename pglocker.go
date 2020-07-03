package pglocker

import (
	"context"
	"database/sql"
	"fmt"
	"hash/crc32"
	"time"
)

const defaultPingInterval = 10 * time.Second

type lockOpts struct {
	timeout      time.Duration
	pingInterval time.Duration
}

// LockOption is an optional value for Lock
type LockOption func(*lockOpts)

// WithTimeout sets a timeout for Lock to wait before giving up on getting a lock.
// When unset, Lock will error out immediately if the lock is unavailable.
func WithTimeout(timeout time.Duration) LockOption {
	return func(o *lockOpts) {
		o.timeout = timeout
	}
}

// WithPingInterval sets the interval for Lock to ping the connection. Default is 10 seconds.
func WithPingInterval(pingInterval time.Duration) LockOption {
	return func(o *lockOpts) {
		o.pingInterval = pingInterval
	}
}

// Lock gets an advisory lock from postgres and holds it until ctx is canceled.
// It pings the db connection at a regular interval to keep it from timing out.
// If the lock is unavailable and "WithTimeout" is set, it will continue trying until it either times out or obtains a lock.
// Returns an error channel that will receive an error when the lock is released.
func Lock(ctx context.Context, db *sql.DB, lockName string, options ...LockOption) (<-chan error, error) {
	opts := &lockOpts{
		pingInterval: defaultPingInterval,
	}
	for _, o := range options {
		o(opts)
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}

	ok, err := getLock(ctx, conn, lockName, opts.timeout)
	if err != nil || !ok {
		_ = conn.Close() //nolint:errcheck
		err = fmt.Errorf("could not obtain lock: %v", err)
		return nil, err
	}
	errs := make(chan error, 1)

	go func() {
		defer close(errs)
		ticker := time.NewTicker(opts.pingInterval)
		defer ticker.Stop()
		var lErr error
		for lErr == nil {
			select {
			case <-ctx.Done():
				lErr = ctx.Err()
			case <-ticker.C:
				lErr = conn.PingContext(ctx)
			}
		}
		releaseErr := ignoreErr(releaseLock(conn, lockName))
		if releaseErr != nil {
			lErr = releaseErr
		}
		errs <- ignoreErr(lErr)
	}()

	return errs, nil
}

func lockID(lockName string) uint32 {
	return crc32.ChecksumIEEE([]byte(lockName))
}

var ignoreableErrs = []error{
	context.DeadlineExceeded,
	context.Canceled,
	sql.ErrConnDone,
}

// ignoreErr returns err unless it is one of ignoreableErrs
func ignoreErr(err error) error {
	for _, ignoreMe := range ignoreableErrs {
		if err == ignoreMe {
			return nil
		}
	}
	return err
}

func getLock(ctx context.Context, conn *sql.Conn, lockName string, timeout time.Duration) (bool, error) {
	if timeout == 0 {
		return tryLock(ctx, conn, lockName)
	}
	return waitForLock(ctx, conn, lockName, timeout)
}

func tryLock(ctx context.Context, conn *sql.Conn, lockName string) (bool, error) {
	var ok bool
	err := conn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", lockID(lockName)).Scan(&ok)
	if err != nil {
		return false, err
	}
	return ok, nil
}

func waitForLock(ctx context.Context, conn *sql.Conn, lockName string, timeout time.Duration) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	_, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock($1)", lockID(lockName))
	if err != nil {
		return false, err
	}
	return true, nil
}

func releaseLock(conn *sql.Conn, lockName string) error {
	// use our own context so we can attempt to release a lock even after the calling function's context has been closed
	ctx := context.Background()
	_, err := conn.ExecContext(ctx, "SELECT pg_advisory_unlock($1)", lockID(lockName))
	return err
}
