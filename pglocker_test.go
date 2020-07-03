package pglocker

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func TestNothing(t *testing.T) {
	require.True(t, true)
}

var _pgAddr string

func pgAddr(t *testing.T) string {
	t.Helper()
	addr := os.Getenv("PG_ADDR")
	if addr != "" {
		return addr
	}
	out, err := exec.Command("docker-compose", "port", "postgres", "5432").Output()
	require.NoError(t, err)
	_pgAddr = strings.TrimSpace(string(out))
	return _pgAddr
}

func getDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := fmt.Sprintf("postgres://pglocker:pglocker@%s/pglocker?sslmode=disable", pgAddr(t))
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for ctx.Err() == nil {
		err = db.Ping()
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.NoError(t, err, "timed out waiting for connection")
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})
	return db
}

func TestLock(t *testing.T) {
	t.Run("locks", func(t *testing.T) {
		lockName := "lock"
		db := getDB(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		errs, err := Lock(ctx, db, lockName, time.Second)
		require.NoError(t, err)
		cancel()
		require.NoError(t, <-errs)
	})

	t.Run("can't get the same lock twice", func(t *testing.T) {
		lockName := "doublelock"
		db := getDB(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_, err := Lock(ctx, db, lockName, time.Second)
		require.NoError(t, err)
		errs, err := Lock(ctx, db, lockName, time.Millisecond)
		require.Error(t, err)
		require.Nil(t, errs)
	})

	t.Run("release and relock", func(t *testing.T) {
		lockName := "releaseandrelock"
		db := getDB(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		errs, err := Lock(ctx, db, lockName, time.Second)
		require.NoError(t, err)
		cancel()
		require.NoError(t, <-errs)
		ctx2, cancel2 := context.WithCancel(context.Background())
		defer cancel2()
		errs, err = Lock(ctx2, db, lockName, time.Second)
		require.NoError(t, err)
		cancel2()
		require.NoError(t, <-errs)
	})
}
