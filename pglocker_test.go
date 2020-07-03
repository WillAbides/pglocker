package pglocker

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

var (
	_pgAddr   string
	setupOnce sync.Once
)

func pgAddr(t *testing.T) string {
	t.Helper()
	setupOnce.Do(func() {
		_pgAddr = os.Getenv("PG_ADDR")
		if _pgAddr != "" {
			return
		}
		out, err := exec.Command("docker-compose", "port", "postgres", "5432").Output()
		require.NoError(t, err)
		_pgAddr = strings.TrimSpace(string(out))
	})
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
		t.Parallel()
		lockName := t.Name()
		db := getDB(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		errs, err := Lock(ctx, db, lockName, WithPingInterval(10*time.Millisecond))
		require.NoError(t, err)
		require.NotNil(t, errs)
		time.Sleep(50 * time.Millisecond)
		cancel()
		require.NoError(t, <-errs)
	})

	t.Run("can't get the same lock twice", func(t *testing.T) {
		t.Parallel()
		lockName := t.Name()
		db := getDB(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_, err := Lock(ctx, db, lockName)
		require.NoError(t, err)
		errs, err := Lock(ctx, db, lockName)
		require.Error(t, err)
		require.Nil(t, errs)
	})

	t.Run("waits for lock", func(t *testing.T) {
		t.Parallel()
		lockName := t.Name()
		db := getDB(t)
		ctx1, cancel1 := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel1()
		errs1, err := Lock(ctx1, db, lockName)
		require.NoError(t, err)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			require.NoError(t, <-errs1)
			wg.Done()
		}()
		ctx2, cancel2 := context.WithCancel(context.Background())
		errs2, err := Lock(ctx2, db, lockName, WithTimeout(time.Second))
		require.NoError(t, err)
		cancel2()
		require.NoError(t, <-errs2)
		wg.Wait()
	})

	t.Run("times out waiting for lock", func(t *testing.T) {
		t.Parallel()
		lockName := t.Name()
		db := getDB(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_, err := Lock(ctx, db, lockName)
		require.NoError(t, err)
		timeout := time.Millisecond * 30
		startTime := time.Now()
		errs, err := Lock(ctx, db, lockName, WithTimeout(timeout))
		delta := time.Since(startTime)
		require.Error(t, err)
		require.Nil(t, errs)
		require.Greater(t, int64(delta), int64(timeout))
	})

	t.Run("release and relock", func(t *testing.T) {
		t.Parallel()
		lockName := t.Name()
		db := getDB(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		errs, err := Lock(ctx, db, lockName)
		require.NoError(t, err)
		require.NotNil(t, errs)
		cancel()
		require.NoError(t, <-errs)
		ctx2, cancel2 := context.WithCancel(context.Background())
		defer cancel2()
		errs, err = Lock(ctx2, db, lockName)
		require.NoError(t, err)
		require.NotNil(t, errs)
		cancel2()
		require.NoError(t, <-errs)
	})
}
