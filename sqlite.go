package sqlite

import (
	"context"
	"errors"
	"github.com/whosonfirst/go-whosonfirst-iterate/emitter"
	"github.com/whosonfirst/go-whosonfirst-iterate/filters"
	"github.com/whosonfirst/go-ioutil"
	"github.com/whosonfirst/go-whosonfirst-sqlite/database"
	"github.com/whosonfirst/go-whosonfirst-sqlite/utils"
	"net/url"
	"runtime"
	"strconv"
	"strings"
)

func init() {
	ctx := context.Background()
	emitter.RegisterEmitter(ctx, "sqlite", NewSQLiteEmitter)
}

type SQLiteEmitter struct {
	emitter.Emitter
	filters  filters.Filters
	throttle chan bool
}

func NewSQLiteEmitter(ctx context.Context, uri string) (emitter.Emitter, error) {

	u, err := url.Parse(uri)

	if err != nil {
		return nil, err
	}

	q := u.Query()

	max_procs := runtime.NumCPU()

	if q.Get("processes") != "" {

		procs, err := strconv.ParseInt(q.Get("processes"), 10, 64)

		if err != nil {
			return nil, err
		}

		max_procs = int(procs)
	}

	throttle_ch := make(chan bool, max_procs)

	for i := 0; i < max_procs; i++ {
		throttle_ch <- true
	}

	f, err := filters.NewQueryFiltersFromQuery(ctx, q)

	if err != nil {
		return nil, err
	}

	em := &SQLiteEmitter{
		filters:  f,
		throttle: throttle_ch,
	}

	return em, nil
}

func (d *SQLiteEmitter) WalkURI(ctx context.Context, emitter_cb emitter.EmitterCallbackFunc, uri string) error {

	db, err := database.NewDB(uri)

	if err != nil {
		return err
	}

	defer db.Close()

	conn, err := db.Conn()

	if err != nil {
		return err
	}

	has_table, err := utils.HasTable(db, "geojson")

	if err != nil {
		return err
	}

	if !has_table {
		return errors.New("database is missing a geojson table")
	}

	rows, err := conn.Query("SELECT id, body FROM geojson")

	if err != nil {
		return err
	}

	// https://github.com/whosonfirst/go-whosonfirst-index/issues/5

	sqlite_ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	error_ch := make(chan error)

	for rows.Next() {

		<-d.throttle

		var wofid int64
		var body string

		err := rows.Scan(&wofid, &body)

		if err != nil {
			return err
		}

		go func(ctx context.Context, wofid int64, body string) {

			defer func() {
				d.throttle <- true
			}()

			select {
			case <-ctx.Done():
				return
			default:
				// pass
			}

			// uri := fmt.Sprintf("sqlite://%s#geojson:%d", path, wofid)

			// see the way we're passing in STDIN and not uri as the path?
			// that because we call ctx, err := ContextForPath(path) in the
			// process() method and since uri won't be there nothing will
			// get indexed - it's not ideal it's just what it is today...
			// (20171213/thisisaaronland)

			sr := strings.NewReader(body)

			fh, err := ioutil.NewReadSeekCloser(sr)

			if err != nil {
				error_ch <- err
				return
			}

			if d.filters != nil {

				ok, err := d.filters.Apply(ctx, fh)

				if err != nil {
					error_ch <- err
					return
				}

				if !ok {
					return
				}

				_, err = fh.Seek(0, 0)

				if err != nil {
					error_ch <- err
					return

				}
			}

			ctx = emitter.AssignPathContext(ctx, emitter.STDIN)
			err = emitter_cb(ctx, fh)

			if err != nil {
				error_ch <- err
			}

		}(sqlite_ctx, wofid, body)

		select {
		case e := <-error_ch:
			cancel()
			return e
		default:
			// pass
		}
	}

	err = rows.Err()

	if err != nil {
		return err
	}

	return nil
}
