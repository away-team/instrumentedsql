package instrumentedsql

import (
	"context"
	"database/sql/driver"

	svcDB "github.com/healthimation/go-service/database"
	"github.com/pkg/errors"
)

type wrappedDriver struct {
	logger       Logger
	instrumenter svcDB.DBInstrumentTimer
	parent       driver.Driver
}

type wrappedConn struct {
	logger       Logger
	instrumenter svcDB.DBInstrumentTimer
	parent       driver.Conn
}

type wrappedTx struct {
	logger       Logger
	instrumenter svcDB.DBInstrumentTimer
	ctx          context.Context
	parent       driver.Tx
}

type wrappedStmt struct {
	logger       Logger
	instrumenter svcDB.DBInstrumentTimer
	ctx          context.Context
	query        string
	parent       driver.Stmt
}

type wrappedResult struct {
	logger       Logger
	instrumenter svcDB.DBInstrumentTimer
	ctx          context.Context
	parent       driver.Result
}

type wrappedRows struct {
	logger       Logger
	instrumenter svcDB.DBInstrumentTimer
	ctx          context.Context
	parent       driver.Rows
}

// WrapDriver will wrap the passed SQL driver and return a new sql driver that uses it and also logs and traces calls using the passed logger and tracer
// The returned driver will still have to be registered with the sql package before it can be used.
//
// Important note: Seeing as the context passed into the various instrumentation calls this package calls,
// Any call without a context passed will not be instrumented. Please be sure to use the ___Context() and BeginTx() function calls added in Go 1.8
// instead of the older calls which do not accept a context.
func WrapDriver(driver driver.Driver, opts ...Opt) driver.Driver {
	d := wrappedDriver{parent: driver}

	for _, opt := range opts {
		opt(&d)
	}

	if d.logger == nil {
		d.logger = nullLogger{}
	}
	if d.instrumenter == nil {
		d.instrumenter = nullInstrumenter{}
	}

	return d
}

func (d wrappedDriver) Open(name string) (driver.Conn, error) {
	conn, err := d.parent.Open(name)
	if err != nil {
		return nil, err
	}

	return wrappedConn{instrumenter: d.instrumenter, logger: d.logger, parent: conn}, nil
}

func (c wrappedConn) Prepare(query string) (driver.Stmt, error) {
	parent, err := c.parent.Prepare(query)
	if err != nil {
		return nil, err
	}

	return wrappedStmt{instrumenter: c.instrumenter, logger: c.logger, query: query, parent: parent}, nil
}

func (c wrappedConn) Close() error {
	return c.parent.Close()
}

func (c wrappedConn) Begin() (driver.Tx, error) {
	tx, err := c.parent.Begin()
	if err != nil {
		return nil, err
	}

	return wrappedTx{instrumenter: c.instrumenter, logger: c.logger, parent: tx}, nil
}

func (c wrappedConn) BeginTx(ctx context.Context, opts driver.TxOptions) (tx driver.Tx, err error) {
	timer := svcDB.StartDBTimer(ctx, "", "sql-tx-begin", "")
	defer func() {
		timer.End()
	}()

	if connBeginTx, ok := c.parent.(driver.ConnBeginTx); ok {
		tx, err = connBeginTx.BeginTx(ctx, opts)
		if err != nil {
			return nil, err
		}

		return wrappedTx{instrumenter: c.instrumenter, logger: c.logger, ctx: ctx, parent: tx}, nil
	}

	tx, err = c.parent.Begin()
	if err != nil {
		return nil, err
	}

	return wrappedTx{instrumenter: c.instrumenter, logger: c.logger, ctx: ctx, parent: tx}, nil
}

func (c wrappedConn) PrepareContext(ctx context.Context, query string) (stmt driver.Stmt, err error) {
	timer := svcDB.StartDBTimer(ctx, "", "sql-prepare", query)
	defer func() {
		timer.End()
	}()

	if connPrepareCtx, ok := c.parent.(driver.ConnPrepareContext); ok {
		stmt, err := connPrepareCtx.PrepareContext(ctx, query)
		if err != nil {
			return nil, err
		}

		return wrappedStmt{instrumenter: c.instrumenter, logger: c.logger, ctx: ctx, parent: stmt}, nil
	}

	return c.Prepare(query)
}

func (c wrappedConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	if execer, ok := c.parent.(driver.Execer); ok {
		res, err := execer.Exec(query, args)
		if err != nil {
			return nil, err
		}

		return wrappedResult{instrumenter: c.instrumenter, logger: c.logger, parent: res}, nil
	}

	return nil, driver.ErrSkip
}

func (c wrappedConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (r driver.Result, err error) {
	timer := svcDB.StartDBTimer(ctx, "", "sql-conn-exec", query)
	defer func() {
		timer.End()
	}()

	if execContext, ok := c.parent.(driver.ExecerContext); ok {
		res, err := execContext.ExecContext(ctx, query, args)
		if err != nil {
			return nil, err
		}

		return wrappedResult{instrumenter: c.instrumenter, logger: c.logger, ctx: ctx, parent: res}, nil
	}

	// Fallback implementation
	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}

	select {
	default:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	return c.Exec(query, dargs)
}

func (c wrappedConn) Ping(ctx context.Context) (err error) {
	if pinger, ok := c.parent.(driver.Pinger); ok {
		timer := svcDB.StartDBTimer(ctx, "", "sql-ping", "")
		defer func() {
			timer.End()
		}()

		return pinger.Ping(ctx)
	}

	return nil
}

func (c wrappedConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	if queryer, ok := c.parent.(driver.Queryer); ok {
		rows, err := queryer.Query(query, args)
		if err != nil {
			return nil, err
		}

		return wrappedRows{instrumenter: c.instrumenter, logger: c.logger, parent: rows}, nil
	}

	return nil, driver.ErrSkip
}

func (c wrappedConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (rows driver.Rows, err error) {
	timer := svcDB.StartDBTimer(ctx, "", "sql-conn-query", query)
	defer func() {
		timer.End()
	}()

	if queryerContext, ok := c.parent.(driver.QueryerContext); ok {
		rows, err := queryerContext.QueryContext(ctx, query, args)
		if err != nil {
			return nil, err
		}

		return wrappedRows{instrumenter: c.instrumenter, logger: c.logger, ctx: ctx, parent: rows}, nil
	}

	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}

	select {
	default:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	return c.Query(query, dargs)
}

func (t wrappedTx) Commit() (err error) {
	timer := svcDB.StartDBTimer(t.ctx, "", "sql-tx-commit", "")
	defer func() {
		timer.End()
	}()

	return t.parent.Commit()
}

func (t wrappedTx) Rollback() (err error) {
	timer := svcDB.StartDBTimer(t.ctx, "", "sql-tx-rollback", "")
	defer func() {
		timer.End()
	}()

	return t.parent.Rollback()
}

func (s wrappedStmt) Close() (err error) {
	timer := svcDB.StartDBTimer(s.ctx, "", "sql-stmt-close", "")
	defer func() {
		timer.End()
	}()

	return s.parent.Close()
}

func (s wrappedStmt) NumInput() int {
	return s.parent.NumInput()
}

func (s wrappedStmt) Exec(args []driver.Value) (res driver.Result, err error) {
	timer := svcDB.StartDBTimer(s.ctx, "", "sql-stmt-exec", "")
	defer func() {
		timer.End()
	}()

	res, err = s.parent.Exec(args)
	if err != nil {
		return nil, err
	}

	return wrappedResult{instrumenter: s.instrumenter, logger: s.logger, ctx: s.ctx, parent: res}, nil
}

func (s wrappedStmt) Query(args []driver.Value) (rows driver.Rows, err error) {
	timer := svcDB.StartDBTimer(s.ctx, "", "sql-stmt-query", "")
	defer func() {
		timer.End()
	}()

	rows, err = s.parent.Query(args)
	if err != nil {
		return nil, err
	}

	return wrappedRows{instrumenter: s.instrumenter, logger: s.logger, ctx: s.ctx, parent: rows}, nil
}

func (s wrappedStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (res driver.Result, err error) {
	timer := svcDB.StartDBTimer(s.ctx, "", "sql-stmt-exec", "")
	defer func() {
		timer.End()
	}()

	if stmtExecContext, ok := s.parent.(driver.StmtExecContext); ok {
		res, err := stmtExecContext.ExecContext(ctx, args)
		if err != nil {
			return nil, err
		}

		return wrappedResult{instrumenter: s.instrumenter, logger: s.logger, ctx: ctx, parent: res}, nil
	}

	// Fallback implementation
	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}

	select {
	default:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	return s.Exec(dargs)
}

func (s wrappedStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (rows driver.Rows, err error) {
	timer := svcDB.StartDBTimer(s.ctx, "", "sql-stmt-query", "")
	defer func() {
		timer.End()
	}()

	if stmtQueryContext, ok := s.parent.(driver.StmtQueryContext); ok {
		rows, err := stmtQueryContext.QueryContext(ctx, args)
		if err != nil {
			return nil, err
		}

		return wrappedRows{instrumenter: s.instrumenter, logger: s.logger, ctx: ctx, parent: rows}, nil
	}

	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}

	select {
	default:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	return s.Query(dargs)
}

func (r wrappedResult) LastInsertId() (id int64, err error) {
	timer := svcDB.StartDBTimer(r.ctx, "", "sql-res-lastInsertId", "")
	defer func() {
		timer.End()
	}()

	return r.parent.LastInsertId()
}

func (r wrappedResult) RowsAffected() (num int64, err error) {
	timer := svcDB.StartDBTimer(r.ctx, "", "sql-res-rowsAffected", "")
	defer func() {
		timer.End()
	}()

	return r.parent.RowsAffected()
}

func (r wrappedRows) Columns() []string {
	return r.parent.Columns()
}

func (r wrappedRows) Close() error {
	return r.parent.Close()
}

func (r wrappedRows) Next(dest []driver.Value) (err error) {
	timer := svcDB.StartDBTimer(r.ctx, "", "sql-rows-next", "")
	defer func() {
		timer.End()
	}()

	return r.parent.Next(dest)
}

// namedValueToValue is a helper function copied from the database/sql package
func namedValueToValue(named []driver.NamedValue) ([]driver.Value, error) {
	dargs := make([]driver.Value, len(named))
	for n, param := range named {
		if len(param.Name) > 0 {
			return nil, errors.New("sql: driver does not support the use of Named Parameters")
		}
		dargs[n] = param.Value
	}
	return dargs, nil
}
