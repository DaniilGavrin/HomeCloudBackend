package database

import (
	"database/sql"
	"context"
	"database/sql/driver"
	"strconv"
	"strings"

	"github.com/lib/pq"
)

type rebindDriver struct {
	driver driver.Driver
}

func (d *rebindDriver) Open(name string) (driver.Conn, error) {
	conn, err := d.driver.Open(name)
	if err != nil {
		return nil, err
	}
	return &rebindConn{Conn: conn}, nil
}

type rebindConn struct {
	driver.Conn
}

func (c *rebindConn) Prepare(query string) (driver.Stmt, error) {
	stmt, err := c.Conn.Prepare(rebindPostgres(query))
	if err != nil {
		return nil, err
	}
	return stmt, nil
}

func (c *rebindConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	query = rebindPostgres(query)
	if conn, ok := c.Conn.(driver.ConnPrepareContext); ok {
		return conn.PrepareContext(ctx, query)
	}
	return c.Conn.Prepare(query)
}

func (c *rebindConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	execer, ok := c.Conn.(driver.ExecerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	return execer.ExecContext(ctx, rebindPostgres(query), args)
}

func (c *rebindConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	queryer, ok := c.Conn.(driver.QueryerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	return queryer.QueryContext(ctx, rebindPostgres(query), args)
}

func (c *rebindConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if conn, ok := c.Conn.(driver.ConnBeginTx); ok {
		return conn.BeginTx(ctx, opts)
	}
	if opts.ReadOnly {
		return nil, driver.ErrSkip
	}
	return c.Conn.Begin()
}

func (c *rebindConn) Ping(ctx context.Context) error {
	if pinger, ok := c.Conn.(driver.Pinger); ok {
		return pinger.Ping(ctx)
	}
	return nil
}

func (c *rebindConn) CheckNamedValue(value *driver.NamedValue) error {
	if checker, ok := c.Conn.(driver.NamedValueChecker); ok {
		return checker.CheckNamedValue(value)
	}
	return driver.ErrSkip
}

func init() {
	sql.Register("postgres-rebind", &rebindDriver{driver: &pq.Driver{}})
}

// rebindPostgres converts positional question-mark parameters to PostgreSQL
// numbered parameters. It deliberately ignores quoted strings, quoted
// identifiers, comments, and dollar-quoted function bodies.
func rebindPostgres(query string) string {
	var out strings.Builder
	out.Grow(len(query) + 16)
	parameter := 0

	for i := 0; i < len(query); {
		switch {
		case query[i] == '\'':
			start := i
			i++
			for i < len(query) {
				if query[i] == '\\' && i+1 < len(query) {
					i += 2
					continue
				}
				if query[i] == '\'' {
					i++
					if i < len(query) && query[i] == '\'' {
						i++
						continue
					}
					break
				}
				i++
			}
			out.WriteString(query[start:i])

		case query[i] == '"':
			start := i
			i++
			for i < len(query) {
				if query[i] == '"' {
					i++
					if i < len(query) && query[i] == '"' {
						i++
						continue
					}
					break
				}
				i++
			}
			out.WriteString(query[start:i])

		case query[i] == '-' && i+1 < len(query) && query[i+1] == '-':
			start := i
			for i < len(query) && query[i] != '\n' {
				i++
			}
			out.WriteString(query[start:i])

		case query[i] == '/' && i+1 < len(query) && query[i+1] == '*':
			start := i
			i += 2
			depth := 1
			for i < len(query) && depth > 0 {
				switch {
				case i+1 < len(query) && query[i] == '/' && query[i+1] == '*':
					depth++
					i += 2
				case i+1 < len(query) && query[i] == '*' && query[i+1] == '/':
					depth--
					i += 2
				default:
					i++
				}
			}
			out.WriteString(query[start:i])

		case query[i] == '$':
			if delimiter := postgresDollarDelimiter(query[i:]); delimiter != "" {
				start := i
				i += len(delimiter)
				if end := strings.Index(query[i:], delimiter); end >= 0 {
					i += end + len(delimiter)
				} else {
					i = len(query)
				}
				out.WriteString(query[start:i])
			} else {
				out.WriteByte(query[i])
				i++
			}

		case query[i] == '?' && i+1 < len(query) && (query[i+1] == '|' || query[i+1] == '&'):
			out.WriteByte(query[i])
			i++

		case query[i] == '?' && i+1 < len(query) && query[i+1] == '?':
			out.WriteByte('?')
			i += 2

		case query[i] == '?':
			parameter++
			out.WriteByte('$')
			out.WriteString(strconv.Itoa(parameter))
			i++

		default:
			out.WriteByte(query[i])
			i++
		}
	}
	return out.String()
}

func postgresDollarDelimiter(s string) string {
	if len(s) < 2 || s[0] != '$' {
		return ""
	}
	for i := 1; i < len(s); i++ {
		ch := s[i]
		if ch == '$' {
			return s[:i+1]
		}
		if !((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
			(ch >= '0' && ch <= '9' && i > 1) || ch == '_') {
			return ""
		}
	}
	return ""
}
