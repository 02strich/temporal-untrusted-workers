package main

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	// Registers the literal-value implementation the parser needs.
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
)

// Access is what a query reads and writes: table names, lowercased, as
// "schema.table" when qualified.
type Access struct {
	Reads  map[string]bool
	Writes map[string]bool
}

// ReadList and WriteList return the tables sorted, for logs and messages.
func (a Access) ReadList() []string  { return sortedKeys(a.Reads) }
func (a Access) WriteList() []string { return sortedKeys(a.Writes) }

// Analyze returns the tables query reads and writes. query may hold several
// statements; they are combined.
//
// Only SELECT (including UNION and friends), INSERT, UPDATE and DELETE are
// understood. Anything else - another statement, SELECT ... INTO, SQL that
// doesn't parse - is an error, so a caller can deny what it can't analyze.
//
// The target of an INSERT, UPDATE or DELETE is written, not read, unless the
// statement names it again (e.g. in a subquery). Every table of a
// multi-table UPDATE or DELETE counts as written. Every other table a
// statement names is read; names of common table expressions aren't tables.
//
// The parser speaks MySQL with ANSI_QUOTES ("x" is an identifier) and doesn't
// record how an identifier was quoted, so names are compared
// case-insensitively: "Customers" counts as customers.
func Analyze(query string) (Access, error) {
	p := parser.New()
	p.SetSQLMode(mysql.ModeANSIQuotes)
	stmts, _, err := p.Parse(query, "", "")
	if err != nil {
		return Access{}, fmt.Errorf("parsing SQL: %w", err)
	}
	if len(stmts) == 0 {
		return Access{}, errors.New("no SQL statement")
	}
	access := Access{Reads: map[string]bool{}, Writes: map[string]bool{}}
	for _, stmt := range stmts {
		switch stmt.(type) {
		case *ast.SelectStmt, *ast.SetOprStmt, *ast.InsertStmt, *ast.UpdateStmt, *ast.DeleteStmt:
		default:
			return Access{}, fmt.Errorf("unsupported SQL statement (only SELECT, INSERT, UPDATE and DELETE): %s", keyword(stmt.Text()))
		}
		c := &collector{ctes: map[string]bool{}}
		stmt.Accept(c)
		if c.err != nil {
			return Access{}, c.err
		}
		// Each target was visited as a table once; what remains was read.
		reads := c.tables
		for _, target := range c.targets {
			if i := slices.Index(reads, target); i >= 0 {
				reads = slices.Delete(reads, i, i+1)
			}
		}
		for _, table := range reads {
			// A single-part name may refer to a common table expression; a
			// qualified one never does.
			if strings.Contains(table, ".") || !c.ctes[table] {
				access.Reads[table] = true
			}
		}
		for _, target := range c.targets {
			access.Writes[target] = true
		}
	}
	return access, nil
}

// collector walks a statement: every table it names, once per occurrence;
// every table it writes; the names of its common table expressions.
type collector struct {
	tables  []string
	targets []string
	ctes    map[string]bool
	err     error
}

func (c *collector) Enter(n ast.Node) (ast.Node, bool) {
	if c.err != nil {
		return n, true
	}
	switch n := n.(type) {
	case *ast.DeleteTableList:
		// A multi-table DELETE's targets, already taken from the DeleteStmt;
		// the tables themselves are named in its join.
		return n, true
	case *ast.TableName:
		c.tables = append(c.tables, tableName(n))
	case *ast.WithClause:
		for _, cte := range n.CTEs {
			c.ctes[cte.Name.L] = true
		}
	case *ast.SelectStmt:
		if n.SelectIntoOpt != nil {
			c.err = errors.New("SELECT ... INTO is not supported")
		}
	case *ast.InsertStmt:
		c.addTargets(joinedTables(n.Table))
	case *ast.UpdateStmt:
		c.addTargets(joinedTables(n.TableRefs))
	case *ast.DeleteStmt:
		if n.IsMultiTable && n.Tables != nil {
			// DELETE o FROM orders o JOIN customers c ...: the targets name
			// tables of the join, possibly by alias.
			aliases := joinedAliases(n.TableRefs)
			for _, t := range n.Tables.Tables {
				name := tableName(t)
				if table, ok := aliases[name]; ok {
					name = table
				}
				c.targets = append(c.targets, name)
			}
		} else {
			c.addTargets(joinedTables(n.TableRefs))
		}
	}
	return n, c.err != nil
}

func (c *collector) Leave(n ast.Node) (ast.Node, bool) { return n, c.err == nil }

func (c *collector) addTargets(targets []string) {
	if len(targets) == 0 {
		c.err = errors.New("no target table")
		return
	}
	c.targets = append(c.targets, targets...)
}

// joinedTables returns the tables named directly in a FROM-like clause (not
// in subqueries).
func joinedTables(refs *ast.TableRefsClause) []string {
	if refs == nil {
		return nil
	}
	var names []string
	var walk func(ast.ResultSetNode)
	walk = func(node ast.ResultSetNode) {
		switch node := node.(type) {
		case *ast.Join:
			walk(node.Left)
			if node.Right != nil {
				walk(node.Right)
			}
		case *ast.TableSource:
			if t, ok := node.Source.(*ast.TableName); ok {
				names = append(names, tableName(t))
			}
		case *ast.TableName:
			names = append(names, tableName(node))
		}
	}
	walk(refs.TableRefs)
	return names
}

// joinedAliases maps the aliases (and names) of the tables in a FROM-like
// clause to the tables.
func joinedAliases(refs *ast.TableRefsClause) map[string]string {
	aliases := map[string]string{}
	if refs == nil {
		return aliases
	}
	var walk func(ast.ResultSetNode)
	walk = func(node ast.ResultSetNode) {
		switch node := node.(type) {
		case *ast.Join:
			walk(node.Left)
			if node.Right != nil {
				walk(node.Right)
			}
		case *ast.TableSource:
			if t, ok := node.Source.(*ast.TableName); ok {
				aliases[tableName(t)] = tableName(t)
				if node.AsName.L != "" {
					aliases[node.AsName.L] = tableName(t)
				}
			}
		}
	}
	walk(refs.TableRefs)
	return aliases
}

func tableName(t *ast.TableName) string {
	if t.Schema.L != "" {
		return t.Schema.L + "." + t.Name.L
	}
	return t.Name.L
}

// keyword returns a statement's leading keywords, for error messages.
func keyword(text string) string {
	fields := strings.Fields(text)
	if len(fields) > 2 {
		fields = fields[:2]
	}
	return strings.ToUpper(strings.Join(fields, " "))
}

func sortedKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}
