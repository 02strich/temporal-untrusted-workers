package main

import (
	"slices"
	"testing"
)

func TestAnalyze(t *testing.T) {
	for _, tc := range []struct {
		query         string
		reads, writes []string
	}{
		// SELECT
		{"SELECT * FROM customers", []string{"customers"}, nil},
		{`SELECT c.name, o.total FROM customers c JOIN orders o ON o.customer_id = c.id
		  WHERE o.id IN (SELECT order_id FROM refunds)`, []string{"customers", "orders", "refunds"}, nil},
		{"SELECT * FROM (SELECT * FROM orders) AS t", []string{"orders"}, nil},
		{"SELECT id FROM customers UNION SELECT id FROM suppliers", []string{"customers", "suppliers"}, nil},
		{"SELECT 1", nil, nil},
		{"SELECT week, signups FROM metrics WHERE week >= '2026-10-05' ORDER BY week", []string{"metrics"}, nil},
		{`SELECT c.company, sum(m.signups) AS signups FROM metrics m JOIN customers c ON c.id = m.customer_id
		  GROUP BY c.company ORDER BY signups DESC LIMIT 3`, []string{"customers", "metrics"}, nil},
		// CTEs
		{"WITH recent AS (SELECT * FROM orders) SELECT * FROM recent", []string{"orders"}, nil},
		{"WITH orders AS (SELECT * FROM secrets) SELECT * FROM orders", []string{"secrets"}, nil},
		{"WITH orders AS (SELECT 1) SELECT * FROM orders, public.orders", []string{"public.orders"}, nil},
		// writes
		{"INSERT INTO orders (id, total) VALUES (1, 2)", nil, []string{"orders"}},
		{"INSERT INTO archive SELECT * FROM orders WHERE ts < '2020-01-01'", []string{"orders"}, []string{"archive"}},
		{"UPDATE orders SET status = 'paid' WHERE id = 7", nil, []string{"orders"}},
		{"UPDATE orders SET total = (SELECT max(total) FROM orders) WHERE id = 1", []string{"orders"}, []string{"orders"}},
		{"UPDATE orders o JOIN prices p ON o.sku = p.sku SET o.total = p.total", nil, []string{"orders", "prices"}},
		{"DELETE FROM sessions WHERE expired", nil, []string{"sessions"}},
		{"DELETE FROM orders WHERE customer_id IN (SELECT id FROM banned)", []string{"banned"}, []string{"orders"}},
		{"DELETE o FROM orders o JOIN customers c ON o.cid = c.id", []string{"customers"}, []string{"orders"}},
		{"DELETE orders, c FROM orders JOIN customers c ON orders.cid = c.id", nil, []string{"customers", "orders"}},
		// several statements
		{"SELECT * FROM customers; UPDATE orders SET x = 1", []string{"customers"}, []string{"orders"}},
		// names
		{`SELECT * FROM Orders, "Customers", Public.Items`, []string{"customers", "orders", "public.items"}, nil},
	} {
		access, err := Analyze(tc.query)
		if err != nil {
			t.Errorf("%s: %v", tc.query, err)
			continue
		}
		if got := access.ReadList(); !slices.Equal(got, nonNil(tc.reads)) {
			t.Errorf("%s: reads %v, want %v", tc.query, got, tc.reads)
		}
		if got := access.WriteList(); !slices.Equal(got, nonNil(tc.writes)) {
			t.Errorf("%s: writes %v, want %v", tc.query, got, tc.writes)
		}
	}
}

func TestAnalyzeRejects(t *testing.T) {
	for _, query := range []string{
		"DROP TABLE orders",
		"TRUNCATE orders",
		"CREATE TABLE t (id INT)",
		"ALTER TABLE orders ADD COLUMN x INT",
		"GRANT SELECT ON orders TO bob",
		"SELECT * INTO OUTFILE '/tmp/x' FROM customers",
		"SELECT 1; DROP TABLE orders",
		"SELEC * FROM orders",
		"",
		"-- nothing",
	} {
		if access, err := Analyze(query); err == nil {
			t.Errorf("%q: want an error, got %+v", query, access)
		}
	}
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}
