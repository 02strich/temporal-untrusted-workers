//! Which tables the SQL queries in a tool call's parameters read and write.
//!
//! An action in the action schema declares where its SQL is with an `@sql`
//! annotation: one or more comma-separated paths under `context.input`, each
//! naming a string or a list of strings, e.g. `@sql("arg0.query")` or
//! `@sql("arg0.queries, arg1.audit")`. The verifier parses every query and
//! gives the policies `context.sql = { reads: Set<String>, writes: Set<String> }`
//! for the whole call (see [`SqlAccess`]).
//!
//! Only `SELECT` (including `TABLE t`), `INSERT`, `UPDATE` and `DELETE` are
//! understood. Anything else - another statement, `SELECT ... INTO`, a table
//! function as an `INSERT` target, a query that doesn't parse, a path that
//! doesn't name a string - is an error, and the call gets no `context.sql`,
//! so every policy that requires it fails closed.
//!
//! Function calls are not analyzed: a policy only learns which tables a
//! query names, not what a function it calls does.

use std::collections::{BTreeSet, HashMap};
use std::ops::ControlFlow;

use anyhow::{Context as _, anyhow, bail};
use cedar_policy::{EntityNamespace, Schema, SchemaFragment};
use serde_json::Value;
use sqlparser::ast::{
    FromTable, ObjectName, ObjectNamePart, Query, Select, SetExpr, Statement, TableFactor,
    TableObject, TableWithJoins, Visit, Visitor,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// The action annotation naming an action's SQL parameters.
pub const SQL_ANNOTATION: &str = "sql";

/// The tables a call's SQL reads and writes.
///
/// A table is named as written: its identifiers joined with `.`, unquoted
/// ones lowercased, quoted ones verbatim (`Orders` and `orders` are
/// `orders`, `"Orders"` is `Orders`, `public.orders` is not `orders`).
///
/// The target of an `INSERT`, `UPDATE` or `DELETE` is written, not read,
/// unless the statement names it again (e.g. in a subquery). Every other
/// table a statement names is read; names of common table expressions are
/// not tables.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SqlAccess {
    pub reads: BTreeSet<String>,
    pub writes: BTreeSet<String>,
}

impl SqlAccess {
    /// Adds what `other` reads and writes.
    pub fn union(&mut self, other: SqlAccess) {
        self.reads.extend(other.reads);
        self.writes.extend(other.writes);
    }

    /// `{"reads": [...], "writes": [...]}`, the shape policies see (arrays
    /// become Cedar sets).
    pub fn to_json(&self) -> Value {
        serde_json::json!({"reads": self.reads, "writes": self.writes})
    }
}

/// The tables `sql` (one or more statements) reads and writes.
pub fn analyze(sql: &str) -> anyhow::Result<SqlAccess> {
    let statements = Parser::parse_sql(&GenericDialect {}, sql).context("parsing SQL")?;
    if statements.is_empty() {
        bail!("no SQL statement");
    }
    let mut access = SqlAccess::default();
    for statement in &statements {
        access.union(analyze_statement(statement)?);
    }
    Ok(access)
}

fn analyze_statement(statement: &Statement) -> anyhow::Result<SqlAccess> {
    let mut collector = Collector::default();
    if let ControlFlow::Break(error) = statement.visit(&mut collector) {
        return Err(error);
    }
    let Collector {
        relations,
        targets,
        ctes,
    } = collector;

    // Each target was visited as a relation once; what remains was read.
    let mut reads = relations;
    for target in &targets {
        if let Some(i) = reads.iter().position(|r| r == target) {
            reads.remove(i);
        }
    }
    Ok(SqlAccess {
        // A single-part name may refer to a common table expression; a
        // qualified one never does.
        reads: reads
            .into_iter()
            .filter(|r| r.contains('.') || !ctes.contains(r))
            .collect(),
        writes: targets.into_iter().collect(),
    })
}

/// Walks a statement, including the statements nested in it (e.g. a
/// data-modifying `WITH` clause).
#[derive(Default)]
struct Collector {
    /// Every table named, once per occurrence.
    relations: Vec<String>,
    /// Every table written, once per statement writing it.
    targets: Vec<String>,
    /// The names of the common table expressions.
    ctes: BTreeSet<String>,
}

impl Visitor for Collector {
    type Break = anyhow::Error;

    fn pre_visit_statement(&mut self, statement: &Statement) -> ControlFlow<anyhow::Error> {
        let targets = match statement {
            Statement::Query(_) => Ok(Vec::new()),
            Statement::Insert(insert) => match &insert.table {
                TableObject::TableName(name) => table_name(name).map(|n| vec![n]),
                _ => Err(anyhow!("INSERT into a table function is not supported")),
            },
            // Every table of a (MySQL) multi-table UPDATE may be written.
            Statement::Update(update) => joined_tables(std::slice::from_ref(&update.table)),
            Statement::Delete(delete) => {
                if delete.tables.is_empty() {
                    let (FromTable::WithFromKeyword(from) | FromTable::WithoutKeyword(from)) =
                        &delete.from;
                    joined_tables(from)
                } else {
                    delete.tables.iter().map(table_name).collect()
                }
            }
            other => Err(anyhow!(
                "unsupported SQL statement (only SELECT, INSERT, UPDATE and DELETE): {}",
                statement_keyword(other)
            )),
        };
        match targets {
            Ok(targets) => {
                self.targets.extend(targets);
                ControlFlow::Continue(())
            }
            Err(error) => ControlFlow::Break(error),
        }
    }

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<anyhow::Error> {
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                self.ctes.insert(identifier(&cte.alias.name));
            }
        }
        // `TABLE t` names its table outside of a relation.
        let mut bodies = vec![query.body.as_ref()];
        while let Some(body) = bodies.pop() {
            match body {
                SetExpr::SetOperation { left, right, .. } => {
                    bodies.push(left);
                    bodies.push(right);
                }
                SetExpr::Table(table) => {
                    let Some(name) = &table.table_name else {
                        return ControlFlow::Break(anyhow!("TABLE without a table name"));
                    };
                    let mut parts: Vec<String> = table.schema_name.iter().cloned().collect();
                    parts.push(name.clone());
                    // Already unquoted by the parser: keep as is, lowercased
                    // like an unquoted identifier.
                    self.relations.push(parts.join(".").to_lowercase());
                }
                _ => {}
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_select(&mut self, select: &Select) -> ControlFlow<anyhow::Error> {
        if select.into.is_some() {
            return ControlFlow::Break(anyhow!("SELECT ... INTO is not supported"));
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<anyhow::Error> {
        match table_name(relation) {
            Ok(name) => {
                self.relations.push(name);
                ControlFlow::Continue(())
            }
            Err(error) => ControlFlow::Break(error),
        }
    }
}

/// The tables named directly in `FROM`-like lists (not in subqueries).
fn joined_tables(tables: &[TableWithJoins]) -> anyhow::Result<Vec<String>> {
    let mut names = Vec::new();
    for table in tables {
        for factor in
            std::iter::once(&table.relation).chain(table.joins.iter().map(|j| &j.relation))
        {
            if let TableFactor::Table { name, .. } = factor {
                names.push(table_name(name)?);
            }
        }
    }
    if names.is_empty() {
        bail!("no target table");
    }
    Ok(names)
}

fn table_name(name: &ObjectName) -> anyhow::Result<String> {
    let parts = name
        .0
        .iter()
        .map(|part| match part {
            ObjectNamePart::Identifier(ident) => Ok(identifier(ident)),
            ObjectNamePart::Function(_) => Err(anyhow!("computed table name {name}")),
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(parts.join("."))
}

fn identifier(ident: &sqlparser::ast::Ident) -> String {
    match ident.quote_style {
        Some(_) => ident.value.clone(),
        None => ident.value.to_lowercase(),
    }
}

/// The statement's leading keyword(s), for error messages.
fn statement_keyword(statement: &Statement) -> String {
    statement
        .to_string()
        .split_whitespace()
        .take(2)
        .collect::<Vec<_>>()
        .join(" ")
}

/// Where each action's SQL is, from the action schema's `@sql` annotations.
#[derive(Debug, Default)]
pub struct SqlFields(HashMap<String, Vec<Vec<String>>>);

impl SqlFields {
    /// Reads the `@sql` annotations of `namespace`'s actions.
    pub fn from_schema(schema: &str, namespace: &str) -> anyhow::Result<Self> {
        let (fragment, _) = SchemaFragment::from_cedarschema_str(schema)
            .map_err(|e| anyhow!("parsing action schema: {e}"))?;
        let (actions, _) = Schema::from_cedarschema_str(schema)
            .map_err(|e| anyhow!("parsing action schema: {e}"))?;
        let ns: EntityNamespace = namespace
            .parse()
            .map_err(|e| anyhow!("namespace {namespace}: {e}"))?;
        let mut fields = HashMap::new();
        for action in actions.actions() {
            if action.type_name().namespace() != namespace {
                continue;
            }
            let Some(spec) =
                fragment.action_annotation(Some(ns.clone()), action.id(), SQL_ANNOTATION)
            else {
                continue;
            };
            let name = action.id().unescaped().to_owned();
            let paths = parse_paths(spec)
                .with_context(|| format!("@{SQL_ANNOTATION} of action {name:?}"))?;
            fields.insert(name, paths);
        }
        Ok(Self(fields))
    }

    /// The tables the SQL of a call of `action` reads and writes, given its
    /// `context.input`; None if the action declares no SQL. An error if a
    /// declared path doesn't name a string (or list of strings), or a query
    /// can't be analyzed.
    pub fn access(&self, action: &str, input: &Value) -> Option<anyhow::Result<SqlAccess>> {
        let paths = self.0.get(action)?;
        Some(access(paths, input))
    }
}

fn parse_paths(spec: &str) -> anyhow::Result<Vec<Vec<String>>> {
    spec.split(',')
        .map(|path| {
            let path = path.trim();
            let segments: Vec<String> = path.split('.').map(str::to_owned).collect();
            if segments.iter().any(String::is_empty) {
                bail!("invalid path {path:?} (expected e.g. \"arg0.query\")");
            }
            Ok(segments)
        })
        .collect()
}

fn access(paths: &[Vec<String>], input: &Value) -> anyhow::Result<SqlAccess> {
    let mut access = SqlAccess::default();
    for path in paths {
        let dotted = path.join(".");
        let value = path
            .iter()
            .try_fold(input, |value, segment| value.get(segment))
            .ok_or_else(|| anyhow!("no SQL at input.{dotted}"))?;
        let queries: Vec<&str> = match value {
            Value::String(query) => vec![query],
            Value::Array(items) if !items.is_empty() => items
                .iter()
                .map(|item| {
                    item.as_str()
                        .ok_or_else(|| anyhow!("input.{dotted} is not a list of strings"))
                })
                .collect::<anyhow::Result<_>>()?,
            _ => bail!("input.{dotted} is not a string or a list of strings"),
        };
        for (i, query) in queries.into_iter().enumerate() {
            let analyzed = analyze(query).with_context(|| match value {
                Value::Array(_) => format!("input.{dotted}[{i}]"),
                _ => format!("input.{dotted}"),
            })?;
            access.union(analyzed);
        }
    }
    Ok(access)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn set(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|n| (*n).to_owned()).collect()
    }

    #[track_caller]
    fn assert_access(sql: &str, reads: &[&str], writes: &[&str]) {
        let access = analyze(sql).unwrap_or_else(|e| panic!("{sql}: {e:#}"));
        assert_eq!(access.reads, set(reads), "reads of {sql}");
        assert_eq!(access.writes, set(writes), "writes of {sql}");
    }

    #[track_caller]
    fn assert_rejected(sql: &str) {
        assert!(analyze(sql).is_err(), "{sql} should be rejected");
    }

    #[test]
    fn selects() {
        assert_access("SELECT * FROM customers", &["customers"], &[]);
        assert_access(
            "SELECT c.name, o.total FROM customers c JOIN orders o ON o.customer_id = c.id \
             WHERE o.id IN (SELECT order_id FROM refunds)",
            &["customers", "orders", "refunds"],
            &[],
        );
        assert_access(
            "SELECT * FROM (SELECT * FROM orders) AS t, LATERAL (SELECT 1 FROM items) i",
            &["items", "orders"],
            &[],
        );
        assert_access(
            "SELECT id FROM customers UNION SELECT id FROM suppliers",
            &["customers", "suppliers"],
            &[],
        );
        assert_access("SELECT 1", &[], &[]);
        // `TABLE t` (the generic dialect only parses it in a set operation).
        assert_access(
            "SELECT 1 UNION TABLE public.secrets",
            &["public.secrets"],
            &[],
        );
    }

    #[test]
    fn ctes_are_not_tables() {
        assert_access(
            "WITH recent AS (SELECT * FROM orders WHERE ts > now()) SELECT * FROM recent",
            &["orders"],
            &[],
        );
        // A CTE named like a table hides it; its body is what's read.
        assert_access(
            "WITH orders AS (SELECT * FROM secrets) SELECT * FROM orders",
            &["secrets"],
            &[],
        );
        // A qualified name never refers to a CTE.
        assert_access(
            "WITH orders AS (SELECT 1) SELECT * FROM orders, public.orders",
            &["public.orders"],
            &[],
        );
    }

    #[test]
    fn writes() {
        assert_access(
            "INSERT INTO orders (id, total) VALUES (1, 2)",
            &[],
            &["orders"],
        );
        assert_access(
            "INSERT INTO archive SELECT * FROM orders WHERE ts < '2020-01-01'",
            &["orders"],
            &["archive"],
        );
        assert_access(
            "UPDATE orders SET status = 'paid' WHERE id = 7",
            &[],
            &["orders"],
        );
        assert_access(
            "UPDATE orders SET total = p.total FROM prices p WHERE orders.sku = p.sku",
            &["prices"],
            &["orders"],
        );
        assert_access(
            "UPDATE orders SET total = (SELECT max(total) FROM orders) WHERE id = 1",
            &["orders"],
            &["orders"],
        );
        assert_access("DELETE FROM sessions WHERE expired", &[], &["sessions"]);
        assert_access(
            "DELETE FROM orders USING customers WHERE orders.customer_id = customers.id",
            &["customers"],
            &["orders"],
        );
        assert_access(
            "DELETE FROM orders WHERE customer_id IN (SELECT id FROM banned)",
            &["banned"],
            &["orders"],
        );
        // A data-modifying CTE writes, too.
        assert_access(
            "WITH gone AS (DELETE FROM orders RETURNING *) INSERT INTO archive SELECT * FROM gone",
            &[],
            &["archive", "orders"],
        );
    }

    #[test]
    fn statements_are_combined() {
        assert_access(
            "SELECT * FROM customers; UPDATE orders SET x = 1",
            &["customers"],
            &["orders"],
        );
    }

    #[test]
    fn names() {
        assert_access(
            r#"SELECT * FROM Orders, "Customers", Public.Items, "Sales"."Q1""#,
            &["Customers", "Sales.Q1", "orders", "public.items"],
            &[],
        );
    }

    #[test]
    fn unsupported_sql_is_rejected() {
        assert_rejected("DROP TABLE orders");
        assert_rejected("TRUNCATE orders");
        assert_rejected("CREATE TABLE t (id INT)");
        assert_rejected("ALTER TABLE orders ADD COLUMN x INT");
        assert_rejected("GRANT SELECT ON orders TO bob");
        assert_rejected("SELECT * INTO copy FROM orders");
        assert_rejected("SELECT 1; DROP TABLE orders");
        assert_rejected("SELEC * FROM orders");
        assert_rejected("");
        assert_rejected("-- nothing");
    }

    const SCHEMA: &str = r#"
        namespace Temporal {
          entity Worker;
          entity TaskQueue;
          @sql("arg0.query")
          action "db/Query" appliesTo { principal: [Worker], resource: [TaskQueue] };
          @sql(" arg0.queries , arg1 ")
          action "db/Batch" appliesTo { principal: [Worker], resource: [TaskQueue] };
          action "db/Other" appliesTo { principal: [Worker], resource: [TaskQueue] };
        }
    "#;

    #[test]
    fn fields_from_schema() {
        let fields = SqlFields::from_schema(SCHEMA, "Temporal").unwrap();
        assert!(fields.access("db/Other", &json!({})).is_none());
        assert!(fields.access("db/Missing", &json!({})).is_none());

        let access = fields
            .access(
                "db/Query",
                &json!({"arg0": {"query": "SELECT * FROM orders"}}),
            )
            .unwrap()
            .unwrap();
        assert_eq!(access.reads, set(&["orders"]));

        let access = fields
            .access(
                "db/Batch",
                &json!({
                    "arg0": {"queries": ["SELECT * FROM customers", "DELETE FROM sessions"]},
                    "arg1": "INSERT INTO audit VALUES (1)",
                }),
            )
            .unwrap()
            .unwrap();
        assert_eq!(access.reads, set(&["customers"]));
        assert_eq!(access.writes, set(&["audit", "sessions"]));
        assert_eq!(
            access.to_json(),
            json!({"reads": ["customers"], "writes": ["audit", "sessions"]})
        );
    }

    #[test]
    fn bad_sql_fields_are_errors() {
        let fields = SqlFields::from_schema(SCHEMA, "Temporal").unwrap();
        let err = |input: Value| {
            format!(
                "{:#}",
                fields.access("db/Batch", &input).unwrap().unwrap_err()
            )
        };
        let ok = json!(["SELECT 1"]);
        // One bad query spoils the call.
        assert!(
            err(json!({"arg0": {"queries": ["SELECT 1", "DROP TABLE x"]}, "arg1": "SELECT 1"}))
                .contains("input.arg0.queries[1]")
        );
        assert!(err(json!({"arg0": {"queries": ok}})).contains("no SQL at input.arg1"));
        assert!(err(json!({"arg0": {"queries": []}, "arg1": "SELECT 1"})).contains("not a string"));
        assert!(
            err(json!({"arg0": {"queries": [1]}, "arg1": "SELECT 1"}))
                .contains("not a list of strings")
        );
        assert!(err(json!({"arg0": {"queries": ok}, "arg1": 5})).contains("not a string"));
    }

    #[test]
    fn bad_annotation_is_rejected() {
        let schema = SCHEMA.replace(r#"@sql("arg0.query")"#, r#"@sql("arg0..query")"#);
        assert!(SqlFields::from_schema(&schema, "Temporal").is_err());
        let schema = SCHEMA.replace(r#"@sql("arg0.query")"#, r#"@sql("")"#);
        assert!(SqlFields::from_schema(&schema, "Temporal").is_err());
    }
}
