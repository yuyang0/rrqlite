use std::fs;
use std::path::Path;

use core_command::command;
use core_tracing::tracing;
use core_util_misc::random::thread_rand_string;
use r2d2::PooledConnection;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::types::{ToSqlOutput, Value, ValueRef};
use rusqlite::{DatabaseName, OpenFlags, Row, ToSql, Transaction};

use crate::error::Result;

struct Parameter<'a> {
    p: &'a Option<command::parameter::Value>,
}

impl<'a> ToSql for Parameter<'a> {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        let output = match self.p {
            None => ToSqlOutput::Owned(Value::Null),
            Some(v) => match v {
                command::parameter::Value::I(iv) => ToSqlOutput::Owned(Value::Integer(*iv)),
                command::parameter::Value::D(dv) => ToSqlOutput::Owned(Value::Real(*dv)),
                command::parameter::Value::B(bv) => {
                    ToSqlOutput::Owned(Value::Integer(if *bv { 1 } else { 0 }))
                }
                command::parameter::Value::Y(bv) => ToSqlOutput::Borrowed(ValueRef::Blob(bv)),
                command::parameter::Value::S(sv) => {
                    ToSqlOutput::Borrowed(ValueRef::Text(sv.as_bytes()))
                }
            },
        };
        Ok(output)
    }
}

fn convert_params<'a>(
    parameters: &'a Vec<command::Parameter>,
) -> Vec<(&'a str, Box<dyn ToSql + 'a>)> {
    let mut named_params = vec![];
    for p in parameters.iter() {
        let sql_q: Box<dyn ToSql> = Box::new(Parameter { p: &p.value });
        named_params.push((&p.name[..], sql_q));
    }
    named_params
}

#[derive(Default)]
pub struct Context<'conn> {
    pub txn: Option<Transaction<'conn>>,
}

impl<'conn> Context<'conn> {
    pub fn new(
        conn: &'conn mut PooledConnection<SqliteConnectionManager>,
        need_commit: bool,
    ) -> Result<Context<'conn>> {
        let mut txn = conn.transaction()?;
        if need_commit {
            txn.set_drop_behavior(rusqlite::DropBehavior::Commit);
        }
        Ok(Context { txn: Some(txn) })
    }
}

#[derive(Default)]
pub struct Config {
    path: String,
    use_mem: bool,
    enable_shared_cache: bool,
}

pub struct DB {
    path: String,
    in_mem: bool,

    pool: r2d2::Pool<SqliteConnectionManager>,
}

impl DB {
    pub fn new(cfg: &Config) -> Result<DB> {
        let p: String;

        let mut flags = OpenFlags::SQLITE_OPEN_READ_WRITE;
        if cfg.use_mem {
            p = format!("file:/{}", thread_rand_string(8));
            flags |= OpenFlags::SQLITE_OPEN_MEMORY | OpenFlags::SQLITE_OPEN_SHARED_CACHE;
        } else {
            p = format!("file:{}", cfg.path);
            flags |= OpenFlags::SQLITE_OPEN_CREATE;
            if cfg.enable_shared_cache {
                flags |= OpenFlags::SQLITE_OPEN_SHARED_CACHE;
            }
        }
        let manager = SqliteConnectionManager::file(&p).with_flags(flags);
        let pool = r2d2::Pool::new(manager).unwrap();

        let db = DB {
            in_mem: true,
            path: p,
            pool: pool,
        };
        Ok(db)
    }

    pub fn new_mem_db() -> Result<DB> {
        let cfg = Config {
            use_mem: true,
            ..Default::default()
        };
        Self::new(&cfg)
    }

    pub fn new_disk_db<P: AsRef<Path>>(db_path: P, enable_shared_cache: bool) -> Result<DB> {
        let db_path = db_path
            .as_ref()
            .to_str()
            .map_or(String::from(""), |v| String::from(v));
        let cfg = Config {
            path: db_path,
            enable_shared_cache: enable_shared_cache,
            use_mem: false,
            ..Default::default()
        };
        Self::new(&cfg)
    }

    pub fn get_conn(&self) -> Result<PooledConnection<SqliteConnectionManager>> {
        let conn = self.pool.get()?;
        Ok(conn)
    }

    pub fn load_into_mem<P: AsRef<Path>>(db_path: P) -> Result<DB> {
        let mem_db = DB::new_mem_db()?;
        mem_db.restore(db_path)?;
        Ok(mem_db)
    }

    pub fn pragma<F, V>(
        &self,
        ctx: &Context,
        schema_name: Option<DatabaseName<'_>>,
        pragma_name: &str,
        pragma_value: V,
        f: F,
    ) -> Result<()>
    where
        F: FnMut(&Row<'_>) -> rusqlite::Result<()>,
        V: ToSql,
    {
        let pooled_conn = self.pool.get().unwrap();
        let mut conn = &*pooled_conn;

        if ctx.txn.is_some() {
            if let Some(ref ref_txn) = ctx.txn {
                conn = ref_txn;
            }
        }
        conn.pragma(schema_name, pragma_name, pragma_value, f)?;
        Ok(())
    }

    pub fn close(self) -> Result<()> {
        // let ro_res = self.ro_conn.close();
        // let rw_res = self.rw_conn.close();
        // match ro_res {
        //     Err((_, err)) => return Err(ErrorCode::from(err)),
        //     _ => (),
        // }
        // match rw_res {
        //     Err((_, err)) => Err(ErrorCode::from(err)),
        //     _ => Ok(()),
        // }
        Ok(())
    }

    pub fn execute(&self, ctx: &Context, req: &command::Request) -> Result<command::ExecuteResult> {
        let mut final_res = command::ExecuteResult::default();
        let mut pooled_conn = self.pool.get().unwrap();
        let mut conn = &*pooled_conn;
        let mut txn;

        if ctx.txn.is_some() {
            if let Some(ref ref_txn) = ctx.txn {
                conn = ref_txn;
            }
        } else {
            if req.transaction {
                txn = pooled_conn.transaction()?;
                txn.set_drop_behavior(rusqlite::DropBehavior::Commit);
                conn = &txn;
            }
        }

        for proto_stmt in req.statements.iter() {
            let mut single_res = command::SingleExecuteResult::default();

            let params = convert_params(&proto_stmt.parameters);
            let params: Vec<(&str, &dyn ToSql)> = params
                .iter()
                .map(|(name, v)| (*name, v as &dyn ToSql))
                .collect();
            let update_rows = conn.execute(&proto_stmt.sql, &params[..])?;
            single_res.rows_affected += update_rows as i64;
            final_res.results.push(single_res);
        }
        Ok(final_res)
    }

    pub fn execute_batch(&self, sql: &str) -> Result<()> {
        let conn = self.pool.get().unwrap();
        conn.execute_batch(sql)?;
        Ok(())
    }

    pub fn execute_str_stmt(&self, ctx: &Context, stmt: &str) -> Result<command::ExecuteResult> {
        let req = command::Request {
            statements: vec![command::Statement {
                sql: stmt.to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };
        self.execute(ctx, &req)
    }

    pub fn query(&self, ctx: &Context, req: &command::Request) -> Result<command::QueryResult> {
        let mut all_rows = vec![];
        let mut pooled_conn = self.pool.get().unwrap();
        let mut conn = &*pooled_conn;
        let mut txn;

        if ctx.txn.is_some() {
            if let Some(ref ref_txn) = ctx.txn {
                conn = ref_txn;
            }
        } else {
            if req.transaction {
                txn = pooled_conn.transaction()?;
                txn.set_drop_behavior(rusqlite::DropBehavior::Commit);
                conn = &txn;
            }
        }

        for proto_stmt in req.statements.iter() {
            let mut rows = command::QueryRows::default();

            let params = convert_params(&proto_stmt.parameters);
            let params: Vec<(&str, &dyn ToSql)> = params
                .iter()
                .map(|(name, v)| (*name, v as &dyn ToSql))
                .collect();
            let mut stmt = conn.prepare(&proto_stmt.sql)?;
            for col in stmt.columns().iter() {
                rows.columns.push(String::from(col.name()));
                match col.decl_type() {
                    Some(v) => rows.types.push(String::from(v)),
                    _ => rows.types.push(String::from("")),
                }
            }
            // if ! stmt.readonly() {}
            let query_res = stmt.query(&params[..]);
            match query_res {
                Ok(mut raw_rows) => loop {
                    match raw_rows.next() {
                        Ok(v) => match v {
                            Some(raw_row) => {
                                let mut vals = command::Values::default();
                                for idx in 0..rows.types.len() {
                                    let mut p = command::Parameter::default();
                                    p.name = String::from(&rows.columns[idx]);
                                    let raw_v: Value = raw_row.get(idx)?;
                                    match raw_v {
                                        Value::Null => p.value = None,
                                        Value::Integer(iv) => {
                                            if rows.types[idx].is_empty() {
                                                rows.types[idx] = String::from("Integer");
                                            }
                                            p.value = Some(command::parameter::Value::I(iv))
                                        }
                                        Value::Real(rv) => {
                                            if rows.types[idx].is_empty() {
                                                rows.types[idx] = String::from("Real");
                                            }
                                            p.value = Some(command::parameter::Value::D(rv))
                                        }
                                        Value::Text(tv) => {
                                            if rows.types[idx].is_empty() {
                                                rows.types[idx] = String::from("Text");
                                            }
                                            p.value = Some(command::parameter::Value::S(tv))
                                        }
                                        Value::Blob(bv) => {
                                            if rows.types[idx].is_empty() {
                                                rows.types[idx] = String::from("Blob");
                                            }
                                            p.value = Some(command::parameter::Value::Y(bv))
                                        }
                                    }
                                    vals.parameters.push(p);
                                }
                                rows.values.push(vals);
                            }
                            _ => break,
                        },
                        Err(err) => tracing::error!("failed to get row from rows {}", err),
                    }
                },
                Err(err) => rows.error = err.to_string(),
            }
            all_rows.push(rows);
        }
        // if req.transaction {
        //     tx.commit()?;
        // }
        Ok(command::QueryResult {
            results: all_rows,
            ..Default::default()
        })
    }

    pub fn query_str_stmt(&self, ctx: &Context, stmt: &str) -> Result<command::QueryResult> {
        let req = command::Request {
            statements: vec![command::Statement {
                sql: stmt.to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };
        self.query(ctx, &req)
    }

    // pub fn serialize(&self) -> Result<Vec<u8>> {}
    // write snapshot to specified path.
    pub fn backup<P: AsRef<Path>>(&self, dst_path: P) -> Result<()> {
        let conn = self.pool.get().unwrap();
        conn.backup(rusqlite::DatabaseName::Main, dst_path, None)?;
        Ok(())
    }

    pub fn restore<P: AsRef<Path>>(&self, src_path: P) -> Result<()> {
        let f = |p: rusqlite::backup::Progress| {
            tracing::info!("*** pagecount: {}, remaining: {}", p.pagecount, p.remaining);
        };
        let mut conn = self.pool.get().unwrap();
        conn.restore(rusqlite::DatabaseName::Main, src_path, Some(f))?;
        Ok(())
    }

    pub fn backup_to_sql<T: std::io::Write>(&self, ctx: &Context, dst: &mut T) -> Result<()> {
        dst.write(b"PRAGMA foreign_keys=OFF;\nBEGIN TRANSACTION;\n")?;

        // Get the schema.
        let sql = r#"
            SELECT "name", "type", "sql" FROM "sqlite_master"
            WHERE "sql" NOT NULL AND "type" == 'table' ORDER BY "name"
        "#;
        let res = self.query_str_stmt(ctx, sql)?;
        assert_eq!(res.results.len(), 1);
        for vals in res.results[0].values.iter() {
            let p = &vals.parameters[0];
            let tbl_name = match p.value {
                Some(command::parameter::Value::S(ref sv)) => sv,
                _ => panic!("need string"),
            };
            let sql = match tbl_name.as_str() {
                "sqlite_sequence" => r#"DELETE FROM "sqlite_sequence";"#,
                "sqlite_stat1" => r#"ANALYZE "sqlite_master";"#,
                s if s.starts_with("sqlite_") => continue,
                _ => match vals.parameters[2].value {
                    Some(command::parameter::Value::S(ref sv)) => sv,
                    _ => panic!("need string"),
                },
            };
            dst.write(sql.as_bytes())?;
            dst.write(b";\n")?;

            let tbl_ident = tbl_name.replace("\"", "\"\"");
            let mut col_names = vec![];
            self.pragma(
                ctx,
                Some(rusqlite::DatabaseName::Main),
                "table_info",
                &tbl_ident,
                |row| {
                    let col_name: String = row.get(1)?;
                    col_names.push(format!(r#"'||quote("{}")||'"#, col_name));
                    Ok(())
                },
            )?;

            let sql = format!(
                r#"SELECT 'INSERT INTO "{}" VALUES({})' FROM "{}";"#,
                tbl_ident,
                col_names.join(","),
                tbl_ident
            );
            let res = self.query_str_stmt(&ctx, &sql)?;
            for vals in res.results[0].values.iter() {
                let ss = match vals.parameters[0].value {
                    Some(command::parameter::Value::S(ref sv)) => sv,
                    _ => panic!("need string"),
                };
                dst.write(ss.as_bytes())?;
                dst.write(b";\n")?;
            }
        }

        // Do indexes, triggers, and views.
        let sql = r#"SELECT "name", "type", "sql" FROM "sqlite_master"
    			  WHERE "sql" NOT NULL AND "type" IN ('index', 'trigger', 'view')"#;
        let res = self.query_str_stmt(&ctx, sql)?;
        assert_eq!(res.results.len(), 1);
        for vals in res.results[0].values.iter() {
            let p = &vals.parameters[2];
            let sql = match p.value {
                Some(command::parameter::Value::S(ref sv)) => sv,
                _ => panic!("need string"),
            };
            dst.write(sql.as_bytes())?;
            dst.write(b";\n")?;
        }

        dst.write(b"COMMIT;\n")?;
        Ok(())
    }

    pub fn serialize(&self) -> Result<Vec<u8>> {
        let content;
        if self.in_mem {
            let t = tempfile::tempdir()?;
            let p = t.path().join("serialize-tmp.db");
            self.backup(&p)?;
            content = fs::read(&p)?;
        } else {
            content = fs::read(&self.path)?;
        }
        return Ok(content);
    }

    pub fn deserialize(&self, data: &[u8]) -> Result<()> {
        let t = tempfile::tempdir()?;
        let p = t.path().join("deserialize-tmp.db");
        fs::write(&p, data)?;

        self.restore(&p)
    }
}
