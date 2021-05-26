extern crate chrono;
extern crate msql_srv;
extern crate mysql;
extern crate mysql_common as myc;
extern crate nom;
use async_trait::async_trait;

use mysql::prelude::*;
use std::io;

use msql_srv::{
    Column, ErrorKind, InitWriter, MysqlIntermediary, MysqlShim, ParamParser, QueryResultWriter,
    StatementMetaWriter,
};

fn db_test<M, C>(db: M, c: C)
where
    M: MysqlShim + 'static,
    C: FnOnce(&mut mysql::Conn) -> (),
{
    let r = tokio::runtime::Runtime::new().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    let jh = r.spawn(async move {
        let mut listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tx.send(port).unwrap();
        let (s, _) = listener.accept().await.unwrap();
        MysqlIntermediary::run_on_tcp(db, s)
            .await
            .unwrap_or_else(|_| {
                println!("run error");
            })
    });

    let port = rx.recv().unwrap();
    let mut conn = mysql::Conn::new(&format!("mysql://127.0.0.1:{}", port)).unwrap();
    c(&mut conn);
    drop(conn);
    let mut r = tokio::runtime::Runtime::new().unwrap();
    r.block_on(jh).unwrap();
}
//}

#[test]
fn it_connects() {
    pub struct TestingShim;
    #[async_trait]
    impl MysqlShim for TestingShim {
        type Error = io::Error;
    }
    db_test(TestingShim {}, |_| {});
}

#[test]
fn it_inits_ok() {
    pub struct TestingShim;
    #[async_trait]
    impl MysqlShim for TestingShim {
        type Error = io::Error;

        async fn on_init(
            &mut self,
            schema: &str,
            writer: InitWriter<'_>,
        ) -> Result<(), Self::Error> {
            assert_eq!(schema, "test");
            writer.ok()
        }
    }
    db_test(TestingShim {}, |db| assert_eq!(true, db.select_db("test")));
}

#[test]
fn it_inits_error() {
    pub struct TestingShim;
    #[async_trait]
    impl MysqlShim for TestingShim {
        type Error = io::Error;

        async fn on_init(
            &mut self,
            schema: &str,
            writer: InitWriter<'_>,
        ) -> Result<(), Self::Error> {
            assert_eq!(schema, "test");
            writer
                .error(
                    ErrorKind::ER_BAD_DB_ERROR,
                    format!("Database {} not found", schema).as_bytes(),
                )
                .await
        }
    }

    db_test(TestingShim {}, |db| assert_eq!(false, db.select_db("test")));
}

#[test]
fn it_inits_on_use_query_ok() {
    pub struct TestingShim;
    #[async_trait]
    impl MysqlShim for TestingShim {
        type Error = io::Error;

        async fn on_init(
            &mut self,
            schema: &str,
            writer: InitWriter<'_>,
        ) -> Result<(), Self::Error> {
            assert_eq!(schema, "test");
            writer.ok()
        }
    }

    db_test(TestingShim {}, |db| match db.query_drop("USE `test`;") {
        Ok(_) => assert!(true),
        Err(_) => assert!(false),
    });
}

#[test]
fn it_pings() {
    pub struct TestingShim;
    #[async_trait]
    impl MysqlShim for TestingShim {
        type Error = io::Error;
    }

    db_test(TestingShim {}, |db| assert_eq!(db.ping(), true))
}

#[test]
fn empty_response() {
    pub struct TestingShim;
    #[async_trait]
    impl MysqlShim for TestingShim {
        type Error = io::Error;

        async fn on_query(&mut self, _: &str, w: QueryResultWriter<'_>) -> Result<(), Self::Error> {
            w.completed(0, 0).await
        }
    }

    db_test(TestingShim {}, |db| {
        assert_eq!(db.query_iter("SELECT a, b FROM foo").unwrap().count(), 0);
    });
}

#[test]
fn no_rows() {
    pub struct TestingShim {
        cols: Vec<Column>,
    }
    #[async_trait]
    impl MysqlShim for TestingShim {
        type Error = io::Error;

        async fn on_query(&mut self, _: &str, w: QueryResultWriter<'_>) -> Result<(), Self::Error> {
            w.start(&self.cols).await?.finish().await
        }
    }

    db_test(
        TestingShim {
            cols: vec![Column {
                table: String::new(),
                column: "a".to_owned(),
                coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
                colflags: myc::constants::ColumnFlags::empty(),
            }],
        },
        |db| {
            assert_eq!(db.query_iter("SELECT a, b FROM foo").unwrap().count(), 0);
        },
    )
}

#[test]
fn no_columns() {
    pub struct TestingShim {}
    #[async_trait]
    impl MysqlShim for TestingShim {
        type Error = io::Error;

        async fn on_query(&mut self, _: &str, w: QueryResultWriter<'_>) -> Result<(), Self::Error> {
            w.start(&[]).await?.finish().await
        }
    }

    db_test(TestingShim {}, |db| {
        assert_eq!(db.query_iter("SELECT a, b FROM foo").unwrap().count(), 0);
    });
}

#[test]
fn no_columns_but_rows() {
    pub struct TestingShim {}
    #[async_trait]
    impl MysqlShim for TestingShim {
        type Error = io::Error;

        async fn on_query(&mut self, _: &str, w: QueryResultWriter<'_>) -> Result<(), Self::Error> {
            w.start(&[]).await?.write_col(42).map(|_| ())
        }
    }

    db_test(TestingShim {}, |db| {
        assert_eq!(db.query_iter("SELECT a, b FROM foo").unwrap().count(), 0);
    });
}

#[test]
fn error_response() {
    pub struct TestingShim {}
    #[async_trait]
    impl MysqlShim for TestingShim {
        type Error = io::Error;

        async fn on_query(&mut self, _: &str, w: QueryResultWriter<'_>) -> Result<(), Self::Error> {
            let err = (ErrorKind::ER_NO, "clearly not");
            w.error(err.0, err.1.as_bytes()).await
        }
    }

    let err = (ErrorKind::ER_NO, "clearly not");
    db_test(TestingShim {}, |db| {
        if let mysql::Error::MySqlError(e) = db.query_iter("SELECT a, b FROM foo").unwrap_err() {
            assert_eq!(
                e,
                mysql::error::MySqlError {
                    state: String::from_utf8(err.0.sqlstate().to_vec()).unwrap(),
                    message: err.1.to_owned(),
                    code: err.0 as u16,
                }
            );
        } else {
            unreachable!();
        }
    })
}

#[test]
fn empty_on_drop() {
    pub struct TestingShim {
        cols: Vec<Column>,
    }
    #[async_trait]
    impl MysqlShim for TestingShim {
        type Error = io::Error;

        async fn on_query(&mut self, _: &str, w: QueryResultWriter<'_>) -> Result<(), Self::Error> {
            let _ = w.start(&self.cols).await?;
            Ok(())
        }
    }

    db_test(
        TestingShim {
            cols: vec![Column {
                table: String::new(),
                column: "a".to_owned(),
                coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
                colflags: myc::constants::ColumnFlags::empty(),
            }],
        },
        |db| {
            assert_eq!(db.query_iter("SELECT a, b FROM foo").unwrap().count(), 0);
        },
    )
}

#[test]
fn it_queries_nulls() {
    pub struct TestingShim {}
    #[async_trait]
    impl MysqlShim for TestingShim {
        type Error = io::Error;

        async fn on_query(&mut self, _: &str, w: QueryResultWriter<'_>) -> Result<(), Self::Error> {
            let cols = &[Column {
                table: String::new(),
                column: "a".to_owned(),
                coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
                colflags: myc::constants::ColumnFlags::empty(),
            }];
            let mut w = w.start(cols).await?;
            w.write_col(None::<i16>)?;
            w.finish().await
        }
    }
    db_test(TestingShim {}, |db| {
        let row = db
            .query_iter("SELECT a, b FROM foo")
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(row.as_ref(0), Some(&mysql::Value::NULL));
    })
}

#[test]
fn it_queries() {
    pub struct TestingShim {}
    #[async_trait]
    impl MysqlShim for TestingShim {
        type Error = io::Error;

        async fn on_query(&mut self, _: &str, w: QueryResultWriter<'_>) -> Result<(), Self::Error> {
            let cols = &[Column {
                table: String::new(),
                column: "a".to_owned(),
                coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
                colflags: myc::constants::ColumnFlags::empty(),
            }];
            let mut w = w.start(cols).await?;
            w.write_col(1024i16)?;
            w.finish().await
        }
    }

    db_test(TestingShim {}, |db| {
        let row = db
            .query_iter("SELECT a, b FROM foo")
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(row.get::<i16, _>(0), Some(1024));
    })
}

#[test]
fn multi_result() {
    pub struct TestingShim {}
    #[async_trait]
    impl MysqlShim for TestingShim {
        type Error = io::Error;

        async fn on_query(&mut self, _: &str, w: QueryResultWriter<'_>) -> Result<(), Self::Error> {
            let cols = &[Column {
                table: String::new(),
                column: "a".to_owned(),
                coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
                colflags: myc::constants::ColumnFlags::empty(),
            }];
            let mut row = w.start(cols).await?;
            row.write_col(1024i16)?;
            let w = row.finish_one()?;
            let mut row = w.start(cols).await?;
            row.write_col(1025i16)?;
            row.finish().await
        }
    }

    db_test(TestingShim {}, |db| {
        let mut result = db
            .query_iter("SELECT a FROM foo; SELECT a FROM foo")
            .unwrap();
        let mut set = result.next_set().unwrap().unwrap();
        let row1: Vec<_> = set
            .by_ref()
            .filter_map(|row| row.unwrap().get::<i16, _>(0))
            .collect();
        assert_eq!(row1, vec![1024]);
        drop(set);
        let mut set = result.next_set().unwrap().unwrap();
        let row2: Vec<_> = set
            .by_ref()
            .filter_map(|row| row.unwrap().get::<i16, _>(0))
            .collect();
        assert_eq!(row2, vec![1025]);
        drop(set);
        assert!(result.next_set().is_none());
    })
}

#[test]
fn it_queries_many_rows() {
    pub struct TestingShim {}
    #[async_trait]
    impl MysqlShim for TestingShim {
        type Error = io::Error;

        async fn on_query(&mut self, _: &str, w: QueryResultWriter<'_>) -> Result<(), Self::Error> {
            let cols = &[
                Column {
                    table: String::new(),
                    column: "a".to_owned(),
                    coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
                    colflags: myc::constants::ColumnFlags::empty(),
                },
                Column {
                    table: String::new(),
                    column: "b".to_owned(),
                    coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
                    colflags: myc::constants::ColumnFlags::empty(),
                },
            ];
            let mut w = w.start(cols).await?;
            w.write_col(1024i16)?;
            w.write_col(1025i16)?;
            w.end_row()?;
            w.write_row(&[1024i16, 1025i16])?;
            w.finish().await
        }
    }
    db_test(TestingShim {}, |db| {
        let mut rows = 0;
        for row in db.query_iter("SELECT a, b FROM foo").unwrap() {
            let row = row.unwrap();
            assert_eq!(row.get::<i16, _>(0), Some(1024));
            assert_eq!(row.get::<i16, _>(1), Some(1025));
            rows += 1;
        }
        assert_eq!(rows, 2);
    })
}

#[test]
fn it_prepares() {
    let cols = vec![Column {
        table: String::new(),
        column: "a".to_owned(),
        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
        colflags: myc::constants::ColumnFlags::empty(),
    }];
    pub struct TestingShim {
        cols: Vec<Column>,
    }
    #[async_trait]
    impl MysqlShim for TestingShim {
        type Error = io::Error;

        async fn on_prepare(
            &mut self,
            query: &str,
            info: StatementMetaWriter<'_>,
        ) -> io::Result<()> {
            assert_eq!(query, "SELECT a FROM b WHERE c = ?");
            let params = vec![Column {
                table: String::new(),
                column: "c".to_owned(),
                coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
                colflags: myc::constants::ColumnFlags::empty(),
            }];
            info.reply(41, &params, &self.cols)
        }
        async fn on_execute(
            &mut self,
            stmt: u32,
            params: ParamParser<'_>,
            w: QueryResultWriter<'_>,
        ) -> io::Result<()> {
            let params: Vec<msql_srv::ParamValue> = params.into_iter().collect();
            assert_eq!(stmt, 41);
            assert_eq!(params.len(), 1);
            // rust-mysql sends all numbers as LONGLONG
            assert_eq!(
                params[0].coltype,
                myc::constants::ColumnType::MYSQL_TYPE_LONGLONG
            );
            assert_eq!(Into::<i8>::into(params[0].value), 42i8);

            let mut w = w.start(&self.cols).await?;
            w.write_col(1024i16)?;
            w.finish().await
        }
    }
    db_test(TestingShim { cols }, |db| {
        let row = db
            .exec_iter("SELECT a FROM b WHERE c = ?", (42i16,))
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(row.get::<i16, _>(0), Some(1024i16));
    })
}

#[test]
fn insert_exec() {
    pub struct TestingShim {
        params: Vec<Column>,
    }
    #[async_trait]
    impl MysqlShim for TestingShim {
        type Error = io::Error;

        async fn on_prepare(&mut self, _: &str, info: StatementMetaWriter<'_>) -> io::Result<()> {
            info.reply(1, &self.params, &[])
        }
        async fn on_execute(
            &mut self,
            _: u32,
            params: ParamParser<'_>,
            w: QueryResultWriter<'_>,
        ) -> io::Result<()> {
            let params: Vec<msql_srv::ParamValue> = params.into_iter().collect();
            assert_eq!(params.len(), 7);
            assert_eq!(
                params[0].coltype,
                myc::constants::ColumnType::MYSQL_TYPE_VAR_STRING
            );
            assert_eq!(
                params[1].coltype,
                myc::constants::ColumnType::MYSQL_TYPE_VAR_STRING
            );
            assert_eq!(
                params[2].coltype,
                myc::constants::ColumnType::MYSQL_TYPE_VAR_STRING
            );
            assert_eq!(
                params[3].coltype,
                myc::constants::ColumnType::MYSQL_TYPE_DATETIME
            );
            assert_eq!(
                params[4].coltype,
                myc::constants::ColumnType::MYSQL_TYPE_VAR_STRING
            );
            assert_eq!(
                params[5].coltype,
                myc::constants::ColumnType::MYSQL_TYPE_VAR_STRING
            );
            assert_eq!(
                params[6].coltype,
                myc::constants::ColumnType::MYSQL_TYPE_VAR_STRING
            );
            assert_eq!(Into::<&str>::into(params[0].value), "user199");
            assert_eq!(Into::<&str>::into(params[1].value), "user199@example.com");
            assert_eq!(
                Into::<&str>::into(params[2].value),
                "$2a$10$Tq3wrGeC0xtgzuxqOlc3v.07VTUvxvwI70kuoVihoO2cE5qj7ooka"
            );
            assert_eq!(
                Into::<chrono::NaiveDateTime>::into(params[3].value),
                chrono::NaiveDate::from_ymd(2018, 4, 6).and_hms(13, 0, 56)
            );
            assert_eq!(Into::<&str>::into(params[4].value), "token199");
            assert_eq!(Into::<&str>::into(params[5].value), "rsstoken199");
            assert_eq!(Into::<&str>::into(params[6].value), "mtok199");

            w.completed(42, 1).await
        }
    }
    let params = vec![
        Column {
            table: String::new(),
            column: "username".to_owned(),
            coltype: myc::constants::ColumnType::MYSQL_TYPE_VARCHAR,
            colflags: myc::constants::ColumnFlags::empty(),
        },
        Column {
            table: String::new(),
            column: "email".to_owned(),
            coltype: myc::constants::ColumnType::MYSQL_TYPE_VARCHAR,
            colflags: myc::constants::ColumnFlags::empty(),
        },
        Column {
            table: String::new(),
            column: "pw".to_owned(),
            coltype: myc::constants::ColumnType::MYSQL_TYPE_VARCHAR,
            colflags: myc::constants::ColumnFlags::empty(),
        },
        Column {
            table: String::new(),
            column: "created".to_owned(),
            coltype: myc::constants::ColumnType::MYSQL_TYPE_DATETIME,
            colflags: myc::constants::ColumnFlags::empty(),
        },
        Column {
            table: String::new(),
            column: "session".to_owned(),
            coltype: myc::constants::ColumnType::MYSQL_TYPE_VARCHAR,
            colflags: myc::constants::ColumnFlags::empty(),
        },
        Column {
            table: String::new(),
            column: "rss".to_owned(),
            coltype: myc::constants::ColumnType::MYSQL_TYPE_VARCHAR,
            colflags: myc::constants::ColumnFlags::empty(),
        },
        Column {
            table: String::new(),
            column: "mail".to_owned(),
            coltype: myc::constants::ColumnType::MYSQL_TYPE_VARCHAR,
            colflags: myc::constants::ColumnFlags::empty(),
        },
    ];

    db_test(TestingShim { params }, |db| {
        let res = db
            .exec_iter(
                "INSERT INTO `users` \
                 (`username`, `email`, `password_digest`, `created_at`, \
                 `session_token`, `rss_token`, `mailing_list_token`) \
                 VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    "user199",
                    "user199@example.com",
                    "$2a$10$Tq3wrGeC0xtgzuxqOlc3v.07VTUvxvwI70kuoVihoO2cE5qj7ooka",
                    mysql::Value::Date(2018, 4, 6, 13, 0, 56, 0),
                    "token199",
                    "rsstoken199",
                    "mtok199",
                ),
            )
            .unwrap();
        assert_eq!(res.affected_rows(), 42);
        assert_eq!(res.last_insert_id(), Some(1));
    });
}

#[test]
fn send_long() {
    let cols = vec![Column {
        table: String::new(),
        column: "a".to_owned(),
        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
        colflags: myc::constants::ColumnFlags::empty(),
    }];
    let params = vec![Column {
        table: String::new(),
        column: "c".to_owned(),
        coltype: myc::constants::ColumnType::MYSQL_TYPE_BLOB,
        colflags: myc::constants::ColumnFlags::empty(),
    }];

    pub struct TestingShim {
        params: Vec<Column>,
        cols: Vec<Column>,
    }
    #[async_trait]
    impl MysqlShim for TestingShim {
        type Error = io::Error;

        async fn on_prepare(
            &mut self,
            query: &str,
            info: StatementMetaWriter<'_>,
        ) -> io::Result<()> {
            assert_eq!(query, "SELECT a FROM b WHERE c = ?");
            info.reply(41, &self.params, &self.cols)
        }
        async fn on_execute(
            &mut self,
            stmt: u32,
            params: ParamParser<'_>,
            w: QueryResultWriter<'_>,
        ) -> io::Result<()> {
            assert_eq!(stmt, 41);
            let params: Vec<msql_srv::ParamValue> = params.into_iter().collect();
            assert_eq!(params.len(), 1);
            // rust-mysql sends all strings as VAR_STRING
            assert_eq!(
                params[0].coltype,
                myc::constants::ColumnType::MYSQL_TYPE_VAR_STRING
            );
            assert_eq!(Into::<&[u8]>::into(params[0].value), b"Hello world");

            let mut w = w.start(&self.cols).await?;
            w.write_col(1024i16)?;
            w.finish().await
        }
    }

    db_test(TestingShim { params, cols }, |db| {
        let row = db
            .exec_iter("SELECT a FROM b WHERE c = ?", (b"Hello world",))
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(row.get::<i16, _>(0), Some(1024i16));
    })
}

#[test]
fn it_prepares_many() {
    let cols = vec![
        Column {
            table: String::new(),
            column: "a".to_owned(),
            coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
            colflags: myc::constants::ColumnFlags::empty(),
        },
        Column {
            table: String::new(),
            column: "b".to_owned(),
            coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
            colflags: myc::constants::ColumnFlags::empty(),
        },
    ];
    pub struct TestingShim {
        cols: Vec<Column>,
    }
    #[async_trait]
    impl MysqlShim for TestingShim {
        type Error = io::Error;

        async fn on_prepare(&mut self, q: &str, info: StatementMetaWriter<'_>) -> io::Result<()> {
            assert_eq!(q, "SELECT a, b FROM x");
            info.reply(41, &[], &self.cols)
        }
        async fn on_execute(
            &mut self,
            stmt: u32,
            params: ParamParser<'_>,
            w: QueryResultWriter<'_>,
        ) -> io::Result<()> {
            let params: Vec<msql_srv::ParamValue> = params.into_iter().collect();
            assert_eq!(stmt, 41);
            assert_eq!(params.len(), 0);

            let mut w = w.start(&self.cols).await?;
            w.write_col(1024i16)?;
            w.write_col(1025i16)?;
            w.end_row()?;
            w.write_row(&[1024i16, 1025i16])?;
            w.finish().await
        }
    }
    db_test(TestingShim { cols }, |db| {
        let mut rows = 0;
        for row in db.exec_iter("SELECT a, b FROM x", ()).unwrap() {
            let row = row.unwrap();
            assert_eq!(row.get::<i16, _>(0), Some(1024));
            assert_eq!(row.get::<i16, _>(1), Some(1025));
            rows += 1;
        }
        assert_eq!(rows, 2);
    });
}

#[test]
fn prepared_empty() {
    let cols = vec![Column {
        table: String::new(),
        column: "a".to_owned(),
        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
        colflags: myc::constants::ColumnFlags::empty(),
    }];
    let params = vec![Column {
        table: String::new(),
        column: "c".to_owned(),
        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
        colflags: myc::constants::ColumnFlags::empty(),
    }];
    pub struct TestingShim {
        cols: Vec<Column>,
        params: Vec<Column>,
    }
    #[async_trait]
    impl MysqlShim for TestingShim {
        type Error = io::Error;

        async fn on_prepare(&mut self, _: &str, info: StatementMetaWriter<'_>) -> io::Result<()> {
            info.reply(0, &self.params, &self.cols)
        }
        async fn on_execute(
            &mut self,
            _: u32,
            params: ParamParser<'_>,
            w: QueryResultWriter<'_>,
        ) -> io::Result<()> {
            let params: Vec<msql_srv::ParamValue> = params.into_iter().collect();
            assert!(!params.is_empty());
            w.completed(0, 0).await
        }
    }
    db_test(TestingShim { params, cols }, |db| {
        assert_eq!(
            db.exec_iter("SELECT a FROM b WHERE c = ?", (42i16,))
                .unwrap()
                .count(),
            0
        );
    });
}

#[test]
fn prepared_no_params() {
    let cols = vec![Column {
        table: String::new(),
        column: "a".to_owned(),
        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
        colflags: myc::constants::ColumnFlags::empty(),
    }];
    let params = vec![];
    pub struct TestingShim {
        cols: Vec<Column>,
        params: Vec<Column>,
    }
    #[async_trait]
    impl MysqlShim for TestingShim {
        type Error = io::Error;

        async fn on_prepare(&mut self, _: &str, info: StatementMetaWriter<'_>) -> io::Result<()> {
            info.reply(0, &self.params, &self.cols)
        }
        async fn on_execute(
            &mut self,
            _: u32,
            params: ParamParser<'_>,
            w: QueryResultWriter<'_>,
        ) -> io::Result<()> {
            let params: Vec<msql_srv::ParamValue> = params.into_iter().collect();
            assert!(params.is_empty());
            let mut w = w.start(&self.cols).await?;
            w.write_col(1024i16)?;
            w.finish().await
        }
    }
    db_test(TestingShim { params, cols }, |db| {
        let row = db.exec_iter("foo", ()).unwrap().next().unwrap().unwrap();
        assert_eq!(row.get::<i16, _>(0), Some(1024i16));
    });
}

#[test]
fn prepared_nulls() {
    let cols = vec![
        Column {
            table: String::new(),
            column: "a".to_owned(),
            coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
            colflags: myc::constants::ColumnFlags::empty(),
        },
        Column {
            table: String::new(),
            column: "b".to_owned(),
            coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
            colflags: myc::constants::ColumnFlags::empty(),
        },
    ];
    let params = vec![
        Column {
            table: String::new(),
            column: "c".to_owned(),
            coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
            colflags: myc::constants::ColumnFlags::empty(),
        },
        Column {
            table: String::new(),
            column: "d".to_owned(),
            coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
            colflags: myc::constants::ColumnFlags::empty(),
        },
    ];
    pub struct TestingShim {
        cols: Vec<Column>,
        params: Vec<Column>,
    }
    #[async_trait]
    impl MysqlShim for TestingShim {
        type Error = io::Error;

        async fn on_prepare(&mut self, _: &str, info: StatementMetaWriter<'_>) -> io::Result<()> {
            info.reply(0, &self.params, &self.cols)
        }
        async fn on_execute(
            &mut self,
            _: u32,
            params: ParamParser<'_>,
            w: QueryResultWriter<'_>,
        ) -> io::Result<()> {
            let params: Vec<msql_srv::ParamValue> = params.into_iter().collect();
            assert_eq!(params.len(), 2);
            assert!(params[0].value.is_null());
            assert!(!params[1].value.is_null());
            assert_eq!(
                params[0].coltype,
                myc::constants::ColumnType::MYSQL_TYPE_NULL
            );
            // rust-mysql sends all numbers as LONGLONG :'(
            assert_eq!(
                params[1].coltype,
                myc::constants::ColumnType::MYSQL_TYPE_LONGLONG
            );
            assert_eq!(Into::<i8>::into(params[1].value), 42i8);

            let mut w = w.start(&self.cols).await?;
            w.write_row(vec![None::<i16>, Some(42)])?;
            w.finish().await
        }
    }
    db_test(TestingShim { params, cols }, |db| {
        let row = db
            .exec_iter(
                "SELECT a, b FROM x WHERE c = ? AND d = ?",
                (mysql::Value::NULL, 42),
            )
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(row.as_ref(0), Some(&mysql::Value::NULL));
        assert_eq!(row.get::<i16, _>(1), Some(42));
    });
}

#[test]
fn prepared_no_rows() {
    let cols = vec![Column {
        table: String::new(),
        column: "a".to_owned(),
        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
        colflags: myc::constants::ColumnFlags::empty(),
    }];
    let params = vec![];
    pub struct TestingShim {
        cols: Vec<Column>,
        params: Vec<Column>,
    }
    #[async_trait]
    impl MysqlShim for TestingShim {
        type Error = io::Error;

        async fn on_prepare(&mut self, _: &str, info: StatementMetaWriter<'_>) -> io::Result<()> {
            info.reply(0, &self.params, &self.cols)
        }
        async fn on_execute(
            &mut self,
            _: u32,
            _: ParamParser<'_>,
            w: QueryResultWriter<'_>,
        ) -> io::Result<()> {
            w.start(&self.cols).await?.finish().await
        }
    }
    db_test(TestingShim { params, cols }, |db| {
        assert_eq!(db.exec_iter("SELECT a, b FROM foo", ()).unwrap().count(), 0);
    })
}

#[test]
fn prepared_no_cols_but_rows() {
    pub struct TestingShim {}
    #[async_trait]
    impl MysqlShim for TestingShim {
        type Error = io::Error;

        async fn on_prepare(&mut self, _: &str, info: StatementMetaWriter<'_>) -> io::Result<()> {
            info.reply(0, &[], &[])
        }
        async fn on_execute(
            &mut self,
            _: u32,
            _: ParamParser<'_>,
            w: QueryResultWriter<'_>,
        ) -> io::Result<()> {
            let mut writer = w.start(&[]).await?;
            writer.write_col(42)?;
            Ok(())
        }
    }
    db_test(TestingShim {}, |db| {
        assert_eq!(db.exec_iter("SELECT a, b FROM foo", ()).unwrap().count(), 0);
    })
}

#[test]
fn prepared_no_cols() {
    pub struct TestingShim {}
    #[async_trait]
    impl MysqlShim for TestingShim {
        type Error = io::Error;

        async fn on_prepare(&mut self, _: &str, info: StatementMetaWriter<'_>) -> io::Result<()> {
            info.reply(0, &[], &[])
        }
        async fn on_execute(
            &mut self,
            _: u32,
            _: ParamParser<'_>,
            w: QueryResultWriter<'_>,
        ) -> io::Result<()> {
            let writer = w.start(&[]).await?;
            writer.finish().await
        }
    }
    db_test(TestingShim {}, |db| {
        assert_eq!(db.exec_iter("SELECT a, b FROM foo", ()).unwrap().count(), 0);
    })
}

#[test]
fn really_long_query() {
    pub struct TestingShim {
        long: String,
    }
    let long: &'static str= "CREATE TABLE `stories` (`id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, `always_null` int, `created_at` datetime, `user_id` int unsigned, `url` varchar(250) DEFAULT '', `title` varchar(150) DEFAULT '' NOT NULL, `description` mediumtext, `short_id` varchar(6) DEFAULT '' NOT NULL, `is_expired` tinyint(1) DEFAULT 0 NOT NULL, `is_moderated` tinyint(1) DEFAULT 0 NOT NULL, `markeddown_description` mediumtext, `story_cache` mediumtext, `merged_story_id` int, `unavailable_at` datetime, `twitter_id` varchar(20), `user_is_author` tinyint(1) DEFAULT 0,  INDEX `index_stories_on_created_at`  (`created_at`), fulltext INDEX `index_stories_on_description`  (`description`),   INDEX `is_idxes`  (`is_expired`, `is_moderated`),  INDEX `index_stories_on_is_expired`  (`is_expired`),  INDEX `index_stories_on_is_moderated`  (`is_moderated`),  INDEX `index_stories_on_merged_story_id`  (`merged_story_id`), UNIQUE INDEX `unique_short_id`  (`short_id`), fulltext INDEX `index_stories_on_story_cache`  (`story_cache`), fulltext INDEX `index_stories_on_title`  (`title`),  INDEX `index_stories_on_twitter_id`  (`twitter_id`),  INDEX `url`  (`url`(191)),  INDEX `index_stories_on_user_id`  (`user_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;";
    #[async_trait]
    impl MysqlShim for TestingShim {
        type Error = io::Error;
        async fn on_query(&mut self, q: &str, w: QueryResultWriter<'_>) -> Result<(), Self::Error> {
            {
                assert_eq!(q.to_string(), self.long);
            }
            w.start(&[]).await?.finish().await
        }

        async fn on_prepare(&mut self, _: &str, info: StatementMetaWriter<'_>) -> io::Result<()> {
            info.reply(0, &[], &[])
        }
    }

    db_test(
        TestingShim {
            long: long.to_string(),
        },
        move |db| {
            db.query_iter(long).unwrap();
        },
    )
}
