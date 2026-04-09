use diesel::dsl::copy_from;
use diesel::prelude::*;
use diesel_async::pg::copy::sync_compat::AsyncExecuteCopyFromDsl;
use diesel_async::pg::copy::{AsyncCopyFromExpression, ExecuteAsyncCopyFrom};
use diesel_async::RunQueryDsl;

use super::{connection, users};

#[derive(Insertable)]
#[diesel(table_name = users)]
#[diesel(treat_none_as_default_value = false)]
struct NewUser<'a> {
    name: &'a str,
}

#[tokio::test]
async fn copy_from_raw_data() {
    let mut conn = connection().await;

    let result = AsyncExecuteCopyFromDsl::execute(
        copy_from(users::table).from_raw_data((users::name,), |writer: &mut dyn std::io::Write| {
            writeln!(writer, "Alice").unwrap();
            writeln!(writer, "Bob").unwrap();
            Ok::<(), diesel::result::Error>(())
        }),
        &mut conn,
    )
    .await;

    assert_eq!(result.unwrap(), 2, "should have copied 2 rows");

    let names: Vec<String> = users::table
        .select(users::name)
        .order(users::name.asc())
        .load(&mut conn)
        .await
        .unwrap();

    assert_eq!(names, vec!["Alice", "Bob"]);
}

#[tokio::test]
async fn copy_from_insertable() {
    let mut conn = connection().await;

    let new_users = vec![
        NewUser { name: "Charlie" },
        NewUser { name: "Diana" },
        NewUser { name: "Eve" },
    ];

    let result = AsyncExecuteCopyFromDsl::execute(
        copy_from(users::table).from_insertable(new_users),
        &mut conn,
    )
    .await;

    assert_eq!(result.unwrap(), 3, "should have copied 3 rows");

    let names: Vec<String> = users::table
        .select(users::name)
        .order(users::name.asc())
        .load(&mut conn)
        .await
        .unwrap();

    assert_eq!(names, vec!["Charlie", "Diana", "Eve"]);
}

/// A simple async streaming COPY FROM source that yields CSV lines as chunks.
struct AsyncCsvCopyFrom {
    lines: Vec<String>,
}

impl AsyncCopyFromExpression for AsyncCsvCopyFrom {
    fn stream<'a>(&'a mut self) -> futures_core::stream::BoxStream<'a, diesel::QueryResult<bytes::Bytes>> {
        use futures_util::stream::{self, StreamExt};
        stream::iter(
            self.lines
                .drain(..)
                .map(|line| Ok(bytes::Bytes::from(line)))
        )
        .boxed()
    }

    fn walk_target<'b>(
        &'b self,
        mut pass: diesel::query_builder::AstPass<'_, 'b, diesel::pg::Pg>,
    ) -> diesel::QueryResult<()> {
        use diesel::pg::CopyTarget;
        <(users::name,) as CopyTarget>::walk_target(pass.reborrow())
    }

    fn options_ast<'b>(
        &'b self,
        _pass: diesel::query_builder::AstPass<'_, 'b, diesel::pg::Pg>,
    ) -> diesel::QueryResult<()> {
        // No special options, uses default text format
        Ok(())
    }
}

#[tokio::test]
async fn copy_from_async_stream() {
    let mut conn = connection().await;

    let source = AsyncCsvCopyFrom {
        lines: vec!["Frank\n".into(), "Grace\n".into()],
    };

    let result = source.execute_async_copy_from(&mut conn).await;

    assert_eq!(result.unwrap(), 2, "should have copied 2 rows");

    let names: Vec<String> = users::table
        .select(users::name)
        .order(users::name.asc())
        .load(&mut conn)
        .await
        .unwrap();

    assert_eq!(names, vec!["Frank", "Grace"]);
}
