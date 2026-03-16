//! True async streaming `COPY FROM` support.
//!
//! This module defines [`AsyncCopyFromExpression`], a trait for producing
//! `COPY FROM` data as an async stream of byte chunks rather than buffering
//! everything in memory.
//!
//! # Example
//!
//! ```rust,no_run
//! use diesel_async::pg::copy::AsyncCopyFromExpression;
//! use diesel::pg::{CopyTarget, Pg};
//! use diesel::query_builder::AstPass;
//! use diesel::QueryResult;
//! use bytes::Bytes;
//! use futures_core::stream::BoxStream;
//!
//! struct MyCsvStream {
//!     lines: Vec<String>,
//! }
//!
//! // Implement for your own use-case
//! // then call `execute_async_copy_from(my_stream, conn).await`
//! ```

use crate::run_query_dsl::methods::ExecuteDsl;
use crate::AsyncPgConnection;
use diesel::pg::{CopyTarget, Pg};
use diesel::query_builder::{AstPass, QueryBuilder, QueryFragment, QueryId};
use diesel::QueryResult;
use bytes::Bytes;
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::FutureExt;

/// An asynchronous counterpart to Diesel's `CopyFromExpression`.
///
/// Instead of a synchronous callback that blocks the thread while writing to
/// a `std::io::Write` buffer, this trait yields data as an async
/// [`Stream`](futures_core::Stream) of [`Bytes`] chunks. Each chunk is
/// forwarded to the database incrementally, keeping memory usage constant
/// regardless of dataset size.
///
/// # Implementors
///
/// You must implement three methods:
///
/// - **`stream()`** — returns a `BoxStream` of `QueryResult<Bytes>` chunks.
///   Each chunk will be sent to the PostgreSQL server via the `COPY` protocol.
///
/// - **`walk_target()`** — writes the target table/columns SQL fragment into
///   the AST. Typically delegates to `T::walk_target(pass)` where `T: CopyTarget`.
///
/// - **`options_ast()`** — writes any `COPY FROM` options (e.g. `WITH (FORMAT csv)`)
///   into the AST. Return `Ok(())` for no options.
pub trait AsyncCopyFromExpression: Send {
    /// Returns a stream of byte chunks to feed into `COPY FROM STDIN`.
    ///
    /// The stream should yield data in the format matching the `COPY` options
    /// (text, CSV, or binary). When the stream ends, the `COPY` command is
    /// finalized and the row count is returned.
    fn stream<'a>(&'a mut self) -> BoxStream<'a, QueryResult<Bytes>>;

    /// Writes the target table and column list into the SQL AST.
    ///
    /// For example, this should produce `my_table(col1, col2)`.
    fn walk_target<'b>(
        &'b self,
        pass: AstPass<'_, 'b, Pg>,
    ) -> QueryResult<()>;

    /// Writes any `COPY FROM` options into the SQL AST.
    ///
    /// For example, this might produce ` WITH (FORMAT csv, HEADER true)`.
    /// If no options are needed, simply return `Ok(())`.
    fn options_ast<'b>(
        &'b self,
        pass: AstPass<'_, 'b, Pg>,
    ) -> QueryResult<()>;
}

// Internal helper to produce the full `COPY ... FROM STDIN ...` SQL string
// from an AsyncCopyFromExpression.
struct AsyncCopyFromSqlHelper<'a, A: ?Sized> {
    action: &'a A,
}

impl<'a, A> QueryFragment<Pg> for AsyncCopyFromSqlHelper<'a, A>
where
    A: AsyncCopyFromExpression + ?Sized,
{
    fn walk_ast<'b>(&'b self, mut pass: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        pass.unsafe_to_cache_prepared();
        pass.push_sql("COPY ");
        self.action.walk_target(pass.reborrow())?;
        pass.push_sql(" FROM STDIN");
        self.action.options_ast(pass.reborrow())?;
        Ok(())
    }
}

impl<'a, A> QueryId for AsyncCopyFromSqlHelper<'a, A>
where
    A: AsyncCopyFromExpression + ?Sized,
{
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

/// Execute trait for async streaming `COPY FROM` expressions.
///
/// This is the async-streaming counterpart to
/// [`AsyncExecuteCopyFromDsl`](super::sync_compat::AsyncExecuteCopyFromDsl).
///
/// Unlike the sync-compat path, this does **not** buffer the entire payload in
/// memory. Instead it pumps chunks from the async stream directly into
/// `tokio-postgres`'s `CopyInSink`.
pub trait ExecuteAsyncCopyFrom {
    /// Execute the `COPY FROM` statement, streaming data to the database.
    fn execute_async_copy_from<'conn>(
        self,
        conn: &'conn mut AsyncPgConnection,
    ) -> BoxFuture<'conn, QueryResult<usize>>
    where
        Self: 'conn;
}

impl<A> ExecuteAsyncCopyFrom for A
where
    A: AsyncCopyFromExpression + Send,
{
    fn execute_async_copy_from<'conn>(
        mut self,
        conn: &'conn mut AsyncPgConnection,
    ) -> BoxFuture<'conn, QueryResult<usize>>
    where
        Self: 'conn,
    {
        // Build the SQL statement synchronously (it's just string manipulation).
        let helper = AsyncCopyFromSqlHelper { action: &self };
        let mut qb = diesel::pg::PgQueryBuilder::default();
        let sql = match QueryFragment::<Pg>::to_sql(&helper, &mut qb, &Pg) {
            Ok(_) => qb.finish(),
            Err(e) => return futures_util::future::err(e).boxed(),
        };

        async move {
            let sink = conn.copy_in(&sql).await?;

            use futures_util::SinkExt;
            use futures_util::StreamExt;
            tokio::pin!(sink);

            let mut stream = self.stream();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                sink.send(chunk)
                    .await
                    .map_err(|e| diesel::result::Error::SerializationError(Box::new(e) as _))?;
            }

            let rows = sink
                .finish()
                .await
                .map_err(|e| diesel::result::Error::SerializationError(Box::new(e) as _))?;

            rows.try_into()
                .map_err(|e| diesel::result::Error::DeserializationError(Box::new(e) as _))
        }
        .boxed()
    }
}
