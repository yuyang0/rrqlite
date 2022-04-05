use core_command::command::{ExecuteResult, QueryResult};
use reqwest::{Client, RequestBuilder, Url};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Empty {}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct URLParams {
    pub transaction: bool,
    pub timings: bool,
    pub redirect: bool,
    pub timeout: i64,
    pub freshness: i64,
    pub level: String,
}

pub struct RqliteClient {
    pub addr: String,
    pub inner: Client,
}

impl RqliteClient {
    /// Create a client with a leader node id and a node manager to get node
    /// address by node id.
    pub fn new(leader_addr: String) -> Self {
        Self {
            addr: leader_addr,
            inner: reqwest::Client::new(),
        }
    }

    async fn query_helper(
        &self,
        mut builder: RequestBuilder,
        params: &URLParams,
    ) -> std::result::Result<QueryResult, reqwest::Error> {
        if params.transaction {
            builder = builder.query(&[("transaction", "true")]);
        }
        if params.timings {
            builder = builder.query(&[("timings", "true")]);
        }
        if params.redirect {
            builder = builder.query(&[("redirect", "true")]);
        }
        if params.timeout > 0 {
            builder = builder.query(&[("timeout", params.timeout)])
        }
        if !params.freshness > 0 {
            builder = builder.query(&[("freshness", params.freshness)])
        }
        if !params.level.is_empty() {
            builder = builder.query(&[("level", &params.level)]);
        }
        let mut resp = builder.send().await?;
        resp = resp.error_for_status()?;

        let res = resp.json::<QueryResult>().await?;
        Ok(res)
    }

    pub async fn query<T: Serialize>(
        &self,
        stmt: &T,
        params: &URLParams,
    ) -> std::result::Result<QueryResult, reqwest::Error> {
        let url = format!("http://{}/db/query", self.addr);
        let builder = self.inner.post(url.clone()).json(stmt);
        self.query_helper(builder, params).await
    }

    pub async fn query_get(
        &self,
        stmt: &str,
        params: &URLParams,
    ) -> std::result::Result<QueryResult, reqwest::Error> {
        let mut url = Url::parse(&format!("http://{}/db/query", self.addr)).unwrap();
        url.query_pairs_mut().append_pair("q", stmt.trim());
        let builder = self.inner.get(url);
        self.query_helper(builder, params).await
    }

    pub async fn execute<T: Serialize>(
        &self,
        stmt: &T,
        params: &URLParams,
    ) -> std::result::Result<ExecuteResult, reqwest::Error> {
        let url = format!("http://{}/db/execute", self.addr);
        let mut builder = self.inner.post(url.clone()).json(stmt);
        if params.transaction {
            builder = builder.query(&[("transaction", "true")]);
        }
        if params.timings {
            builder = builder.query(&[("timings", "true")]);
        }
        if params.redirect {
            builder = builder.query(&[("redirect", "true")]);
        }
        if params.timeout > 0 {
            builder = builder.query(&[("timeout", params.timeout)])
        }
        let mut resp = builder.send().await?;
        resp = resp.error_for_status()?;

        let res = resp.json::<ExecuteResult>().await?;
        Ok(res)
    }
}
