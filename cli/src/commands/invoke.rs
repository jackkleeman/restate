// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;

use anyhow::{Context, Result};
use cling::prelude::*;
use serde::Serialize;

use crate::{c_println, cli_env::CliEnv, clients::IngressClient};

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_invoke")]
pub struct Invoke {
    /// Send the invocation asynchronously
    #[clap(long)]
    send: bool,
    /// Delay the invocation for this number of seconds; implies 'send'
    #[clap(long)]
    delay: Option<u64>,
    // The target to invoke, in format MyService/myHandler or MyVirtualObject/myObjectKey/myHandler
    target: InvocationTarget,
    /// The JSON body to send.
    #[clap(short, long)]
    data: Option<JsonArgument>,
}

#[derive(Clone, Debug, Serialize)]
struct JsonArgument(serde_json::Value);

impl FromStr for JsonArgument {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> std::prelude::v1::Result<Self, Self::Err> {
        Ok(Self(
            serde_json::from_str(s).context("data must be valid JSON")?,
        ))
    }
}

#[derive(Clone)]
pub enum InvocationTarget {
    Service {
        service: String,
        method: String,
    },
    Object {
        service: String,
        key: String,
        method: String,
    },
}

impl FromStr for InvocationTarget {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> std::prelude::v1::Result<Self, Self::Err> {
        let parts: Vec<_> = s.splitn(4, '/').collect();
        match parts.len() {
            2 => Ok(InvocationTarget::Service { service: parts[0].into(), method: parts[1].into() }),
            3 => Ok(InvocationTarget::Object { service: parts[0].into(), key: parts[1].into(), method: parts[2].into() }),
            _ => Err(anyhow::anyhow!(
                "Invalid invocation target; expected MyService/myHandler or MyVirtualObject/myObjectKey/myHandler"
            )),
        }
    }
}

pub async fn run_invoke(State(env): State<CliEnv>, opts: &Invoke) -> Result<()> {
    let client = IngressClient::new(&env)?;

    let path = match &opts.target {
        InvocationTarget::Object {
            service,
            key,
            method,
        } => format!("/{service}/{key}/{method}/"),
        InvocationTarget::Service { service, method } => format!("/{service}/{method}/"),
    };

    let url = client.base_url.join(&path)?;

    let url = match (opts.delay, opts.send) {
        (Some(delay), _) => {
            let mut url = url.join("send")?;
            url.query_pairs_mut()
                .append_pair("delaySec", &delay.to_string());
            url
        }
        (None, true) => url.join("send")?,
        (None, false) => url,
    };

    let result: serde_json::Value = if let Some(body) = &opts.data {
        client.run_with_body(url, body).await?
    } else {
        client.run(url).await?
    }
    .into_body()
    .await?;

    c_println!("{}", serde_json::to_string_pretty(&result)?);

    Ok(())
}
