// Copyright Rivtower Technologies LLC.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use clap::Clap;
use git_version::git_version;
use log::{debug, info};

const GIT_VERSION: &str = git_version!(
    args = ["--tags", "--always", "--dirty=-modified"],
    fallback = "unknown"
);
const GIT_HOMEPAGE: &str = "https://github.com/cita-cloud/executor_poc";

/// network service
#[derive(Clap)]
#[clap(version = "0.1.0", author = "Rivtower Technologies.")]
struct Opts {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

#[derive(Clap)]
enum SubCommand {
    /// print information from git
    #[clap(name = "git")]
    GitInfo,
    /// run this service
    #[clap(name = "run")]
    Run(RunOpts),
}

/// A subcommand for run
#[derive(Clap)]
struct RunOpts {
    /// Sets grpc port of this service.
    #[clap(short = 'p', long = "port", default_value = "50002")]
    grpc_port: String,
}

fn main() {
    ::std::env::set_var("RUST_BACKTRACE", "full");

    let opts: Opts = Opts::parse();

    match opts.subcmd {
        SubCommand::GitInfo => {
            println!("git version: {}", GIT_VERSION);
            println!("homepage: {}", GIT_HOMEPAGE);
        }
        SubCommand::Run(opts) => {
            // init log4rs
            log4rs::init_file("executor-log4rs.yaml", Default::default()).unwrap();
            info!("grpc port of this service: {}", opts.grpc_port);
            run(opts).unwrap();
        }
    }
}

mod chaincode;
mod executor;

mod common {
    tonic::include_proto!("common");
}

mod protos {
    tonic::include_proto!("protos");
}

mod queryresult {
    tonic::include_proto!("queryresult");
}

use executor::ChaincodeExecutor;

#[tokio::main]
async fn run(opts: RunOpts) -> Result<(), Box<dyn std::error::Error>> {
    let executor_addr = format!("127.0.0.1:{}", opts.grpc_port).parse()?;
    let chaincode_addr = "127.0.0.1:7052".parse().unwrap();
    ChaincodeExecutor::run(executor_addr, chaincode_addr).await;

    Ok(())
}
