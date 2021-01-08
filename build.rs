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

fn main() {
    tonic_build::configure()
        .build_client(false)
        .build_server(true)
        .format(true)
        .compile(
            &[
                "composer.proto",
                "common/common.proto",
                "msp/identities.proto",
                "peer/chaincode.proto",
                "peer/chaincode_shim.proto",
                "ledger/rwset/kvrwset/kv_rwset.proto",
                "ledger/queryresult/kv_query_result.proto",
            ],
            &["protos", "protos/fabric-protos"],
        )
        .unwrap_or_else(|e| panic!("{}", e));
}
