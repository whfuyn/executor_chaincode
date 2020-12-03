fn main() {
    tonic_build::configure()
        .build_client(false)
        .build_server(true)
        .format(true)
        .compile(
            &[
                "fabric-protos/common/common.proto",
                "fabric-protos/msp/identities.proto",
                "fabric-protos/peer/chaincode.proto",
                "fabric-protos/peer/chaincode_shim.proto",
                "fabric-protos/ledger/rwset/kvrwset/kv_rwset.proto",
                "fabric-protos/ledger/queryresult/kv_query_result.proto",
            ],
            &["fabric-protos"],
        )
        .unwrap_or_else(|e| panic!("Failed to compile protos {:?}", e));
}
