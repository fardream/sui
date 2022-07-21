// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use multiaddr::Multiaddr;
use std::time::Duration;
use sui_config::ValidatorInfo;
use sui_core::authority_client::AuthorityAPI;
use sui_core::safe_client::SafeClient;
use sui_node::SuiNode;
use sui_types::base_types::{ObjectID, ObjectRef};
use sui_types::crypto::{get_key_pair, AuthorityKeyPair, KeypairTraits};
use sui_types::error::SuiResult;
use sui_types::messages::ObjectInfoResponse;
use sui_types::messages::{CallArg, ObjectArg, ObjectInfoRequest, TransactionEffects};
use sui_types::object::Object;
use sui_types::SUI_SYSTEM_STATE_OBJECT_ID;
use test_utils::authority::test_authority_configs;
use test_utils::messages::move_transaction;
use test_utils::objects::{generate_gas_object_with_balance, test_gas_objects};
use test_utils::transaction::submit_shared_object_transaction;

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn reconfig_end_to_end_tests() {
    telemetry_subscribers::init_for_testing();

    let mut configs = test_authority_configs();
    for c in configs.validator_configs.iter_mut() {
        c.enable_gossip = true;
        c.enable_reconfig = true;
    }
    let validator_info = configs.validator_set();
    let mut gas_objects = test_gas_objects();
    let validator_stake = generate_gas_object_with_balance(100000000000000);
    let mut states = Vec::new();
    let mut nodes = Vec::new();
    for validator in configs.validator_configs() {
        let node = SuiNode::start(validator).await.unwrap();
        let state = node.state();

        for gas in gas_objects.clone() {
            state.insert_genesis_object(gas).await;
        }
        state.insert_genesis_object(validator_stake.clone()).await;
        states.push(state);
        nodes.push(node);
    }

    // get sui system state and confirm it matches network info
    let sui_system_state = states[0].get_sui_system_state_object().await.unwrap();
    let mut net_addrs_from_chain: Vec<Multiaddr> = Vec::new();
    let old_committee_size = sui_system_state.validators.next_epoch_validators.len();
    for validator in sui_system_state.validators.active_validators {
        let address = Multiaddr::try_from(validator.metadata.net_address);
        net_addrs_from_chain.push(address.unwrap());
    }
    let mut net_addrs_from_config = Vec::new();
    for validator in configs.validator_configs() {
        net_addrs_from_config.push(validator.network_address.clone());
    }
    assert_eq!(net_addrs_from_config.len(), net_addrs_from_chain.len());
    net_addrs_from_config.sort();
    net_addrs_from_chain.sort();
    for (conf, chain) in net_addrs_from_config.iter().zip(&net_addrs_from_chain) {
        assert_eq!(conf, chain);
    }

    let effects = create_and_register_new_validator(
        states[0].get_framework_object_ref().await.unwrap(),
        &mut gas_objects,
        validator_stake.compute_object_reference(),
        validator_info,
    )
    .await
    .unwrap();
    assert!(effects.status.is_ok());
    let sui_system_state = states[0].get_sui_system_state_object().await.unwrap();
    let new_committee_size = sui_system_state.validators.next_epoch_validators.len();
    assert_eq!(old_committee_size + 1, new_committee_size);

    let results: Vec<_> = nodes
        .iter()
        .map(|node| async {
            // Just wait for the checkpoint driver to trigger and finish epoch changes.
            while node.state().epoch() == 0 {
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
        })
        .collect();

    futures::future::join_all(results).await;

    // refresh the system state and network addresses
    let sui_system_state = states[0].get_sui_system_state_object().await.unwrap();
    assert_eq!(sui_system_state.epoch, 1);
    // We should now have one more active validator.
    assert_eq!(sui_system_state.validators.active_validators.len(), 5);
}

pub async fn create_and_register_new_validator(
    framework_pkg: ObjectRef,
    gas_objects: &mut Vec<Object>,
    validator_stake: ObjectRef,
    validator_info: &[ValidatorInfo],
) -> SuiResult<TransactionEffects> {
    let new_validator = get_new_validator();

    let validator_tx = move_transaction(
        gas_objects.pop().unwrap(),
        "sui_system",
        "request_add_validator",
        framework_pkg,
        vec![
            CallArg::Object(ObjectArg::SharedObject(SUI_SYSTEM_STATE_OBJECT_ID)),
            CallArg::Pure(bcs::to_bytes(&new_validator.public_key()).unwrap()),
            CallArg::Pure(
                bcs::to_bytes(format!("Validator{}", new_validator.sui_address()).as_bytes())
                    .unwrap(),
            ),
            CallArg::Pure(bcs::to_bytes(&new_validator.network_address).unwrap()),
            CallArg::Object(ObjectArg::ImmOrOwnedObject(validator_stake)),
        ],
    );
    submit_shared_object_transaction(validator_tx, validator_info).await
}

pub fn get_new_validator() -> ValidatorInfo {
    let keypair: AuthorityKeyPair = get_key_pair().1;
    ValidatorInfo {
        name: "".to_string(),
        public_key: keypair.public().into(),
        stake: 1,
        delegation: 0,
        network_address: sui_config::utils::new_network_address(),
        narwhal_primary_to_primary: sui_config::utils::new_network_address(),
        narwhal_worker_to_primary: sui_config::utils::new_network_address(),
        narwhal_primary_to_worker: sui_config::utils::new_network_address(),
        narwhal_worker_to_worker: sui_config::utils::new_network_address(),
        narwhal_consensus_address: sui_config::utils::new_network_address(),
    }
}

#[allow(dead_code)]
pub async fn get_latest_ref<A>(authority: &SafeClient<A>, object_id: ObjectID) -> ObjectRef
where
    A: AuthorityAPI + Send + Sync + Clone + 'static,
{
    if let Ok(ObjectInfoResponse {
        requested_object_reference: Some(object_ref),
        ..
    }) = authority
        .handle_object_info_request(ObjectInfoRequest::latest_object_info_request(
            object_id, None,
        ))
        .await
    {
        return object_ref;
    }
    panic!("Object not found!");
}
