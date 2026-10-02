//! Maps reth's error messages to the exception names that fixtures expect.
//!
//! The table is a copy of `RethExceptionMapper` in EEST, the mapping its `consume` command uses
//! for reth: `packages/testing/src/execution_testing/client_clis/clis/reth.py` in
//! <https://github.com/ethereum/execution-specs>, at commit
//! `847cdbdb7e0130132dbbc987ffa3a9d31ca3ade5`. Keep the two in sync.

use regex::Regex;
use std::sync::LazyLock;

/// How an exception is recognized in an error message.
#[derive(Debug, Clone, Copy)]
enum Pattern {
    /// The message contains this text (EEST's `mapping_substring`).
    Substring(&'static str),
    /// This regex matches somewhere in the message (EEST's `mapping_regex`).
    Regex(&'static str),
}

use Pattern::{Regex as R, Substring as S};

const MAPPING: &[(&str, Pattern)] = &[
    // mapping_substring
    (
        "TransactionException.SENDER_NOT_EOA",
        S("reject transactions from senders with deployed code"),
    ),
    ("TransactionException.INSUFFICIENT_ACCOUNT_FUNDS", S("lack of funds")),
    ("TransactionException.INITCODE_SIZE_EXCEEDED", S("create initcode size limit")),
    ("TransactionException.INSUFFICIENT_MAX_FEE_PER_GAS", S("gas price is less than basefee")),
    (
        "TransactionException.PRIORITY_GREATER_THAN_MAX_FEE_PER_GAS",
        S("priority fee is greater than max fee"),
    ),
    ("TransactionException.GASLIMIT_PRICE_PRODUCT_OVERFLOW", S("overflow")),
    ("TransactionException.NONCE_IS_MAX", S("nonce overflow in transaction")),
    ("TransactionException.TYPE_3_TX_CONTRACT_CREATION", S("unexpected length")),
    ("TransactionException.TYPE_3_TX_WITH_FULL_BLOBS", S("unexpected list")),
    ("TransactionException.INVALID_CHAINID", S("invalid chain ID")),
    ("TransactionException.TYPE_3_TX_INVALID_BLOB_VERSIONED_HASH", S("blob version not supported")),
    ("TransactionException.TYPE_3_TX_ZERO_BLOBS", S("empty blobs")),
    ("TransactionException.TYPE_4_EMPTY_AUTHORIZATION_LIST", S("empty authorization list")),
    ("TransactionException.TYPE_4_TX_CONTRACT_CREATION", S("unexpected length")),
    (
        "TransactionException.TYPE_4_TX_PRE_FORK",
        S("eip 7702 transactions present in pre-prague payload"),
    ),
    ("BlockException.INVALID_REQUESTS", S("mismatched block requests hash")),
    ("BlockException.INVALID_RECEIPTS_ROOT", S("receipt root mismatch")),
    ("BlockException.INVALID_STATE_ROOT", S("mismatched block state root")),
    ("BlockException.INVALID_BLOCK_HASH", S("block hash mismatch")),
    ("BlockException.INVALID_GAS_USED", S("block gas used mismatch")),
    ("BlockException.RLP_BLOCK_LIMIT_EXCEEDED", S("block is too large: ")),
    ("BlockException.INVALID_BASEFEE_PER_GAS", S("block base fee mismatch")),
    ("BlockException.EXTRA_DATA_TOO_BIG", S("invalid payload extra data")),
    ("BlockException.INVALID_LOG_BLOOM", S("header bloom filter mismatch")),
    // mapping_regex
    (
        "TransactionException.INVALID_SIGNATURE_VRS",
        R(r"invalid bool value, must be 0 or 1|Failed to recover the signer|Unexpected type flag"),
    ),
    ("TransactionException.NONCE_MISMATCH_TOO_LOW", R(r"nonce \d+ too low, expected \d+")),
    ("TransactionException.NONCE_MISMATCH_TOO_HIGH", R(r"nonce \d+ too high, expected \d+")),
    (
        "TransactionException.INSUFFICIENT_MAX_FEE_PER_BLOB_GAS",
        R(r"blob gas price \(\d+\) is greater than max fee per blob gas \(\d+\)"),
    ),
    (
        "TransactionException.INTRINSIC_GAS_TOO_LOW",
        R(concat!(
            r"call gas cost \(\d+\) exceeds the gas limit \(\d+\)|",
            r"gas floor \(\d+\) exceeds the gas limit \(\d+\)",
        )),
    ),
    (
        "TransactionException.INTRINSIC_GAS_BELOW_FLOOR_GAS_COST",
        R(r"gas floor \(\d+\) exceeds the gas limit \(\d+\)"),
    ),
    (
        "TransactionException.TYPE_3_TX_MAX_BLOB_GAS_ALLOWANCE_EXCEEDED",
        R(r"blob gas used \d+ exceeds maximum allowance \d+"),
    ),
    ("TransactionException.TYPE_3_TX_BLOB_COUNT_EXCEEDED", R(r"too many blobs, have \d+, max \d+")),
    (
        "TransactionException.TYPE_3_TX_PRE_FORK",
        R(r"blob transactions present in pre-cancun payload|empty blobs"),
    ),
    (
        "TransactionException.GAS_ALLOWANCE_EXCEEDED",
        R(concat!(
            r"transaction gas limit \w+ is more than blocks available gas \w+|",
            r"caller gas limit exceeds the block gas limit",
        )),
    ),
    (
        "TransactionException.GAS_LIMIT_EXCEEDS_MAXIMUM",
        R(r"transaction gas limit.*is greater than the cap"),
    ),
    ("BlockException.SYSTEM_CONTRACT_CALL_FAILED", R(r"failed to apply .* requests contract call")),
    (
        "BlockException.INCORRECT_BLOB_GAS_USED",
        R(r"blob gas used mismatch|blob gas used \d+ is not a multiple of blob gas per blob"),
    ),
    (
        "BlockException.INCORRECT_EXCESS_BLOB_GAS",
        R(r"excess blob gas \d+ is not a multiple of blob gas per blob|invalid excess blob gas"),
    ),
    (
        "BlockException.INVALID_GAS_USED_ABOVE_LIMIT",
        R(r"block used gas \(\d+\) is greater than gas limit \(\d+\)"),
    ),
    (
        "BlockException.INVALID_GASLIMIT",
        R(concat!(
            r"child gas_limit \d+ max .* is .*|",
            r"child gas_limit \d+ is below the max allowed decrease .*|",
            r"child gas limit \d+ is below the minimum allowed limit",
        )),
    ),
    (
        "BlockException.INVALID_BLOCK_TIMESTAMP_OLDER_THAN_PARENT",
        R(r"block timestamp \d+ is in the past compared to the parent timestamp \d+"),
    ),
    (
        "BlockException.INVALID_BLOCK_NUMBER",
        R(r"block number \d+ does not match parent block number \d+"),
    ),
    (
        "BlockException.GAS_USED_OVERFLOW",
        R(r"transaction gas limit \w+ is more than blocks available gas \w+"),
    ),
    ("BlockException.INVALID_BAL_HASH", R(r"block access list hash mismatch")),
    (
        "BlockException.INVALID_BLOCK_ACCESS_LIST",
        R(concat!(
            r"failed to decode block access list|",
            r"block access list hash mismatch|",
            r"BAL rejection: FinalHashMismatch|",
            r"Bal error: Account .* not found in BAL|",
            r"Bal error: Slot .* not found in BAL for account .*",
        )),
    ),
    (
        "BlockException.BLOCK_ACCESS_LIST_GAS_LIMIT_EXCEEDED",
        R(r"block access list item cost exceeds gas limit"),
    ),
    ("BlockException.SYSTEM_CONTRACT_EMPTY", R(r"system contract .* has no code")),
    (
        "BlockException.INCORRECT_BLOCK_FORMAT",
        R(r"block access list hash mismatch|BAL rejection: FinalHashMismatch"),
    ),
    // reth does not check the deposit log layout, so EEST maps this to the requests hash.
    (
        "BlockException.INVALID_DEPOSIT_EVENT_LAYOUT",
        R(r"failed to decode deposit requests from receipts|mismatched block requests hash"),
    ),
];

/// The table with each substring escaped into a regex.
static COMPILED: LazyLock<Vec<(&'static str, Regex)>> = LazyLock::new(|| {
    MAPPING
        .iter()
        .map(|&(name, pattern)| {
            let regex = match pattern {
                Pattern::Substring(text) => Regex::new(&regex::escape(text)),
                Pattern::Regex(regex) => Regex::new(regex),
            };
            (name, regex.expect("valid regex"))
        })
        .collect()
});

/// Returns the exception names that `message` maps to, empty if it maps to none.
pub fn exceptions_for(message: &str) -> Vec<&'static str> {
    COMPILED.iter().filter(|(_, regex)| regex.is_match(message)).map(|(name, _)| *name).collect()
}

/// Checks that `message`, the client's error, maps to one of the `|`-separated exception names in
/// `expected`. On a mismatch the error names both the expected exceptions and what the client
/// reported.
pub fn check_exception(expected: &str, message: &str) -> Result<(), String> {
    let actual = exceptions_for(message);
    if expected.split('|').map(str::trim).any(|name| actual.contains(&name)) {
        return Ok(())
    }
    let actual = if actual.is_empty() { "an unmapped error".to_string() } else { actual.join("|") };
    Err(format!("expected exception {expected}, got {actual}: {message}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_regex_compiles() {
        assert_eq!(COMPILED.len(), MAPPING.len());
    }

    #[test]
    fn any_listed_name_matches() {
        let message = "block access list hash mismatch: got 0x01, expected 0x02";
        assert!(check_exception("BlockException.INVALID_BLOCK_ACCESS_LIST", message).is_ok());
        assert!(check_exception(
            "BlockException.INVALID_GAS_USED|BlockException.INVALID_BAL_HASH",
            message
        )
        .is_ok());
    }

    #[test]
    fn wrong_reason_names_both() {
        let message = "block access list hash mismatch: got 0x01, expected 0x02";
        let err = check_exception("TransactionException.INSUFFICIENT_ACCOUNT_FUNDS", message)
            .unwrap_err();
        assert!(err.contains("TransactionException.INSUFFICIENT_ACCOUNT_FUNDS"), "{err}");
        assert!(err.contains("BlockException.INVALID_BLOCK_ACCESS_LIST"), "{err}");
        assert!(err.contains(message), "{err}");
    }

    #[test]
    fn unmapped_error_fails() {
        let err =
            check_exception("BlockException.INVALID_STATE_ROOT", "something else").unwrap_err();
        assert!(err.contains("an unmapped error"), "{err}");
    }
}
