import sys

import rlp
from cfx_account._utils.transactions.legacy_transactions import serializable_unsigned_transaction_from_dict
from eth_utils import decode_hex

from post_quantom_consensus_proof import verify_dilithium_signature
from test_framework.util import get_simple_rpc_proxy, assert_equal
from test_framework.blocktools import encode_hex_0x
from conflux_web3 import Web3

def main():
    tx_hash = sys.argv[1]
    pub_key = sys.argv[2]
    client = get_simple_rpc_proxy("http://localhost:15025")
    tx = client.cfx_getTransactionByHash(tx_hash)
    print(f"Read transaction from the blockchain node: tx={tx}")
    sign_info = tx["signInfo"]["Quantum"]
    assert_equal(encode_hex_0x(bytes(sign_info["public_key"])), pub_key)
    signature = encode_hex_0x(bytes(sign_info["signed_msg"]))
    print(f"Signature found: signature={signature}")
    remove_list = ['blockHash', 'contractCreated', 'from', 'hash', 'signInfo', 'status', 'transactionIndex', 'r', 's', 'v']
    for item in remove_list:
        del tx[item]
    int_list = ["nonce", "gasPrice", "value", "storageLimit", "gas", "epochHeight", "chainId"]
    for item in int_list:
        tx[item] = int(tx[item], 16)
    # tx["to"] = Web3.to_checksum_address(b32_address_to_hex(tx["to"]))
    tx["data"] = decode_hex(tx["data"])
    unsigned_tx = serializable_unsigned_transaction_from_dict(tx)
    sign_msg = rlp.encode(unsigned_tx)
    print(f"Encode the transaction for verification, sign_msg={sign_msg}")
    verify_dilithium_signature(client, encode_hex_0x(sign_msg), signature, pub_key)


if __name__ == '__main__':
    main()