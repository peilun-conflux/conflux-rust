#!/usr/bin/env python3
import sys

import rlp
from cfx_account._utils.transactions.legacy_transactions import serializable_unsigned_transaction_from_dict
from eth_utils import decode_hex

from post_quantom_consensus_proof import verify_dilithium_signature
from test_framework.util import get_simple_rpc_proxy, assert_equal
from test_framework.blocktools import encode_hex_0x
from conflux_web3 import Web3

def main():
    # 从命令行参数获取交易哈希和公钥
    tx_hash = sys.argv[1]
    pub_key = sys.argv[2]

    # 获取RPC客户端
    client = get_simple_rpc_proxy("http://localhost:15025")

    # 通过交易哈希获取交易信息
    tx = client.cfx_getTransactionByHash(tx_hash)
    print(f"读取区块链节点中的交易: tx={tx}")

    # 提取签名信息并验证公钥
    sign_info = tx["signInfo"]["Quantum"]
    assert_equal(encode_hex_0x(bytes(sign_info["public_key"])), pub_key)

    # 获取签名
    signature = encode_hex_0x(bytes(sign_info["signed_msg"]))
    print(f"找到签名: signature={signature}")

    # 移除不需要的字段
    remove_list = ['blockHash', 'contractCreated', 'from', 'hash', 'signInfo', 'status', 'transactionIndex', 'r', 's', 'v']
    for item in remove_list:
        del tx[item]
    int_list = ["nonce", "gasPrice", "value", "storageLimit", "gas", "epochHeight", "chainId"]
    for item in int_list:
        tx[item] = int(tx[item], 16)
    tx["data"] = decode_hex(tx["data"])

    # 创建可序列化的未签名交易
    unsigned_tx = serializable_unsigned_transaction_from_dict(tx)
    sign_msg = rlp.encode(unsigned_tx)
    print(f"编码交易以进行验证, sign_msg={sign_msg}")

    # 验证Dilithium签名
    verify_dilithium_signature(client, encode_hex_0x(sign_msg), signature, pub_key)
    print("测试成功")


if __name__ == '__main__':
    main()