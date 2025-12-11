#!/usr/bin/env python3
# coding: utf-8
import array
import datetime
import struct
import time
import os
import types
import shutil
import sys

from eth_utils import decode_hex

from conflux.rpc import RpcClient
from test_framework.test_framework import DefaultConfluxTestFramework
from test_framework.util import assert_equal

sys.path.insert(1, "../../cfx-account")
from cfx_account.account import (
    Account
)
from cfx_account._utils.account_ffi import Pffi

PRECOMPILE_ADDRESS = "0x00000000000000000000000000000000000003E9"

class SignTest(DefaultConfluxTestFramework):
    def set_test_params(self):
        self.num_nodes = 2

    def run_test(self):
        self.client = RpcClient(self.nodes[0])
        if self.options.test_name == "valid":
            self.test_valid_signature()
        elif self.options.test_name == "invalid":
            self.test_invalid_signature()

    def encode_input_data(self, message, signed_message, public_key):
        """
        Encode input data according to your Rust implementation format:
        [message_len(2 bytes)][secret_message_len(2 bytes)][public_key_len(2 bytes)][message][signed_message][public_key]
        """
        message_len = len(message)
        signed_message_len = len(signed_message)
        public_key_len = len(public_key)

        # Pack lengths as little-endian 16-bit integers
        header = struct.pack('<HHH', message_len, signed_message_len, public_key_len)

        # Concatenate all data
        input_data = header + message + signed_message + public_key

        return input_data.hex()

    def generate_test_dilithium_data(self):
        """
        Generate test Dilithium2 data for testing
        """
        pk, sk = Account.get_key_pair_post_quantum()
        message = b"Hello, post-quantum world!"
        sm = sign_dilithium(message, pk, sk)
        return message, bytes(sm), decode_hex(pk)

    def test_valid_signature(self):
        """
        测试有效的签名（应返回0）
        """
        # 打印测试标题
        print("\n=== Testing Valid Signature ===")

        # 生成用于测试的Dilithium数据
        message, signed_message, public_key = self.generate_test_dilithium_data()
        # 编码输入数据
        input_data = self.encode_input_data(message, signed_message, public_key)
        # 发送交易并获取结果
        result = self.send_transaction(input_data)
        print(f"Transaction execution result: {result}")
        # 断言结果应为0
        assert_equal(result, "0x00")
        print("Valid signature test passed")


    def test_invalid_signature(self):
        """
        测试无效的签名（应返回1）
        """
        # 打印测试标题
        print("\n=== Testing Invalid Signature ===")

        # 生成用于测试的Dilithium数据
        message, signed_message, public_key = self.generate_test_dilithium_data()
        # 修改签名使其无效
        signed_message = signed_message[:-10] + b"corrupted!"
        # 编码输入数据
        input_data = self.encode_input_data(message, signed_message, public_key)
        # 发送交易并获取结果
        result = self.send_transaction(input_data)
        print(f"Transaction execution result: {result}")
        # 断言结果应为1
        assert_equal(result, "0x01")
        print("Invalid signature test passed")

    def send_transaction(self, input_data):
        tx_hash = self.client.send_tx(self.client.new_contract_tx(receiver=PRECOMPILE_ADDRESS, data_hex=input_data),
                                      wait_for_receipt=True)
        receipt = self.client.get_transaction_receipt(tx_hash)
        if receipt["outcomeStatus"] == "0x0":
            # Call the precompile to get return value
            result = self.client.call(
                PRECOMPILE_ADDRESS,
                "0x" + input_data,
            )
            return result
        else:
            return None


def sign_dilithium(data: bytes, public_key_hex: str, secret_key_hex: str):
    data_array = list(data)
    # print("rlp.encode(unsigned_transaction):", list(rlp.encode(unsigned_transaction)))

    public_key_bytes = bytes.fromhex(public_key_hex)
    public_key_array = array.array('B', public_key_bytes).tolist()

    secret_key_bytes = bytes.fromhex(secret_key_hex)
    secret_key_array = array.array('B', secret_key_bytes).tolist()


    pffi = Pffi()
    ffi = pffi.get_ffi()
    rust_lib = pffi.get_rust_lib()

    transaction_array_ptr = ffi.new("uint8_t[]", data_array)
    secret_key_array_ptr = ffi.new("uint8_t[]", secret_key_array)
    # void pqcrystals_dilithium2_ref(uint8_t* sm, size_t smlen, const uint8_t* m, size_t mlen, uint8_t* sk);

    smlen = len(data_array) + pffi.CRYPTO_BYTES
    mlen = len(data_array)
    sklen = pffi.CRYPTO_SECRETKEYBYTES
    pklen = pffi.CRYPTO_PUBLICKEYBYTES

    smlen_ptr = ffi.new("size_t *")
    sm_buffer = ffi.new("uint8_t[]", smlen)  # sm 缓冲区
    m_buffer = ffi.new("uint8_t[]", mlen)    # m 缓冲区
    sk_buffer = ffi.new("uint8_t[]", sklen)  # sk 缓冲区

    ffi.memmove(m_buffer, transaction_array_ptr, mlen)
    ffi.memmove(sk_buffer, secret_key_array_ptr, sklen)


    rust_lib.pqcrystals_dilithium2_ref(sm_buffer, smlen_ptr, m_buffer, mlen, sk_buffer)

    sm_array = ffi.unpack(sm_buffer, smlen)
    # print("sm_array:", sm_array)
    return sm_array

if __name__ == "__main__":
    SignTest().main()
