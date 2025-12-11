#!/usr/bin/env python3
# coding: utf-8
import datetime
import time
import os
import types
import shutil
import sys

from hexbytes import HexBytes

from conflux.rpc import RpcClient
# from eth_utils import decode_hex
from test_framework import coverage

from conflux.messages import GetBlockHeaders, GET_BLOCK_HEADERS_RESPONSE
from test_framework.mininode import start_p2p_connection
from test_framework.test_framework import ConfluxTestFramework
from test_framework.util import assert_equal, connect_nodes, get_peer_addr, wait_until, WaitHandler, \
    initialize_datadir, PortMin, get_datadir_path, connect_sample_nodes, sync_blocks, assert_raises_rpc_error
from test_framework.blocktools import wait_for_initial_nonce_for_address, create_transaction
import random
sys.path.insert(1, "../../cfx-account")
from cfx_account.account import (
    Account
)
import threading
from cfx_address.utils import public_key_to_cfx_hex
import pprint
from conflux_web3 import Web3
from solcx import install_solc, compile_source

ACCOUNT_NUM = 20
TX_NUM_FOR_ACCOUNT = 50

class SignTest(ConfluxTestFramework): 
    def __init__(self):
        super().__init__()
        self.nonce_map = {}        

    def set_test_params(self):
        self.num_nodes = 2
        self.conf_parameters = {
            "executive_trace": "true",
            "public_rpc_apis": "\"cfx,debug,test,pubsub,trace\"",
            "mining_type": "'disable'",
        }
        self.conf_parameters["log_level"] = '"trace"'

    def run_test(self):
        if self.options.test_name == "ecdsa":
            self._test_sign()
        elif self.options.test_name == "post_quantum_valid":
            self._test_quantum_sign(True)
        elif self.options.test_name == "post_quantum_invalid":
            self._test_quantum_sign(False)

    def start_node(self, i, extra_args=None, phase_to_wait=["NormalSyncPhase"], wait_time=30, *args, **kwargs):
        node = self.nodes[i]
        node.start(extra_args, *args, **kwargs)
        node.wait_for_rpc_connection()
        node.wait_for_nodeid()
        if phase_to_wait is not None:
            node.wait_for_recovery(phase_to_wait, wait_time)

        if self.options.coveragedir is not None:
            coverage.write_all_rpc_commands(self.options.coveragedir, node.rpc)

    def setup_network(self):
        self.setup_nodes()

    def set_genesis_secrets(self):
        genesis_file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "conflux_sj/sign_secrets.txt")
        self.conf_parameters["genesis_secrets"] = f"\"{genesis_file_path}\""

    def start_network(self, node_count):
        self.nodes = []
        self.add_nodes(node_count)
        for node_index in range(node_count):
            self.set_genesis_secrets()
            initialize_datadir(self.options.tmpdir, node_index, PortMin.n, self.conf_parameters)
            self.start_node(node_index, phase_to_wait=None)
        connect_sample_nodes(self.nodes, self.log, sample=self.num_nodes - 1)
        sync_blocks(self.nodes)
        for node in self.nodes:
            node.wait_for_recovery(["NormalSyncPhase"], 30)

    def _test_quantum_sign(self, is_valid: bool):
        # 准备环境
        self.stop_nodes()

        # delete nodes' file
        for i in range(len(self.nodes)):
            datadir = get_datadir_path(self.options.tmpdir, i)
            shutil.rmtree(datadir)
        old_pos_files = ["initial_nodes.json", "genesis_file", "waypoint_config", "public_key"]
        for f in old_pos_files:
            os.remove(os.path.join(self.options.tmpdir, f))
        shutil.rmtree(os.path.join(self.options.tmpdir, "private_keys"))

        # generate accounts
        key_list = self.generate_quantum_accounts(ACCOUNT_NUM)

        # copy to sign_secrets.txt
        ps_keys_list = {}
        current_path = os.path.abspath(os.path.dirname(__file__))
        with open(current_path + '/conflux_sj/sign_secrets.txt', 'w') as file:
            for key in key_list:            
                file.write("quantum:" + key[0] + key[1])
                file.write('\n') 
                ps_keys_list[key[0]] = key[1]

        # start three new nodes and only one execute test method
        self.start_network(3)
        current_path = os.path.abspath(os.path.dirname(__file__))
        with open(current_path + "/conflux_sj/sign_secrets.txt", 'r') as file:
            lines = file.readlines()
        account_num = len(lines)
        address_list = {} 
        for i in range(0, account_num):
            line = lines[i].strip()
            if "quantum" not in line:
                continue
            # line = "0x" + line
            line = line.replace("quantum:", "")
            account = public_key_to_cfx_hex("0x" + line[0:40])
            address_list[account] = line[0:2624]
         
        # 选择一个地址进行交易签名
        for address, pub_key in address_list.items():
            pub_key = pub_key.replace("0x", "")
            if pub_key in ps_keys_list.keys():
                secrete_key = ps_keys_list[pub_key]
            else:
                secrete_key = ""

            self.log.info(f"Account public key: 0x{pub_key}")
            # 生成一个后量子算法签名的交易
            signed_tx = Account.sign_transaction_post_quantum(self.get_transaction(), pub_key, secrete_key)
            self.log.info(f"Transaction signed: {signed_tx}")
            client = RpcClient(self.nodes[0])
            if is_valid:
                # 合法签名测试，直接发送交易
                tx_hash = client.send_raw_tx(signed_tx.rawTransaction.hex())
                # 等待交易被确认
                client.wait_for_receipt(tx_hash)
                self.log.info(f"Transaction sent and committed, hash={tx_hash}")
                self.log.info("Please run the verification script...")
                # 保持节点运行，等待验证脚本进行查询
                time.sleep(100000)
            else:
                # 非法签名测试
                self.log.info("Modify the tx signature and submit")
                invalid_raw_tx = bytearray(signed_tx.rawTransaction)
                # 修改签名内容，使得签名非法
                invalid_raw_tx[10:20] = b"corrupted!"
                # 提交交易并确认报错
                assert_raises_rpc_error(None, None, client.send_raw_tx, HexBytes(invalid_raw_tx).hex())
                self.log.info("Modified tx cannot pass verification")
            break

    def generate_quantum_accounts(self, account_num):
        account_list = []
        for i in range(0, account_num):
            account_list.append(Account.get_key_pair_post_quantum())
        return account_list

    def get_transaction(self):
        transaction = {
            # 'from': '0x1b981f81568edd843dcb5b407ff0dd2e25618622'.lower(),
            'to': 'cfxtest:aak7fsws4u4yf38fk870218p1h3gxut3ku00u1k1da',
            'nonce': 0,
            'value': 1,
            'gas': 100000,
            'gasPrice': 1,
            'storageLimit': 100,
            'epochHeight': 100,
            'chainId': 10
        }
        return transaction

    def _test_sign(self):
        client = RpcClient(self.nodes[0])

        # 测试发送常规的ECDSA签名交易
        self.log.info("Test sending regular ECDSA signed transactions")
        # 创建一个交易
        tx = create_transaction()
        self.log.info(f"Transaction signed: {tx}")
        # 发送交易
        tx_hash = client.send_tx(tx)
        # 等待交易被确认
        client.wait_for_receipt(tx_hash)
        self.log.info(f"ECDSA signed transaction sent and committed, hash={tx_hash}")


if __name__ == "__main__":
    SignTest().main()
