import json
import os.path
import pickle
import random
import subprocess
import time
from threading import Thread

import eth_utils
import rlp
import siphash
from cfx_account import Account

from conflux.config import default_config
from conflux.rpc import RpcClient
from test_framework.test_framework import ConfluxTestFramework
from test_framework.util import assert_equal

collision_tx_file = "encoding_collision_tx.json"

class HashCollisionTest(ConfluxTestFramework):
    def set_test_params(self):
        self.num_nodes = 2

    def run_test(self):
        # send a tx to node 0
        tx = self.client.new_tx(priv_key=default_config["GENESIS_PRI_KEY_2"])
        if os.path.exists(collision_tx_file):
            with open(collision_tx_file, "r") as f:
                collision_txs = json.load(f)
        else:
            collision_txs = self.generate_collision_txs(tx)
            with open(collision_tx_file, "w") as f:
                json.dump(collision_txs, f)
        accounts, tx_list = collision_txs
        # setup accounts for collision txs
        for account in accounts:
            self.cfx_transfer(account, value=100000)
        print("finish setup")

        Thread(target=async_send_txs, args=(self.client, tx_list)).start()
        print("starting sending attacked tx")
        self.client.send_tx(tx)
        print("finish sending attacked tx")
        # assert exist and not packed
        assert_equal(self.client.get_tx(tx.hash_hex())["blockHash"], None)

        client2 = RpcClient(self.nodes[1])
        for _ in range(100):
            try:
                tx2 = client2.get_tx(tx.hash_hex())
                assert_equal(tx2["blockHash"], None)
                print("client2 received")
                print("Success!")
                break
            except Exception as e:
                print("wait for client2 to receive tx", e)
                time.sleep(0.5)

    def generate_collision_txs(self, tx):
        # guess the random key between nodes
        tx_list = []
        accounts = []
        for _ in range(10):
            print("guess a new key")
            key = random.randbytes(16)
            for _ in range(100):
                account = Account.create()
                value = 0
                while True:
                    new_tx = self.client.new_tx(priv_key=account.key.hex(), value=value, sign=True)
                    if (siphash.siphash24(key, new_tx.hash).hexdigest()[0]
                            == siphash.siphash24(key, tx.hash).hexdigest()[0]):
                        print("random byte collision tx found", value)
                        accounts.append(account.address)
                        tx_list.append(eth_utils.encode_hex(rlp.encode(new_tx)))
                        break
                    else:
                        value += 1
        return accounts, tx_list

def async_send_txs(client, txs):
    print("start sending collision txs")
    for tx in txs:
        client.send_raw_tx(tx)
    print("finish sending collision txs")

if __name__ == '__main__':
    HashCollisionTest().main()
