#!/usr/bin/env python3

# allow imports from parent directory
# source: https://stackoverflow.com/a/11158224
import os, sys

import eth_utils
import time

from conflux.rpc import RpcClient
from conflux.utils import int_to_hex, priv_to_addr
from test_framework.test_framework import DefaultConfluxTestFramework
from test_framework.util import *


class PosCommittedBlockTest(DefaultConfluxTestFramework):
    def set_test_params(self):
        self.num_nodes = 4
        self.conf_parameters["vrf_proposal_threshold"] = '"{}"'.format(int_to_hex(int(2 ** 256 - 1)))
        self.conf_parameters["pos_pivot_decision_defer_epoch_count"] = '0'
        self.conf_parameters["pos_round_per_term"] = '10'
        self.conf_parameters["log_level"] = '"trace"'


    def run_test(self):
        self.log.info("wait for initialization")
        client = RpcClient(self.nodes[0])
        # wait for the first epoch to end
        wait_until(lambda: client.pos_status()["latestVoted"] is not None)
        wait_until(lambda: int(client.pos_status()["latestCommitted"], 0) >= 8)
        self.log.info("wait for PoS progress")
        wait_until(lambda: int(client.pos_status()["epoch"], 0) == 2)
        self.log.info("PoS epoch 2 committed")
        wait_until(lambda: int(client.pos_status()["epoch"], 0) == 3)
        self.log.info("PoS epoch 3 committed")
        time.sleep(100000)


if __name__ == '__main__':
    PosCommittedBlockTest().main()
