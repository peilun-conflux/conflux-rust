import subprocess

from test_framework.test_framework import ConfluxTestFramework
from test_framework.util import assert_equal


class HashCollisionTest(ConfluxTestFramework):
    def set_test_params(self):
        self.num_nodes = 2

    def run_test(self):
        # start a process to generate hash collision
        subprocess.Popen()
        # send a tx to node 0
        receipt = self.cfx_transfer(self.core_secrets[0])
        assert_equal(receipt["outcomeStatus"], "0x0")