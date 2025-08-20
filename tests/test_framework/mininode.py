#!/usr/bin/env python3
"""Conflux P2P network half-a-node.

`P2PConnection: A low-level connection object to a node's P2P interface
P2PInterface: A high-level interface object for communicating to a node over P2P
"""
import time
from eth_utils import decode_hex

from conflux import utils
from conflux.config import DEFAULT_PY_TEST_CHAIN_ID
from conflux.messages import *
import asyncore
from collections import defaultdict
from io import BytesIO
import rlp
from rlp.sedes import big_endian_int, CountableList, boolean
import logging
import socket
import struct
import sys
import threading

from conflux.transactions import Transaction
from conflux.utils import hash32, hash20, sha3, int_to_bytes, sha3_256, ecrecover_to_pub, ec_random_keys, ecsign, \
    bytes_to_int, encode_int32, int_to_hex, int_to_32bytearray, zpad, rzpad
from test_framework.blocktools import make_genesis
from test_framework.util import wait_until, get_ip_address

logger = logging.getLogger("TestFramework.mininode")


class P2PConnection(asyncore.dispatcher):
    """A low-level connection object to a node's P2P interface.

    This class is responsible for:

    - opening and closing the TCP connection to the node
    - reading bytes from and writing bytes to the socket
    - deserializing and serializing the P2P message header
    - logging messages as they are sent and received

    This class contains no logic for handling the P2P message payloads. It must be
    sub-classed and the on_message() callback overridden."""

    def __init__(self):
        self.chain_id = None
        assert not network_thread_running()

        super().__init__(map=mininode_socket_map)

    def set_chain_id(self, chain_id):
        self.chain_id = chain_id

    def peer_connect(self, dstaddr, dstport):
        self.dstaddr = dstaddr
        self.dstport = dstport
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.sendbuf = b""
        self.recvbuf = b""
        self.state = "connecting"
        self.disconnect = False
        self.had_hello = False

        logger.debug('Connecting to Conflux Node: %s:%d' %
                     (self.dstaddr, self.dstport))

        try:
            self.connect((dstaddr, dstport))
        except Exception as e:
            logger.debug("network connect error" + str(e))
            self.handle_close()

    def peer_disconnect(self):
        # Connection could have already been closed by other end.
        if self.state == "connected":
            self.disconnect_node()

    # Connection and disconnection methods

    def handle_connect(self):
        """asyncore callback when a connection is opened."""
        if self.state != "connected":
            logger.debug("Connected & Listening: %s:%d" %
                         (self.dstaddr, self.dstport))
            self.state = "connected"
            self.on_open()

    def handle_close(self):
        """asyncore callback when a connection is closed."""
        logger.debug("Closing connection to: %s:%d" %
                     (self.dstaddr, self.dstport))
        self.state = "closed"
        self.recvbuf = b""
        self.sendbuf = b""
        try:
            self.close()
        except:
            pass
        self.on_close()

    def disconnect_node(self):
        """Disconnect the p2p connection.

        Called by the test logic thread. Causes the p2p connection
        to be disconnected on the next iteration of the asyncore loop."""
        self.disconnect = True

    # Socket read methods

    def handle_read(self):
        """asyncore callback when data is read from the socket."""
        buf = self.recv(8192)
        if len(buf) > 0:
            self.recvbuf += buf
            self._on_data()

    def read_connection_packet(self):
        if len(self.recvbuf) < 3:
            return None

        packet_size = struct.unpack("<L", rzpad(self.recvbuf[:3], 4))[0]
        if len(self.recvbuf) < 3 + packet_size:
            return

        self.recvbuf = self.recvbuf[3:]
        packet = self.recvbuf[:packet_size]
        self.recvbuf = self.recvbuf[packet_size:]

        if len(packet) > 3:
            packet = packet[-3:] + packet[:-3]

        return packet

    def assemble_connection_packet(self, data):
        data_len = struct.pack("<L", len(data))[:3]

        if len(data) > 3:
            return data_len + data[3:] + data[:3]
        else:
            return data_len + data

    def read_session_packet(self, packet):
        if packet[-2] == 0:
            return (packet[-1], None, packet[:-2])
        else:
            return (packet[-1], packet[-5:-2], packet[:-5])

    def assemble_session_packet(self, packet_id, protocol, payload):
        packet_id = struct.pack("<B", packet_id)
        if protocol is None:
            return payload + b'\x00' + packet_id
        else:
            return payload + protocol + b'\x01' + packet_id

    def read_protocol_msg(self, msg):
        return (msg[-1], msg[:-1])

    def assemble_protocol_msg(self, msg):
        return rlp.encode(msg) + int_to_bytes(get_msg_id(msg))

    def _on_data(self):
        """Try to read P2P messages from the recv buffer.

        This method reads data from the buffer in a loop. It deserializes,
        parses and verifies the P2P header, then passes the P2P payload to
        the on_message callback for processing."""
        try:
            while True:
                packet = self.read_connection_packet()
                if packet is None:
                    return

                if self.on_handshake(packet):
                    continue

                packet_id, protocol, payload = self.read_session_packet(packet)
                self._log_message("receive", packet_id)

                if packet_id != PACKET_HELLO and packet_id != PACKET_DISCONNECT and (not self.had_hello):
                    raise ValueError("bad protocol")

                if packet_id == PACKET_HELLO:
                    self.on_hello(payload)
                elif packet_id == PACKET_DISCONNECT:
                    disconnect = Disconnect(payload[0], payload[1:])
                    self.on_disconnect(disconnect)
                else:
                    assert packet_id == PACKET_PROTOCOL
                    self.on_protocol_packet(protocol, payload)
        except Exception as e:
            logger.exception('Error reading message: ' + repr(e))
            raise

    def on_handshake(self, payload) -> bool:
        return False

    def on_hello(self, payload):
        self.had_hello = True

    def on_disconnect(self, disconnect):
        self.on_close()

    def on_protocol_packet(self, protocol, payload):
        """Callback for processing a protocol-specific P2P payload. Must be overridden by derived class."""
        raise NotImplementedError

    # Socket write methods

    def writable(self):
        """asyncore method to determine whether the handle_write() callback should be called on the next loop."""
        with mininode_lock:
            pre_connection = self.state == "connecting"
            length = len(self.sendbuf)
        return (length > 0 or pre_connection)

    def handle_write(self):
        """asyncore callback when data should be written to the socket."""
        with mininode_lock:
            # asyncore does not expose socket connection, only the first read/write
            # event, thus we must check connection manually here to know when we
            # actually connect
            if self.state == "connecting":
                self.handle_connect()
            if not self.writable():
                return

            try:
                sent = self.send(self.sendbuf)
            except:
                self.handle_close()
                return
            self.sendbuf = self.sendbuf[sent:]

    def send_packet(self, packet_id, payload, pushbuf=False):
        """Send a P2P message over the socket.

        This method takes a P2P payload, builds the P2P header and adds
        the message to the send buffer to be sent over the socket."""
        self._log_message("send", packet_id)
        buf = self.assemble_session_packet(packet_id, None, payload)

        self.send_data(buf)


    def send_data(self, data, pushbuf=False):
        if self.state != "connected" and not pushbuf:
            raise IOError('Not connected, no pushbuf')

        buf = self.assemble_connection_packet(data)

        with mininode_lock:
            if (len(self.sendbuf) == 0 and not pushbuf):
                try:
                    sent = self.send(buf)
                    self.sendbuf = buf[sent:]
                except BlockingIOError:
                    self.sendbuf = buf
            else:
                self.sendbuf += buf

    def send_protocol_packet(self, payload):
        """Send packet of protocols"""
        buf = self.assemble_session_packet(PACKET_PROTOCOL, self.protocol, payload)
        self.send_data(buf)

    def send_protocol_msg(self, msg):
        """Send packet of protocols"""
        payload = self.assemble_protocol_msg(msg)
        self.send_protocol_packet(payload)

    # Class utility methods

    def _log_message(self, direction, msg):
        """Logs a message being sent or received over the connection."""
        if direction == "send":
            log_message = "Send message to "
        elif direction == "receive":
            log_message = "Received message from "
        log_message += "%s:%d: %s" % (self.dstaddr,
                                      self.dstport, repr(msg)[:500])
        if len(log_message) > 500:
            log_message += "... (msg truncated)"
        logger.debug(log_message)


class P2PInterface(P2PConnection):
    """A high-level P2P interface class for communicating with a Conflux node.

    This class provides high-level callbacks for processing P2P message
    payloads, as well as convenience methods for interacting with the
    node over P2P.

    Individual testcases should subclass this and override the on_* methods
    if they want to alter message handling behaviour."""

    def __init__(self, genesis: str, remote=False):
        super().__init__()

        # Track number of messages of each type received and the most recent
        # message of each type
        self.message_count = defaultdict(int)
        self.protocol_message_count = defaultdict(int)
        self.last_message = {}
        self.last_protocol_message = {}

        # Default protocol version
        self.protocol = b'cfx'
        self.protocol_version = 3
        # Store genesis_hash
        self.genesis = decode_hex(genesis)
        self.best_block_hash = self.genesis
        self.blocks = {self.genesis: self.genesis}
        self.peer_pubkey = None
        self.priv_key, self.pub_key = ec_random_keys()
        x, y = self.pub_key
        self.key = "0x" + utils.encode_hex(bytes(int_to_32bytearray(x))) + utils.encode_hex(bytes(int_to_32bytearray(y)))
        self.had_status = False
        self.on_packet_func = {}
        self.remote = remote

    def peer_connect(self, *args, **kwargs):
        super().peer_connect(*args, **kwargs)

    def wait_for_status(self, timeout=60):
        wait_until(lambda: self.had_status, timeout=timeout, lock=mininode_lock)

    def set_callback(self, msgid, func):
        self.on_packet_func[msgid] = func

    def reset_callback(self, msgid):
        del self.on_packet_func[msgid]

    # Message receiving methods

    def send_status(self):
        status = Status(
            ChainIdParams(self.chain_id),
            self.genesis, 0, 0, [self.best_block_hash])
        self.send_protocol_msg(status)

    def on_protocol_packet(self, protocol, payload):
        """Receive message and dispatch message to appropriate callback.

        We keep a count of how many of each message type has been received
        and the most recent message of each type."""
        with mininode_lock:
            try:
                assert(protocol == self.protocol)  # Possible to be false?
                packet_type, payload = self.read_protocol_msg(payload)
                self.protocol_message_count[packet_type] += 1
                msg = None
                msg_class = get_msg_class(packet_type)
                logger.debug("%s %s", packet_type, rlp.decode(payload))
                if msg_class is not None:
                    msg = rlp.decode(payload, msg_class)
                if packet_type == STATUS_V3:
                    self._log_message("receive", "STATUS, terminal_hashes:{}"
                                      .format([utils.encode_hex(i) for i in msg.terminal_block_hashes]))
                    self.had_status = True
                elif packet_type == GET_BLOCK_HEADERS:
                    self._log_message("receive", "GET_BLOCK_HEADERS of {}".format(msg.hashes))
                elif packet_type == GET_BLOCK_HEADER_CHAIN:
                    self._log_message("receive", "GET_BLOCK_HEADER_CHAIN of {} {}".format(msg.hash, msg.max_blocks))
                elif packet_type == GET_BLOCK_BODIES:
                    hashes = msg.hashes
                    self._log_message("receive", "GET_BLOCK_BODIES of {} blocks".format(len(hashes)))
                elif packet_type == GET_BLOCK_HEADERS_RESPONSE:
                    self._log_message("receive", "BLOCK_HEADERS of {} headers".format(len(msg.headers)))
                elif packet_type == GET_BLOCK_BODIES_RESPONSE:
                    self._log_message("receive", "BLOCK_BODIES of {} blocks".format(len(msg)))
                elif packet_type == NEW_BLOCK:
                    self._log_message("receive", "NEW_BLOCK, hash:{}".format(msg.block.block_header.hash))
                elif packet_type == GET_BLOCK_HASHES:
                    self._log_message("receive", "GET_BLOCK_HASHES, hash:{}, max_blocks:{}"
                                      .format(msg.hash, msg.max_blocks))
                elif packet_type == GET_BLOCK_HASHES_RESPONSE:
                    self._log_message("receive", "BLOCK_HASHES, {} hashes".format(len(msg.hashes)))
                elif packet_type == GET_TERMINAL_BLOCK_HASHES:
                    self._log_message("receive", "GET_TERMINAL_BLOCK_HASHES")
                elif packet_type == TRANSACTIONS:
                    self._log_message("receive", "TRANSACTIONS, {} transactions".format(len(msg.transactions)))
                elif packet_type == GET_TERMINAL_BLOCK_HASHES_RESPONSE:
                    self._log_message("receive", "TERMINAL_BLOCK_HASHES, {} hashes".format(len(msg.hashes)))
                elif packet_type == NEW_BLOCK_HASHES:
                    self._log_message("receive", "NEW_BLOCK_HASHES, {} hashes".format(len(msg.block_hashes)))
                elif packet_type == GET_BLOCKS_RESPONSE:
                    self._log_message("receive", "BLOCKS, {} blocks".format(len(msg.blocks)))
                elif packet_type == GET_CMPCT_BLOCKS_RESPONSE:
                    self._log_message("receive", "GET_CMPCT_BLOCKS_RESPONSE, {} blocks".format(len(msg.blocks)))
                elif packet_type == GET_BLOCK_TXN_RESPONSE:
                    self._log_message("receive", "GET_BLOCK_TXN_RESPONSE, block:{}".format(len(msg.block_hash)))
                elif packet_type == GET_BLOCKS:
                    self._log_message("receive", "GET_BLOCKS, {} hashes".format(len(msg.hashes)))
                    self.on_get_blocks(msg)
                elif packet_type == GET_CMPCT_BLOCKS:
                    self._log_message("receive", "GET_CMPCT_BLOCKS, {} hashes".format(len(msg.hashes)))
                    self.on_get_compact_blocks(msg)
                elif packet_type == GET_BLOCK_TXN:
                    self._log_message("receive", "GET_BLOCK_TXN, hash={}".format(len(msg.block_hash)))
                    self.on_get_blocktxn(msg)
                elif packet_type == GET_BLOCK_HASHES_BY_EPOCH:
                    self._log_message("receive", "GET_BLOCK_HASHES_BY_EPOCH, epochs: {}".format(msg.epochs))
                    self.on_get_block_hashes_by_epoch(msg)
                else:
                    self._log_message("receive", "Unknown packet {}".format(packet_type))
                    return
                if packet_type in self.on_packet_func and msg is not None:
                    self.on_packet_func[packet_type](self, msg)
            except:
                raise

    def on_hello(self, payload):
        hello = rlp.decode(payload, Hello)

        capabilities = []
        for c in hello.capabilities:
            capabilities.append((c.protocol, c.version))
        self._log_message(
            "receive", "Hello, capabilities:{}".format(capabilities))
        ip = [127, 0, 0, 1]
        if self.remote:
            ip = get_ip_address()
        endpoint = NodeEndpoint(address=bytes(ip), tcp_port=32325, udp_port=32325)
        # FIXME: Use a valid pos_public_key.
        hello = Hello(DEFAULT_PY_TEST_CHAIN_ID, [Capability(self.protocol, self.protocol_version)], endpoint,
                      decode_hex('ab489a6b93a6768c89609ddb3f8adc8a2681358a435d3ac036b3c1a36bdd0b5eec6dad568538c5b911ee8873eedd690b3b83aca86e96f4344f7f959439b9bdfd170db887a569f1cc8bf2742a169f4b9b663cb442f0d9f87be300d9808a0ded0c6464525d3dd71770419cfca4a26f5dd630aa19dd7c771c13a67d6329d4fe6c55f18cfbecb0facd2a0737b10c74297957066f84ab1a83b9d3d801e1a1ead79fb3bbdeb17271834890ec35216ccc6835571bc3f7638ca31bd90c26b775c2f19c6a0c316372c46b17994483cfac9f530dd6cb5c91c64aab753a60ac9f2972e8868e4ee7eb3e6c3068326f397955efc5dcad929bb279ef764d145e0863273c79ab0dd591692715f22357a83da61535aa33bb3e113750d1a4d04fea48fd7f42a9274ed51989c4b919f80bda7955fc27fb0e41c575d39a1bfb32c0cccdec434090cd9f92ed5649051d39d70e45c3c265b9acd01e7504501f3cbe9aa712e956540bb7a659b55416b481d24b53ed593f5ab16000498c51d767cca2a544b7f4d313fbea0c4835b0eaa6823a83e11ff4c73082ac77470bb6352e874ca2ba4288f8074797590bd8dc01c1a2830ca862e4a2abd11164135c023e6190d60248487b30502b2d3f8c4a32e33d499401a24f2045198d6acd50c83baa9d1a6ff17e9479b876e164b6e6fac51adab664564b10a718c842667b59a7c2db67def38c55998b061af7e801e71a323dc772ddbe2207d90828a65240410f2fe0eba32f2488f0dabb0d5802278e3322e389ccb37e3bb76edfb3c0b700a8c19e2f5fb60a9d26ce0c9fd700c05dec09276775f17334f4085b1384a1559819fe516d1d29c1a3e8d02988399758410cd00e25df4feb03f0ccc81afa7c138d25baeb553009d292e05d41205d9fc4bb1ea69e14ecafc3b2b99afcc0cc9a6a0287390a7406d99fc055128118e0cffb458f1167a85bfe3880c836dcc4b5fb74c3b611961c34e2d2630e6dab58e98e9d56043c91e3a8ef31055a835282d60334eb6f2c56552a59dbe548215a1e064ee129a117c30323b5b6e18b0c261333b4a73c41772078a19fed89f6ec33f0985eabbf1bb078f8a70901fe2d01d865ddeb6e89ab72219e5c021c704031c725ab43f00d0fff4efd077976e334fcd9b5ba709a8cd0b7471d8f72518a1a69fda9ba1a49c9c178fa64514f30093e9c1da6702805e20349783c25f50ea91f37dc28526ed5b7a366a11d100dce85c056433393324d6910020cd79cde5467ea53576c3d1a1e07866098c94f59ef25d645fa733dad2d896515bce4e1b33f778a543324bd77db843b20d71f2b6e64ee51bc3599161bac0c46be38ba4cc75e0020abc393515b9650074c14183e6c18312e1aeeb679828087fca10e5372649ef3500108a66b6f5155aa65e53591502be559ffe8fa9acfd440bdfcad4c3b8d2e155947f44d485a2ab13c4e74d09e7930fa32fe92af8fb28aefdae248bfbb6f24c1bae9285643bf5df14deb8c350712bf0dba940954fbbab02239eae83d485469723adb84f8114ee32228954bad388777503dd2ba0c5ccd9345bc0832bbca4a514810e5ce07b3a89c5c49bbf5ffe3300fdc85757bbd4f1842a4f64d641aacc697f5f145edec01fdf43d286181a28f1ab41f294aa090719a9ff73cf29a304e8724e5e690ee85a00ef010a0b0703bfd6adb047ca36b501781ced516c81fb67a812355d539f6578fad999a3725bac7ce42e55c61e3c2329803a8c1683783256c3705b0e8b08bfb7eea14125a816e4866d0766e7e5dbc93cff17d5d82c7f20de2331c045458e20b758d6e7f50b29c3923ee0d8add389e9227d2225c30b76c4c0e0b54b14e11778c754809333263438fd07edd1e03246683ff58da8bdde286c321032765258d0c34ff'))

        self.send_packet(PACKET_HELLO, rlp.encode(hello, Hello))
        self.had_hello = True
        self.send_status()

    # Callback methods. Can be overridden by subclasses in individual test
    # cases to provide custom message handling behaviour.

    def on_open(self):
        self.handshake = Handshake(self)
        self.handshake.write_auth()

    def on_close(self): pass

    def on_handshake(self, payload) -> bool:
        if self.handshake.state == "ReadingAck":
            self.handshake.read_ack(payload)
            return True

        assert self.handshake.state == "StartSession"

        return False

    def on_get_blocks(self, msg):
        resp = Blocks(reqid=msg.reqid, blocks=[])
        self.send_protocol_msg(resp)

    def on_get_compact_blocks(self, msg):
        resp = GetCompactBlocksResponse(reqid=msg.reqid, compact_blocks=[], blocks=[])
        self.send_protocol_msg(resp)

    def on_get_blocktxn(self, msg):
        resp = GetBlockTxnResponse(reqid=msg.reqid, block_hash=b'\x00'*32, block_txn=[])
        self.send_protocol_msg(resp)

    def on_get_block_hashes_by_epoch(self, msg):
        resp = BlockHashes(reqid=msg.reqid, hashes=[])
        self.send_protocol_msg(resp)

# Keep our own socket map for asyncore, so that we can track disconnects
# ourselves (to work around an issue with closing an asyncore socket when
# using select)
mininode_socket_map = dict()

# One lock for synchronizing all data access between the networking thread (see
# NetworkThread below) and the thread running the test logic.  For simplicity,
# P2PConnection acquires this lock whenever delivering a message to a P2PInterface,
# and whenever adding anything to the send buffer (in send_message()).  This
# lock should be acquired in the thread running the test logic to synchronize
# access to any data shared with the P2PInterface or P2PConnection.
mininode_lock = threading.RLock()

class DefaultNode(P2PInterface):
    def __init__(self, genesis: str, remote = False):
        super().__init__(genesis, remote)

class NetworkThread(threading.Thread):

    def __init__(self):
        super().__init__(name="NetworkThread")

    def run(self):
        while mininode_socket_map:
            # We check for whether to disconnect outside of the asyncore
            # loop to work around the behavior of asyncore when using
            # select
            disconnected = []
            for fd, obj in mininode_socket_map.items():
                if obj.disconnect:
                    disconnected.append(obj)
            [obj.handle_close() for obj in disconnected]
            asyncore.loop(0.1, use_poll=True, map=mininode_socket_map, count=1)
        logger.debug("Network thread closing")


def network_thread_running():
    """Return whether the network thread is running."""
    return any([thread.name == "NetworkThread" for thread in threading.enumerate()])


def network_thread_start():
    """Start the network thread."""
    assert not network_thread_running()

    NetworkThread().start()


def network_thread_join(timeout=10):
    """Wait timeout seconds for the network thread to terminate.

    Throw if network thread doesn't terminate in timeout seconds."""
    network_threads = [
        thread for thread in threading.enumerate() if thread.name == "NetworkThread"]
    assert len(network_threads) <= 1
    for thread in network_threads:
        thread.join(timeout)
        assert not thread.is_alive()

def start_p2p_connection(nodes: list, remote=False):
    if len(nodes) == 0:
        return
    p2p_connections = []
    # TODO(lpl): Figure out why pos slows down node starting.
    time.sleep(1)
    genesis = nodes[0].cfx_getBlockByEpochNumber("0x0", False)["hash"]

    for node in nodes:
        conn = DefaultNode(genesis, remote)
        p2p_connections.append(conn)
        node.add_p2p_connection(conn)

    network_thread_start()
    
    for p2p in p2p_connections:
        p2p.wait_for_status()

    return p2p_connections

class Handshake:
    def __init__(self, peer: P2PInterface):
        self.peer = peer
        self.state = "New"

    def write_auth(self):
        node_id = utils.decode_hex(self.peer.key)
        self.peer.send_data(node_id)
        self.state = "ReadingAck"

    def read_ack(self, remote_node_id: bytes):
        assert len(remote_node_id) == 64, "invalid node id length {}".format(len(remote_node_id))
        self.peer.peer_key = utils.encode_hex(remote_node_id)
        self.state = "StartSession"
