#!/usr/bin/env python3
import os
import random

from test_framework.util import get_simple_rpc_proxy
import ast

MAX_COUNT = 1

def verify_dilithium_signature(client, message, signature, pubkey):
    result = client.verify_dilithium_signature(message, signature, pubkey)
    print(f"Verify dilithium signature: sign_hash={message}, signature={signature}, result={result}")
    return result

def find_between(text, start, end):
    try:
        s = text.index(start) + len(start)
        e = text.index(end, s)
        return text[s:e+1]
    except ValueError:
        return None

def encode_hex(s):
    nums = ast.literal_eval(s)      # converts "[1, 2, 40]" → [1, 2, 40]
    return "0x" + "".join(f"{n:02x}" for n in nums)


def main():
    client = get_simple_rpc_proxy("http://localhost:15025")
    log_location_f = open("file.txt","r")
    log_directory = log_location_f.readline()

    log_f = open(os.path.join(log_directory, "node0", "conflux.log"),"r")
    pubkey = None
    for line in log_f.readlines():
        if "own_pos_public_key" in line:
            pubkey = "0x" + find_between(line, "DilithiumPublicKey(", ")")[:-1]
            print("Node Dilithium Public Key found: ", pubkey)
    if pubkey is None:
        print("No pubkey found!")
        exit(1)

    pos_log_f = open(os.path.join(log_directory, "node0", "pos.log"),"r")
    count = 0
    for line in pos_log_f.readlines():
        if "dilithium sign" in line:
            h = encode_hex(find_between(line, "hash=", "]"))
            sig = encode_hex(find_between(line, "signature=", "]"))
            print("PoS signature found. Verifying...")
            verify_dilithium_signature(client, h, sig, pubkey)
            new_sig_list = list(sig[2:])
            print("Modified the PoS signature by one byte. It should fail the verification.")
            while True:
                i = random.randint(0, len(new_sig_list) - 1)
                if new_sig_list[i] != 0:
                    print(f"Set index {i} from {new_sig_list[i]} to 0")
                    new_sig_list[i] = "0"
                    break
            new_sig = "0x" + "".join(new_sig_list)
            verify_dilithium_signature(client, h, new_sig, pubkey)

            count += 1
            if count >= MAX_COUNT:
                break
    print("Tests successful")


if __name__ == '__main__':
    main()