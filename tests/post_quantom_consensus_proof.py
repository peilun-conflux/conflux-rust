#!/usr/bin/env python3
import argparse
import os
import random

from test_framework.util import get_simple_rpc_proxy
import ast

MAX_COUNT = 1

def verify_dilithium_signature(client, message, signature, pubkey):
    # 验证Dilithium签名
    result = client.verify_dilithium_signature(message, signature, pubkey)
    print(f"验证Dilithium签名: sign_hash={message}, signature={signature}, result={result}")
    return result

def find_between(text, start, end):
    try:
        s = text.index(start) + len(start)
        e = text.index(end, s)
        return text[s:e+1]
    except ValueError:
        return None

def encode_hex(s):
    # 将字符串转换为十六进制
    nums = ast.literal_eval(s)  # 转换 "[1, 2, 40]" → [1, 2, 40]
    return "0x" + "".join(f"{n:02x}" for n in nums)


def get_pubkey(log_directory):
    # 从日志文件中获取公钥
    log_f = open(os.path.join(log_directory, "node0", "conflux.log"), "r")
    pubkey = None
    for line in log_f.readlines():
        if "own_pos_public_key" in line:
            pubkey = "0x" + find_between(line, "DilithiumPublicKey(", ")")[:-1]
            print("节点Dilithium公钥找到:", pubkey)
            return pubkey
    print("未找到公钥!")
    exit(1)


def get_signature(log_directory):
    # 从日志文件中获取签名
    pos_log_f = open(os.path.join(log_directory, "node0", "pos.log"), "r")
    count = 0
    for line in pos_log_f.readlines():
        if "dilithium sign" in line:
            h = encode_hex(find_between(line, "hash=", "]"))
            sig = encode_hex(find_between(line, "signature=", "]"))
            print(f"哈希: {h}")
            print(f"签名: {sig}")
            count += 1
            if count >= MAX_COUNT:
                break


def verify(client, pubkey, signature, hash):
    # 验证签名
    print("正在验证签名...")
    if verify_dilithium_signature(client, hash, signature, pubkey):
        print("测试成功")


def main():
    # 解析命令行参数
    parser = argparse.ArgumentParser(description="Dilithium签名操作")
    subparsers = parser.add_subparsers(dest="command", help="可用命令")
    subparsers.add_parser("get-pubkey", help="从日志中获取公钥")
    subparsers.add_parser("get-signature", help="从日志中获取签名")
    verify_parser = subparsers.add_parser("verify", help="验证签名")
    verify_parser.add_argument("--pubkey", type=str, required=True, help="公钥")
    verify_parser.add_argument("--signature", type=str, required=True, help="签名")
    verify_parser.add_argument("--hash", type=str, required=True, help="哈希")
    args = parser.parse_args()
    client = get_simple_rpc_proxy("http://localhost:15025")
    log_location_f = open("file.txt", "r")
    log_directory = log_location_f.readline().strip()

    if args.command == "get-pubkey":
        get_pubkey(log_directory)
    elif args.command == "get-signature":
        get_signature(log_directory)
    elif args.command == "verify":
        verify(client, args.pubkey, args.signature, args.hash)
    else:
        parser.print_help()

if __name__ == '__main__':
    main()