import sys, os
sys.path.append("/Users/peilun/Development/cfx-account")
import cfx_account

ACCOUNT_NUM = 1000

def generate_quantum_accounts(account_num):
    account_list = []
    for i in range(0, account_num):
        account_list.append(cfx_account.Account.get_key_pair_post_quantum())
    return account_list


key_list = generate_quantum_accounts(ACCOUNT_NUM)

# copy to sign_secrets.txt
ps_keys_list = {}
current_path = os.path.abspath(os.path.dirname(__file__))

with open(current_path + '/sign_secrets.txt', 'w') as file:
    for key in key_list:
        file.write("quantum:" + key[0] + key[1])
        file.write('\n')
