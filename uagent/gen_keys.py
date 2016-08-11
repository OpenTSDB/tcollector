#!/usr/bin/python2.7

import gnupg
import os
import sys

if (len(sys.argv) != 4):
    print("Usage: {0} <gnupg-home> <name> <email>".format(sys.argv[0]))
    sys.exit(1)

home = sys.argv[1]
name = sys.argv[2]
email = sys.argv[3]

gpg = gnupg.GPG(gnupghome=home)

input_data = gpg.gen_key_input(key_type="RSA", key_length=4096, name_real=name, name_email=email)
key = gpg.gen_key(input_data)

print("key: " + str(key))
