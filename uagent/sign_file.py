#!/usr/bin/python2.7

import gnupg
import os
import sys

if (len(sys.argv) != 4):
    print("Usage: {0} <gnupg-home> <key-id> <filename>".format(sys.argv[0]))
    sys.exit(1)

gnupg_home = sys.argv[1]
signing_key_id = sys.argv[2]
filename = sys.argv[3]

gpg = gnupg.GPG(gnupghome=gnupg_home)

with open(filename, "rb") as fh:
    signed_data = gpg.sign_file(fh, keyid=signing_key_id, detach=True, output=filename+".sig")

print(str(signed_data))
