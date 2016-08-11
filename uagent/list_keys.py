#!/usr/bin/python2.7

import gnupg
import os
import sys

if (len(sys.argv) != 2):
    print("Usage: {0} <gnupg-home>".format(sys.argv[0]))
    sys.exit(1)

home = sys.argv[1]

gpg = gnupg.GPG(gnupghome=home)

public_keys = gpg.list_keys()
private_keys = gpg.list_keys(True)

print("public keys: " + str(public_keys))
print("private keys: " + str(private_keys))
