uagent.py - The update agent that will download updates (of cloudwiz agent or update agent itself)
            and apply them to the local installation
uagent.conf - Configuration file for the uagent.py.
daemon.py - Run this if you want to run update agent in a long running loop.
calc_checksum.py - Script that will calculate the checksum of a file.
download_file.py - Download a file from server (whose pub cert must be located at gnupg-home/server-certs.pem)
gen_keys.py - Generate a pair of 4096-bit RSA keys that can be used to sign files.
list_keys.py - List all existing keys (private as well as public).
sign_file.py - Sign a file with private key (you need to supply the key ID).
verify_file.py - Verify the signature of a file using existing public keys.
