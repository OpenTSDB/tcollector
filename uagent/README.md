CONTENTS

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


HOW TO USE IT

1. You need to have a web server (e.g. Apache2) that supports HTTPS;
   Remember to copy the ssl cert of the web server to your GNUPG home
   directory (which should be "install_root"/.gnupg. See step 3.)
   and name it "server-certs.pem";
2. Update "uagent.conf" and make suer "server_base" points to your web server
   (e.g. https://localhost)
3. Update "uagent.conf" and make sure "install_root" points to the folder where
   everything (cloudwiz-agent, update-agent, etc.) is installed;
4. To run the update-agent once, run "uagent.py";
5. To run the update-agent in a long lived loop, run "daemon.py";
   Note that this script runs update-agent once an hour. You can adjust this
   in the "daemon.py" script itself.
