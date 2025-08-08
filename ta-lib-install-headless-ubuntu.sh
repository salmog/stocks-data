#!/bin/bash
set -e

# Update system
sudo apt-get update && sudo apt-get upgrade -y

# Install build dependencies
sudo apt-get install -y build-essential wget curl \
    automake autoconf libtool pkg-config

# Download TA-Lib source
cd /tmp
wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz

# Extract
tar -xzf ta-lib-0.4.0-src.tar.gz
cd ta-lib

# Configure and build
./configure --prefix=/usr
make
sudo make install

# Cleanup
cd ..
rm -rf ta-lib ta-lib-0.4.0-src.tar.gz

# Now install the Python wrapper inside your virtualenv or globally
# If you use virtualenv activate it first, then:
pip install ta-lib
