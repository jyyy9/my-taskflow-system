#!/bin/bash

set -e

echo "Installing dependencies for TaskFlow..."

echo "Configuring Aliyun APT mirror..."

if [ ! -f "/etc/apt/sources.list.bak" ]; then
    sudo cp /etc/apt/sources.list /etc/apt/sources.list.bak
    echo "Backed up original sources.list"
fi

cat > /tmp/aliyun_sources.list << EOF
deb http://mirrors.aliyun.com/ubuntu/ jammy main restricted universe multiverse
deb-src http://mirrors.aliyun.com/ubuntu/ jammy main restricted universe multiverse
deb http://mirrors.aliyun.com/ubuntu/ jammy-security main restricted universe multiverse
deb-src http://mirrors.aliyun.com/ubuntu/ jammy-security main restricted universe multiverse
deb http://mirrors.aliyun.com/ubuntu/ jammy-updates main restricted universe multiverse
deb-src http://mirrors.aliyun.com/ubuntu/ jammy-updates main restricted universe multiverse
deb http://mirrors.aliyun.com/ubuntu/ jammy-backports main restricted universe multiverse
deb-src http://mirrors.aliyun.com/ubuntu/ jammy-backports main restricted universe multiverse
EOF

sudo cp /tmp/aliyun_sources.list /etc/apt/sources.list
echo "Configured Aliyun APT mirror successfully"

echo "Updating package lists..."
sudo apt-get update -y

echo "Installing build tools..."
sudo apt-get install -y build-essential cmake git

echo "Installing gRPC and Protocol Buffers..."
sudo apt-get install -y libgrpc++-dev libprotobuf-dev protobuf-compiler-grpc

echo "Installing Boost..."
sudo apt-get install -y libboost-all-dev

echo "Installing Redis..."
sudo apt-get install -y redis-server

echo "Installing etcd..."
sudo apt-get install -y etcd

echo "Installing YAML parser..."
sudo apt-get install -y libyaml-cpp-dev

echo "Installing curl for HTTP..."
sudo apt-get install -y libcurl4-openssl-dev

echo "Installing hiredis for Redis..."
sudo apt-get install -y libhiredis-dev

echo "Creating logs directory..."
mkdir -p logs

echo "All dependencies installed successfully!"
echo ""
echo "Note: APT sources have been changed to Aliyun mirror."
echo "To restore original sources: sudo cp /etc/apt/sources.list.bak /etc/apt/sources.list"
