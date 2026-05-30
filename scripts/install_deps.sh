#!/bin/bash

set -e

echo "Installing dependencies for TaskFlow..."

echo "Updating package lists..."
sudo apt-get update

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
