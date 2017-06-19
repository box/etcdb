#!/usr/bin/env bash


function start_etcd() {

    etcd --name infra0 \
    --initial-advertise-peer-urls http://127.0.0.1:2380 \
    --listen-peer-urls http://127.0.0.1:2380 \
    --listen-client-urls http://127.0.0.1:2379 \
    --advertise-client-urls http://127.0.0.1:2379 \
    --initial-cluster-token etcd-cluster-1 \
    --initial-cluster infra0=http://127.0.0.1:2380 \
    --initial-cluster-state new \
    &
}

apt-get update
packages="etcd
make
python
python-pip"
apt-get -y install ${packages}
start_etcd
