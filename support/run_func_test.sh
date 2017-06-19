#!/usr/bin/env bash

source /etcdb/support/bootstrap.sh

make -C /etcdb bootstrap test-functional
