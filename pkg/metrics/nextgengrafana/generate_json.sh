#!/usr/bin/env bash
# Copyright 2025 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

go install github.com/google/go-jsonnet/cmd/jsonnet@latest

# ref https://github.com/grafana/grafonnet-lib/issues/338, use the forked 
# repo which implements the addOverride and addTransformation funtion for 
# new table.
# TODO: update to https://github.com/grafana/grafonnet
git clone https://github.com/nolouch/grafonnet-lib.git

export JSONNET_PATH=grafonnet-lib
jsonnet tidb_summary_with_keyspace_name.jsonnet > tidb_summary_with_keyspace_name.json
jsonnet tidb_resource_control_with_keyspace_name.jsonnet > tidb_resource_control_with_keyspace_name.json
rm -rf $JSONNET_PATH

