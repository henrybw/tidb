// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logicalop_test

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/session/sessionapi"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/set"
	"github.com/stretchr/testify/require"
)

func getLogicalMemTable(t *testing.T, dom *domain.Domain, se sessionapi.Session, parser *parser.Parser, sql string) (*logicalop.LogicalMemTable, bool) {
	stmt, err := parser.ParseOneStmt(sql, "", "")
	require.NoError(t, err)

	ctx := context.Background()
	builder, _ := plannercore.NewPlanBuilder().Init(se.GetPlanCtx(), dom.InfoSchema(), hint.NewQBHintHandler(nil))
	nodeW := resolve.NewNodeW(stmt)
	plan, err := builder.Build(ctx, nodeW)
	require.NoError(t, err)

	logicalPlan, err := plannercore.LogicalOptimizeTest(ctx, builder.GetOptFlag(), plan.(base.LogicalPlan))
	require.NoError(t, err)

	// Obtain the leaf plan
	leafPlan := logicalPlan
	for len(leafPlan.Children()) > 0 {
		leafPlan = leafPlan.Children()[0]
	}
	switch lg := leafPlan.(type) {
	case *logicalop.LogicalMemTable:
		return lg, true
	case *logicalop.LogicalTableDual:
		return nil, false
	}
	panic("unreachable")
}

func TestClusterConfigTableExtractor(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)

	parser := parser.New()
	var cases = []struct {
		sql         string
		nodeTypes   set.StringSet
		instances   set.StringSet
		skipRequest bool
		dual        bool
	}{
		{
			sql:       "select * from information_schema.cluster_config",
			nodeTypes: nil,
			instances: nil,
		},
		{
			sql:       "select * from information_schema.cluster_config where type='tikv'",
			nodeTypes: set.NewStringSet("tikv"),
			instances: set.NewStringSet(),
		},
		{
			sql:       "select * from information_schema.cluster_config where 'tikv'=type",
			nodeTypes: set.NewStringSet("tikv"),
			instances: set.NewStringSet(),
		},
		{
			sql:       "select * from information_schema.cluster_config where 'TiKV'=type",
			nodeTypes: set.NewStringSet("tikv"),
			instances: set.NewStringSet(),
		},
		{
			sql:       "select * from information_schema.cluster_config where 'tikv'=type",
			nodeTypes: set.NewStringSet("tikv"),
			instances: set.NewStringSet(),
		},
		{
			sql:       "select * from information_schema.cluster_config where 'TiKV'=type or type='tidb'",
			nodeTypes: set.NewStringSet("tikv", "tidb"),
			instances: set.NewStringSet(),
		},
		{
			sql:       "select * from information_schema.cluster_config where 'TiKV'=type or type='tidb' or type='pd'",
			nodeTypes: set.NewStringSet("tikv", "tidb", "pd"),
			instances: set.NewStringSet(),
		},
		{
			sql:       "select * from information_schema.cluster_config where (type='tidb' or type='pd') and (instance='123.1.1.2:1234' or instance='123.1.1.4:1234')",
			nodeTypes: set.NewStringSet("tidb", "pd"),
			instances: set.NewStringSet("123.1.1.2:1234", "123.1.1.4:1234"),
		},
		{
			sql:       "select * from information_schema.cluster_config where type in ('tikv', 'pd')",
			nodeTypes: set.NewStringSet("tikv", "pd"),
			instances: set.NewStringSet(),
		},
		{
			sql:       "select * from information_schema.cluster_config where type in ('tikv', 'pd') and instance='123.1.1.2:1234'",
			nodeTypes: set.NewStringSet("tikv", "pd"),
			instances: set.NewStringSet("123.1.1.2:1234"),
		},
		{
			sql:       "select * from information_schema.cluster_config where type in ('tikv', 'pd') and instance in ('123.1.1.2:1234', '123.1.1.4:1234')",
			nodeTypes: set.NewStringSet("tikv", "pd"),
			instances: set.NewStringSet("123.1.1.2:1234", "123.1.1.4:1234"),
		},
		{
			sql:       "select * from information_schema.cluster_config where type='tikv' and instance in ('123.1.1.2:1234', '123.1.1.4:1234')",
			nodeTypes: set.NewStringSet("tikv"),
			instances: set.NewStringSet("123.1.1.2:1234", "123.1.1.4:1234"),
		},
		{
			sql:       "select * from information_schema.cluster_config where type='tikv' and instance='123.1.1.4:1234'",
			nodeTypes: set.NewStringSet("tikv"),
			instances: set.NewStringSet("123.1.1.4:1234"),
		},
		{
			sql:       "select * from information_schema.cluster_config where type='tikv' and instance='123.1.1.4:1234'",
			nodeTypes: set.NewStringSet("tikv"),
			instances: set.NewStringSet("123.1.1.4:1234"),
		},
		{
			sql:       "select * from information_schema.cluster_config where type='tikv' and instance='cNs2dm.tikv.pingcap.com:1234'",
			nodeTypes: set.NewStringSet("tikv"),
			instances: set.NewStringSet("cNs2dm.tikv.pingcap.com:1234"),
		},
		{
			sql:       "select * from information_schema.cluster_config where type='TIKV' and instance='cNs2dm.tikv.pingcap.com:1234'",
			nodeTypes: set.NewStringSet("tikv"),
			instances: set.NewStringSet("cNs2dm.tikv.pingcap.com:1234"),
		},
		{
			sql:         "select * from information_schema.cluster_config where type='tikv' and type='pd'",
			nodeTypes:   set.NewStringSet(),
			instances:   set.NewStringSet(),
			skipRequest: true,
			dual:        true,
		},
		{
			sql:       "select * from information_schema.cluster_config where type='tikv' and type in ('pd', 'tikv')",
			nodeTypes: set.NewStringSet("tikv"),
			instances: set.NewStringSet(),
		},
		{
			sql:         "select * from information_schema.cluster_config where type='tikv' and type in ('pd', 'tidb')",
			nodeTypes:   set.NewStringSet(),
			instances:   set.NewStringSet(),
			skipRequest: true,
			dual:        true,
		},
		{
			sql:       "select * from information_schema.cluster_config where type in ('tikv', 'tidb') and type in ('pd', 'tidb')",
			nodeTypes: set.NewStringSet("tidb"),
			instances: set.NewStringSet(),
		},
		{
			sql:         "select * from information_schema.cluster_config where instance='123.1.1.4:1234' and instance='123.1.1.5:1234'",
			nodeTypes:   set.NewStringSet(),
			instances:   set.NewStringSet(),
			skipRequest: true,
			dual:        true,
		},
		{
			sql:       "select * from information_schema.cluster_config where instance='123.1.1.4:1234' and instance in ('123.1.1.5:1234', '123.1.1.4:1234')",
			nodeTypes: set.NewStringSet(),
			instances: set.NewStringSet("123.1.1.4:1234"),
		},
		{
			sql:         "select * from information_schema.cluster_config where instance='123.1.1.4:1234' and instance in ('123.1.1.5:1234', '123.1.1.6:1234')",
			nodeTypes:   set.NewStringSet(),
			instances:   set.NewStringSet(),
			skipRequest: true,
			dual:        true,
		},
		{
			sql:       "select * from information_schema.cluster_config where instance in ('123.1.1.5:1234', '123.1.1.4:1234') and instance in ('123.1.1.5:1234', '123.1.1.6:1234')",
			nodeTypes: set.NewStringSet(),
			instances: set.NewStringSet("123.1.1.5:1234"),
		},
		{
			sql: `select * from information_schema.cluster_config
				where instance in ('123.1.1.5:1234', '123.1.1.4:1234')
				  and instance in ('123.1.1.5:1234', '123.1.1.6:1234')
				  and type in ('tikv', 'tidb')
				  and type in ('pd', 'tidb')`,
			nodeTypes: set.NewStringSet("tidb"),
			instances: set.NewStringSet("123.1.1.5:1234"),
		},
		{
			sql: `select * from information_schema.cluster_config
				where instance in ('123.1.1.5:1234', '123.1.1.4:1234')
				  and instance in ('123.1.1.5:1234', '123.1.1.6:1234')
				  and instance in ('123.1.1.6:1234', '123.1.1.7:1234')
				  and instance in ('123.1.1.7:1234', '123.1.1.8:1234')`,
			nodeTypes:   set.NewStringSet(),
			instances:   set.NewStringSet(),
			skipRequest: true,
		},
	}
	for _, ca := range cases {
		logicalMemTable, ok := getLogicalMemTable(t, dom, se, parser, ca.sql)
		if ca.dual {
			require.False(t, ok, ca.sql)
		} else {
			require.True(t, ok, ca.sql)
			require.NotNil(t, logicalMemTable.Extractor)
			clusterConfigExtractor := logicalMemTable.Extractor.(*plannercore.ClusterTableExtractor)
			require.EqualValues(t, ca.nodeTypes, clusterConfigExtractor.NodeTypes, "SQL: %v", ca.sql)
			require.EqualValues(t, ca.instances, clusterConfigExtractor.Instances, "SQL: %v", ca.sql)
			require.EqualValues(t, ca.skipRequest, clusterConfigExtractor.SkipRequest, "SQL: %v", ca.sql)
		}
	}
}

func timestamp(t *testing.T, s string) int64 {
	tt, err := time.ParseInLocation("2006-01-02 15:04:05.999", s, time.Local)
	require.NoError(t, err)
	return tt.UnixMilli()
}

func TestClusterLogTableExtractor(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)

	parser := parser.New()
	var cases = []struct {
		sql                string
		nodeTypes          set.StringSet
		instances          set.StringSet
		skipRequest        bool
		startTime, endTime int64
		patterns           []string
		level              set.StringSet
	}{
		{
			sql:       "select * from information_schema.cluster_log",
			nodeTypes: nil,
			instances: nil,
		},
		{
			// Test for invalid time.
			sql:       "select * from information_schema.cluster_log where time='2019-10-10 10::10'",
			nodeTypes: set.NewStringSet(),
			instances: set.NewStringSet(),
		},
		{
			sql:       "select * from information_schema.cluster_log where type='tikv'",
			nodeTypes: set.NewStringSet("tikv"),
			instances: set.NewStringSet(),
		},
		{
			sql:       "select * from information_schema.cluster_log where 'tikv'=type",
			nodeTypes: set.NewStringSet("tikv"),
			instances: set.NewStringSet(),
		},
		{
			sql:       "select * from information_schema.cluster_log where 'TiKV'=type",
			nodeTypes: set.NewStringSet("tikv"),
			instances: set.NewStringSet(),
		},
		{
			sql:       "select * from information_schema.cluster_log where 'TiKV'=type or type='tidb'",
			nodeTypes: set.NewStringSet("tikv", "tidb"),
			instances: set.NewStringSet(),
		},
		{
			sql:       "select * from information_schema.cluster_log where 'TiKV'=type or type='tidb' or type='pd'",
			nodeTypes: set.NewStringSet("tikv", "tidb", "pd"),
			instances: set.NewStringSet(),
		},
		{
			sql:       "select * from information_schema.cluster_log where (type='tidb' or type='pd') and (instance='123.1.1.2:1234' or instance='123.1.1.4:1234')",
			nodeTypes: set.NewStringSet("tidb", "pd"),
			instances: set.NewStringSet("123.1.1.2:1234", "123.1.1.4:1234"),
		},
		{
			sql:       "select * from information_schema.cluster_log where type in ('tikv', 'pd')",
			nodeTypes: set.NewStringSet("tikv", "pd"),
			instances: set.NewStringSet(),
		},
		{
			sql:       "select * from information_schema.cluster_log where type in ('tikv', 'pd') and instance='123.1.1.2:1234'",
			nodeTypes: set.NewStringSet("tikv", "pd"),
			instances: set.NewStringSet("123.1.1.2:1234"),
		},
		{
			sql:       "select * from information_schema.cluster_log where type in ('tikv', 'pd') and instance in ('123.1.1.2:1234', '123.1.1.4:1234')",
			nodeTypes: set.NewStringSet("tikv", "pd"),
			instances: set.NewStringSet("123.1.1.2:1234", "123.1.1.4:1234"),
		},
		{
			sql:       "select * from information_schema.cluster_log where type='tikv' and instance in ('123.1.1.2:1234', '123.1.1.4:1234')",
			nodeTypes: set.NewStringSet("tikv"),
			instances: set.NewStringSet("123.1.1.2:1234", "123.1.1.4:1234"),
		},
		{
			sql:       "select * from information_schema.cluster_log where type='tikv' and instance='123.1.1.4:1234'",
			nodeTypes: set.NewStringSet("tikv"),
			instances: set.NewStringSet("123.1.1.4:1234"),
		},
		{
			sql:       "select * from information_schema.cluster_log where type='tikv' and instance='123.1.1.4:1234'",
			nodeTypes: set.NewStringSet("tikv"),
			instances: set.NewStringSet("123.1.1.4:1234"),
		},
		{
			sql:       "select * from information_schema.cluster_log where type='tikv' and instance='cNs2dm.tikv.pingcap.com:1234'",
			nodeTypes: set.NewStringSet("tikv"),
			instances: set.NewStringSet("cNs2dm.tikv.pingcap.com:1234"),
		},
		{
			sql:       "select * from information_schema.cluster_log where type='TIKV' and instance='cNs2dm.tikv.pingcap.com:1234'",
			nodeTypes: set.NewStringSet("tikv"),
			instances: set.NewStringSet("cNs2dm.tikv.pingcap.com:1234"),
		},
		{
			sql:         "select * from information_schema.cluster_log where type='tikv' and type='pd'",
			nodeTypes:   set.NewStringSet(),
			instances:   set.NewStringSet(),
			skipRequest: true,
		},
		{
			sql:       "select * from information_schema.cluster_log where type='tikv' and type in ('pd', 'tikv')",
			nodeTypes: set.NewStringSet("tikv"),
			instances: set.NewStringSet(),
		},
		{
			sql:         "select * from information_schema.cluster_log where type='tikv' and type in ('pd', 'tidb')",
			nodeTypes:   set.NewStringSet(),
			instances:   set.NewStringSet(),
			skipRequest: true,
		},
		{
			sql:       "select * from information_schema.cluster_log where type in ('tikv', 'tidb') and type in ('pd', 'tidb')",
			nodeTypes: set.NewStringSet("tidb"),
			instances: set.NewStringSet(),
		},
		{
			sql:         "select * from information_schema.cluster_log where instance='123.1.1.4:1234' and instance='123.1.1.5:1234'",
			nodeTypes:   set.NewStringSet(),
			instances:   set.NewStringSet(),
			skipRequest: true,
		},
		{
			sql:       "select * from information_schema.cluster_log where instance='123.1.1.4:1234' and instance in ('123.1.1.5:1234', '123.1.1.4:1234')",
			nodeTypes: set.NewStringSet(),
			instances: set.NewStringSet("123.1.1.4:1234"),
		},
		{
			sql:         "select * from information_schema.cluster_log where instance='123.1.1.4:1234' and instance in ('123.1.1.5:1234', '123.1.1.6:1234')",
			nodeTypes:   set.NewStringSet(),
			instances:   set.NewStringSet(),
			skipRequest: true,
		},
		{
			sql:       "select * from information_schema.cluster_log where instance in ('123.1.1.5:1234', '123.1.1.4:1234') and instance in ('123.1.1.5:1234', '123.1.1.6:1234')",
			nodeTypes: set.NewStringSet(),
			instances: set.NewStringSet("123.1.1.5:1234"),
		},
		{
			sql: `select * from information_schema.cluster_log
				where instance in ('123.1.1.5:1234', '123.1.1.4:1234')
				  and instance in ('123.1.1.5:1234', '123.1.1.6:1234')
				  and type in ('tikv', 'tidb')
				  and type in ('pd', 'tidb')`,
			nodeTypes: set.NewStringSet("tidb"),
			instances: set.NewStringSet("123.1.1.5:1234"),
		},
		{
			sql: `select * from information_schema.cluster_log
				where instance in ('123.1.1.5:1234', '123.1.1.4:1234')
				  and instance in ('123.1.1.5:1234', '123.1.1.6:1234')
				  and instance in ('123.1.1.6:1234', '123.1.1.7:1234')
				  and instance in ('123.1.1.7:1234', '123.1.1.8:1234')`,
			nodeTypes:   set.NewStringSet(),
			instances:   set.NewStringSet(),
			skipRequest: true,
		},
		{
			sql:       "select * from information_schema.cluster_log where time='2019-10-10 10:10:10'",
			nodeTypes: set.NewStringSet(),
			instances: set.NewStringSet(),
			startTime: timestamp(t, "2019-10-10 10:10:10"),
			endTime:   timestamp(t, "2019-10-10 10:10:10"),
		},
		{
			sql:       "select * from information_schema.cluster_log where time>='2019-10-10 10:10:10' and time<='2019-10-11 10:10:10'",
			nodeTypes: set.NewStringSet(),
			instances: set.NewStringSet(),
			startTime: timestamp(t, "2019-10-10 10:10:10"),
			endTime:   timestamp(t, "2019-10-11 10:10:10"),
		},
		{
			sql:       "select * from information_schema.cluster_log where time>'2019-10-10 10:10:10' and time<'2019-10-11 10:10:10'",
			nodeTypes: set.NewStringSet(),
			instances: set.NewStringSet(),
			startTime: timestamp(t, "2019-10-10 10:10:10") + 1,
			endTime:   timestamp(t, "2019-10-11 10:10:10") - 1,
		},
		{
			sql:       "select * from information_schema.cluster_log where time>='2019-10-10 10:10:10' and time<'2019-10-11 10:10:10'",
			nodeTypes: set.NewStringSet(),
			instances: set.NewStringSet(),
			startTime: timestamp(t, "2019-10-10 10:10:10"),
			endTime:   timestamp(t, "2019-10-11 10:10:10") - 1,
		},
		{
			sql:         "select * from information_schema.cluster_log where time>='2019-10-12 10:10:10' and time<'2019-10-11 10:10:10'",
			nodeTypes:   set.NewStringSet(),
			instances:   set.NewStringSet(),
			startTime:   timestamp(t, "2019-10-12 10:10:10"),
			endTime:     timestamp(t, "2019-10-11 10:10:10") - 1,
			skipRequest: true,
		},
		{
			sql:       "select * from information_schema.cluster_log where time>='2019-10-10 10:10:10'",
			nodeTypes: set.NewStringSet(),
			instances: set.NewStringSet(),
			startTime: timestamp(t, "2019-10-10 10:10:10"),
		},
		{
			sql:       "select * from information_schema.cluster_log where time>='2019-10-10 10:10:10' and  time>='2019-10-11 10:10:10' and  time>='2019-10-12 10:10:10'",
			nodeTypes: set.NewStringSet(),
			instances: set.NewStringSet(),
			startTime: timestamp(t, "2019-10-12 10:10:10"),
		},
		{
			sql:       "select * from information_schema.cluster_log where time>='2019-10-10 10:10:10' and  time>='2019-10-11 10:10:10' and  time>='2019-10-12 10:10:10' and time='2019-10-13 10:10:10'",
			nodeTypes: set.NewStringSet(),
			instances: set.NewStringSet(),
			startTime: timestamp(t, "2019-10-13 10:10:10"),
			endTime:   timestamp(t, "2019-10-13 10:10:10"),
		},
		{
			sql:         "select * from information_schema.cluster_log where time<='2019-10-10 10:10:10' and time='2019-10-13 10:10:10'",
			nodeTypes:   set.NewStringSet(),
			instances:   set.NewStringSet(),
			startTime:   timestamp(t, "2019-10-13 10:10:10"),
			endTime:     timestamp(t, "2019-10-10 10:10:10"),
			skipRequest: true,
		},
		{
			sql:       "select * from information_schema.cluster_log where time='2019-10-10 10:10:10' and time<='2019-10-13 10:10:10'",
			nodeTypes: set.NewStringSet(),
			instances: set.NewStringSet(),
			startTime: timestamp(t, "2019-10-10 10:10:10"),
			endTime:   timestamp(t, "2019-10-10 10:10:10"),
		},
		{
			sql:       "select * from information_schema.cluster_log where time>='2019-10-10 10:10:10' and message like '%a%'",
			nodeTypes: set.NewStringSet(),
			instances: set.NewStringSet(),
			startTime: timestamp(t, "2019-10-10 10:10:10"),
			patterns:  []string{".*a.*"},
		},
		{
			sql:       "select * from information_schema.cluster_log where message like '%a%' and message regexp '^b'",
			nodeTypes: set.NewStringSet(),
			instances: set.NewStringSet(),
			patterns:  []string{".*a.*", "^b"},
		},
		{
			sql:       "select * from information_schema.cluster_log where message='gc'",
			nodeTypes: set.NewStringSet(),
			instances: set.NewStringSet(),
			patterns:  []string{"^gc$"},
		},
		{
			sql:       "select * from information_schema.cluster_log where message='.*txn.*'",
			nodeTypes: set.NewStringSet(),
			instances: set.NewStringSet(),
			patterns:  []string{"^" + regexp.QuoteMeta(".*txn.*") + "$"},
		},
		{
			sql: `select * from information_schema.cluster_log
				where instance in ('123.1.1.5:1234', '123.1.1.4:1234')
				  and (type='tidb' or type='pd')
				  and message like '%coprocessor%'
				  and message regexp '.*txn=123.*'
				  and level in ('debug', 'info', 'ERROR')`,
			nodeTypes: set.NewStringSet("tidb", "pd"),
			instances: set.NewStringSet("123.1.1.5:1234", "123.1.1.4:1234"),
			level:     set.NewStringSet("debug", "info", "error"),
			patterns:  []string{".*coprocessor.*", ".*txn=123.*"},
		},
		{
			sql:       "select * from information_schema.cluster_log where (message regexp '.*pd.*' or message regexp '.*tidb.*' or message like '%tikv%')",
			nodeTypes: set.NewStringSet(),
			instances: set.NewStringSet(),
			patterns:  []string{".*pd.*|.*tidb.*|.*tikv.*"},
		},
		{
			sql:       "select * from information_schema.cluster_log where (level = 'debug' or level = 'ERROR')",
			nodeTypes: set.NewStringSet(),
			instances: set.NewStringSet(),
			level:     set.NewStringSet("debug", "error"),
		},
	}
	for _, ca := range cases {
		logicalMemTable, ok := getLogicalMemTable(t, dom, se, parser, ca.sql)
		if ok {
			require.NotNil(t, logicalMemTable.Extractor, ca.sql)

			clusterConfigExtractor := logicalMemTable.Extractor.(*plannercore.ClusterLogTableExtractor)
			require.EqualValues(t, ca.nodeTypes, clusterConfigExtractor.NodeTypes, "SQL: %v", ca.sql)
			require.EqualValues(t, ca.instances, clusterConfigExtractor.Instances, "SQL: %v", ca.sql)
			require.EqualValues(t, ca.skipRequest, clusterConfigExtractor.SkipRequest, "SQL: %v", ca.sql)
			if ca.startTime > 0 {
				require.Equal(t, ca.startTime, clusterConfigExtractor.StartTime, "SQL: %v", ca.sql)
			}
			if ca.endTime > 0 {
				require.Equal(t, ca.endTime, clusterConfigExtractor.EndTime, "SQL: %v", ca.sql)
			}
			require.EqualValues(t, ca.patterns, clusterConfigExtractor.Patterns, "SQL: %v", ca.sql)
			if len(ca.level) > 0 {
				require.EqualValues(t, ca.level, clusterConfigExtractor.LogLevels, "SQL: %v", ca.sql)
			}
		} else {
			require.True(t, ca.skipRequest, ca.sql)
		}
	}
}

func TestMetricTableExtractor(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)

	parseTime := func(t *testing.T, s string) time.Time {
		tt, err := time.ParseInLocation(util.MetricTableTimeFormat, s, time.Local)
		require.NoError(t, err)
		return tt
	}

	parser := parser.New()
	var cases = []struct {
		sql                string
		skipRequest        bool
		startTime, endTime time.Time
		labelConditions    map[string]set.StringSet
		quantiles          []float64
		promQL             string
	}{
		{
			sql:    "select * from metrics_schema.tidb_query_duration",
			promQL: "histogram_quantile(0.9, sum(rate(tidb_server_handle_query_duration_seconds_bucket{}[60s])) by (le,sql_type,instance))",
		},
		{
			sql:    "select * from metrics_schema.tidb_query_duration where instance='127.0.0.1:10080'",
			promQL: `histogram_quantile(0.9, sum(rate(tidb_server_handle_query_duration_seconds_bucket{instance="127.0.0.1:10080"}[60s])) by (le,sql_type,instance))`,
			labelConditions: map[string]set.StringSet{
				"instance": set.NewStringSet("127.0.0.1:10080"),
			},
		},
		{
			sql:    "select * from metrics_schema.tidb_query_duration where instance='127.0.0.1:10080' or instance='127.0.0.1:10081'",
			promQL: `histogram_quantile(0.9, sum(rate(tidb_server_handle_query_duration_seconds_bucket{instance=~"127.0.0.1:10080|127.0.0.1:10081"}[60s])) by (le,sql_type,instance))`,
			labelConditions: map[string]set.StringSet{
				"instance": set.NewStringSet("127.0.0.1:10080", "127.0.0.1:10081"),
			},
		},
		{
			sql:    "select * from metrics_schema.tidb_query_duration where instance='127.0.0.1:10080' and sql_type='general'",
			promQL: `histogram_quantile(0.9, sum(rate(tidb_server_handle_query_duration_seconds_bucket{instance="127.0.0.1:10080",sql_type="general"}[60s])) by (le,sql_type,instance))`,
			labelConditions: map[string]set.StringSet{
				"instance": set.NewStringSet("127.0.0.1:10080"),
				"sql_type": set.NewStringSet("general"),
			},
		},
		{
			sql:    "select * from metrics_schema.tidb_query_duration where instance='127.0.0.1:10080' or sql_type='general'",
			promQL: `histogram_quantile(0.9, sum(rate(tidb_server_handle_query_duration_seconds_bucket{}[60s])) by (le,sql_type,instance))`,
		},
		{
			sql:    "select * from metrics_schema.tidb_query_duration where instance='127.0.0.1:10080' and sql_type='Update' and time='2019-10-10 10:10:10'",
			promQL: `histogram_quantile(0.9, sum(rate(tidb_server_handle_query_duration_seconds_bucket{instance="127.0.0.1:10080",sql_type="Update"}[60s])) by (le,sql_type,instance))`,
			labelConditions: map[string]set.StringSet{
				"instance": set.NewStringSet("127.0.0.1:10080"),
				"sql_type": set.NewStringSet("Update"),
			},
			startTime: parseTime(t, "2019-10-10 10:10:10"),
			endTime:   parseTime(t, "2019-10-10 10:10:10"),
		},
		{
			sql:       "select * from metrics_schema.tidb_query_duration where time>'2019-10-10 10:10:10' and time<'2019-10-11 10:10:10'",
			promQL:    `histogram_quantile(0.9, sum(rate(tidb_server_handle_query_duration_seconds_bucket{}[60s])) by (le,sql_type,instance))`,
			startTime: parseTime(t, "2019-10-10 10:10:10.001"),
			endTime:   parseTime(t, "2019-10-11 10:10:09.999"),
		},
		{
			sql:       "select * from metrics_schema.tidb_query_duration where time>='2019-10-10 10:10:10'",
			promQL:    `histogram_quantile(0.9, sum(rate(tidb_server_handle_query_duration_seconds_bucket{}[60s])) by (le,sql_type,instance))`,
			startTime: parseTime(t, "2019-10-10 10:10:10"),
			endTime:   parseTime(t, "2019-10-10 10:20:10"),
		},
		{
			sql:         "select * from metrics_schema.tidb_query_duration where time>='2019-10-10 10:10:10' and time<='2019-10-09 10:10:10'",
			promQL:      "",
			startTime:   parseTime(t, "2019-10-10 10:10:10"),
			endTime:     parseTime(t, "2019-10-09 10:10:10"),
			skipRequest: true,
		},
		{
			sql:       "select * from metrics_schema.tidb_query_duration where time<='2019-10-09 10:10:10'",
			promQL:    "histogram_quantile(0.9, sum(rate(tidb_server_handle_query_duration_seconds_bucket{}[60s])) by (le,sql_type,instance))",
			startTime: parseTime(t, "2019-10-09 10:00:10"),
			endTime:   parseTime(t, "2019-10-09 10:10:10"),
		},
		{
			sql: "select * from metrics_schema.tidb_query_duration where quantile=0.9 or quantile=0.8",
			promQL: "histogram_quantile(0.8, sum(rate(tidb_server_handle_query_duration_seconds_bucket{}[60s])) by (le,sql_type,instance))," +
				"histogram_quantile(0.9, sum(rate(tidb_server_handle_query_duration_seconds_bucket{}[60s])) by (le,sql_type,instance))",
			quantiles: []float64{0.8, 0.9},
		},
		{
			sql:       "select * from metrics_schema.tidb_query_duration where quantile=0",
			promQL:    "histogram_quantile(0, sum(rate(tidb_server_handle_query_duration_seconds_bucket{}[60s])) by (le,sql_type,instance))",
			quantiles: []float64{0},
		},
	}
	se.GetSessionVars().TimeZone = time.Local
	se.GetSessionVars().StmtCtx.SetTimeZone(time.Local)
	for _, ca := range cases {
		logicalMemTable, ok := getLogicalMemTable(t, dom, se, parser, ca.sql)
		if ok {
			require.NotNil(t, logicalMemTable.Extractor)
			metricTableExtractor := logicalMemTable.Extractor.(*plannercore.MetricTableExtractor)
			if len(ca.labelConditions) > 0 {
				require.EqualValues(t, ca.labelConditions, metricTableExtractor.LabelConditions, "SQL: %v", ca.sql)
			}
			require.EqualValues(t, ca.skipRequest, metricTableExtractor.SkipRequest, "SQL: %v", ca.sql)
			if len(metricTableExtractor.Quantiles) > 0 {
				require.EqualValues(t, ca.quantiles, metricTableExtractor.Quantiles)
			}
			if !ca.skipRequest {
				promQL := metricTableExtractor.GetMetricTablePromQL(se.GetPlanCtx(), "tidb_query_duration")
				require.EqualValues(t, promQL, ca.promQL, "SQL: %v", ca.sql)
				start, end := metricTableExtractor.StartTime, metricTableExtractor.EndTime
				require.GreaterOrEqual(t, end.UnixNano(), start.UnixNano())
				if ca.startTime.Unix() > 0 {
					require.EqualValues(t, ca.startTime, metricTableExtractor.StartTime, "SQL: %v, start_time: %v", ca.sql, metricTableExtractor.StartTime)
				}
				if ca.endTime.Unix() > 0 {
					require.EqualValues(t, ca.endTime, metricTableExtractor.EndTime, "SQL: %v, end_time: %v", ca.sql, metricTableExtractor.EndTime)
				}
			}
		} else {
			require.True(t, ca.skipRequest, ca.sql)
		}
	}
}

func TestMetricsSummaryTableExtractor(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)

	var cases = []struct {
		sql         string
		names       set.StringSet
		quantiles   []float64
		skipRequest bool
	}{
		{
			sql: "select * from information_schema.metrics_summary",
		},
		{
			sql:       "select * from information_schema.metrics_summary where quantile='0.999'",
			quantiles: []float64{0.999},
		},
		{
			sql:       "select * from information_schema.metrics_summary where '0.999'=quantile or quantile='0.95'",
			quantiles: []float64{0.999, 0.95},
		},
		{
			sql:       "select * from information_schema.metrics_summary where '0.999'=quantile or quantile='0.95' or quantile='0.99'",
			quantiles: []float64{0.999, 0.95, 0.99},
		},
		{
			sql:       "select * from information_schema.metrics_summary where (quantile='0.95' or quantile='0.99') and (metrics_name='metric_name3' or metrics_name='metric_name1')",
			quantiles: []float64{0.95, 0.99},
			names:     set.NewStringSet("metric_name3", "metric_name1"),
		},
		{
			sql:       "select * from information_schema.metrics_summary where quantile in ('0.999', '0.99')",
			quantiles: []float64{0.999, 0.99},
		},
		{
			sql:       "select * from information_schema.metrics_summary where quantile in ('0.999', '0.99') and metrics_name='metric_name1'",
			quantiles: []float64{0.999, 0.99},
			names:     set.NewStringSet("metric_name1"),
		},
		{
			sql:       "select * from information_schema.metrics_summary where quantile in ('0.999', '0.99') and metrics_name in ('metric_name1', 'metric_name2')",
			quantiles: []float64{0.999, 0.99},
			names:     set.NewStringSet("metric_name1", "metric_name2"),
		},
		{
			sql:       "select * from information_schema.metrics_summary where quantile='0.999' and metrics_name in ('metric_name1', 'metric_name2')",
			quantiles: []float64{0.999},
			names:     set.NewStringSet("metric_name1", "metric_name2"),
		},
		{
			sql:       "select * from information_schema.metrics_summary where quantile='0.999' and metrics_name='metric_NAME3'",
			quantiles: []float64{0.999},
			names:     set.NewStringSet("metric_name3"),
		},
		{
			sql:       "select * from information_schema.metrics_summary where quantile='0.999' and quantile in ('0.99', '0.999')",
			quantiles: []float64{0.999},
		},
		{
			sql:       "select * from information_schema.metrics_summary where quantile in ('0.999', '0.95') and quantile in ('0.99', '0.95')",
			quantiles: []float64{0.95},
		},
		{
			sql: `select * from information_schema.metrics_summary
				where metrics_name in ('metric_name1', 'metric_name4')
				  and metrics_name in ('metric_name5', 'metric_name4')
				  and quantile in ('0.999', '0.95')
				  and quantile in ('0.99', '0.95')
				  and quantile in (0.80, 0.90)`,
			skipRequest: true,
		},
		{
			sql: `select * from information_schema.metrics_summary
				where metrics_name in ('metric_name1', 'metric_name4')
				  and metrics_name in ('metric_name5', 'metric_name4')
				  and metrics_name in ('metric_name5', 'metric_name1')
				  and metrics_name in ('metric_name1', 'metric_name3')`,
			skipRequest: true,
		},
	}
	parser := parser.New()
	for _, ca := range cases {
		sort.Float64s(ca.quantiles)

		logicalMemTable, ok := getLogicalMemTable(t, dom, se, parser, ca.sql)
		if ok {
			require.NotNil(t, logicalMemTable.Extractor)

			extractor := logicalMemTable.Extractor.(*plannercore.MetricSummaryTableExtractor)
			if len(ca.quantiles) > 0 {
				require.EqualValues(t, ca.quantiles, extractor.Quantiles, "SQL: %v", ca.sql)
			}
			if len(ca.names) > 0 {
				require.EqualValues(t, ca.names, extractor.MetricsNames, "SQL: %v", ca.sql)
			}
			require.Equal(t, ca.skipRequest, extractor.SkipRequest, "SQL: %v", ca.sql)
		} else {
			require.True(t, ca.skipRequest, "SQL: %v", ca.sql)
		}
	}
}

func TestInspectionResultTableExtractor(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)

	var cases = []struct {
		sql            string
		rules          set.StringSet
		items          set.StringSet
		skipInspection bool
	}{
		{
			sql: "select * from information_schema.inspection_result",
		},
		{
			sql:   "select * from information_schema.inspection_result where rule='ddl'",
			rules: set.NewStringSet("ddl"),
		},
		{
			sql:   "select * from information_schema.inspection_result where 'ddl'=rule",
			rules: set.NewStringSet("ddl"),
		},
		{
			sql:   "select * from information_schema.inspection_result where 'ddl'=rule",
			rules: set.NewStringSet("ddl"),
		},
		{
			sql:   "select * from information_schema.inspection_result where 'ddl'=rule or rule='config'",
			rules: set.NewStringSet("ddl", "config"),
		},
		{
			sql:   "select * from information_schema.inspection_result where 'ddl'=rule or rule='config' or rule='slow_query'",
			rules: set.NewStringSet("ddl", "config", "slow_query"),
		},
		{
			sql:   "select * from information_schema.inspection_result where (rule='config' or rule='slow_query') and (item='ddl.owner' or item='ddl.lease')",
			rules: set.NewStringSet("config", "slow_query"),
			items: set.NewStringSet("ddl.owner", "ddl.lease"),
		},
		{
			sql:   "select * from information_schema.inspection_result where rule in ('ddl', 'slow_query')",
			rules: set.NewStringSet("ddl", "slow_query"),
		},
		{
			sql:   "select * from information_schema.inspection_result where rule in ('ddl', 'slow_query') and item='ddl.lease'",
			rules: set.NewStringSet("ddl", "slow_query"),
			items: set.NewStringSet("ddl.lease"),
		},
		{
			sql:   "select * from information_schema.inspection_result where rule in ('ddl', 'slow_query') and item in ('ddl.lease', '123.1.1.4:1234')",
			rules: set.NewStringSet("ddl", "slow_query"),
			items: set.NewStringSet("ddl.lease", "123.1.1.4:1234"),
		},
		{
			sql:   "select * from information_schema.inspection_result where rule='ddl' and item in ('ddl.lease', '123.1.1.4:1234')",
			rules: set.NewStringSet("ddl"),
			items: set.NewStringSet("ddl.lease", "123.1.1.4:1234"),
		},
		{
			sql:   "select * from information_schema.inspection_result where rule='ddl' and item='123.1.1.4:1234'",
			rules: set.NewStringSet("ddl"),
			items: set.NewStringSet("123.1.1.4:1234"),
		},
		{
			sql:   "select * from information_schema.inspection_result where rule='ddl' and item='123.1.1.4:1234'",
			rules: set.NewStringSet("ddl"),
			items: set.NewStringSet("123.1.1.4:1234"),
		},
		{
			sql:   "select * from information_schema.inspection_result where rule='ddl' and item='DDL.lease'",
			rules: set.NewStringSet("ddl"),
			items: set.NewStringSet("ddl.lease"),
		},
		{
			sql:   "select * from information_schema.inspection_result where rule='ddl' and item='ddl.OWNER'",
			rules: set.NewStringSet("ddl"),
			items: set.NewStringSet("ddl.owner"),
		},
		{
			sql:            "select * from information_schema.inspection_result where rule='ddl' and rule='slow_query'",
			skipInspection: true,
		},
		{
			sql:   "select * from information_schema.inspection_result where rule='ddl' and rule in ('slow_query', 'ddl')",
			rules: set.NewStringSet("ddl"),
		},
		{
			sql:   "select * from information_schema.inspection_result where rule in ('ddl', 'config') and rule in ('slow_query', 'config')",
			rules: set.NewStringSet("config"),
		},
		{
			sql:            "select * from information_schema.inspection_result where item='ddl.lease' and item='raftstore.threadpool'",
			skipInspection: true,
		},
		{
			sql:   "select * from information_schema.inspection_result where item='raftstore.threadpool' and item in ('raftstore.threadpool', 'ddl.lease')",
			items: set.NewStringSet("raftstore.threadpool"),
		},
		{
			sql:            "select * from information_schema.inspection_result where item='raftstore.threadpool' and item in ('ddl.lease', 'scheduler.limit')",
			skipInspection: true,
		},
		{
			sql:   "select * from information_schema.inspection_result where item in ('ddl.lease', 'scheduler.limit') and item in ('raftstore.threadpool', 'scheduler.limit')",
			items: set.NewStringSet("scheduler.limit"),
		},
		{
			sql: `select * from information_schema.inspection_result
				where item in ('ddl.lease', 'scheduler.limit')
				  and item in ('raftstore.threadpool', 'scheduler.limit')
				  and rule in ('ddl', 'config')
				  and rule in ('slow_query', 'config')`,
			rules: set.NewStringSet("config"),
			items: set.NewStringSet("scheduler.limit"),
		},
		{
			sql: `select * from information_schema.inspection_result
				where item in ('ddl.lease', 'scheduler.limit')
				  and item in ('raftstore.threadpool', 'scheduler.limit')
				  and item in ('raftstore.threadpool', 'ddl.lease')
				  and item in ('ddl.lease', 'ddl.owner')`,
			skipInspection: true,
		},
	}
	parser := parser.New()
	for _, ca := range cases {
		logicalMemTable, ok := getLogicalMemTable(t, dom, se, parser, ca.sql)
		if ok {
			require.NotNil(t, logicalMemTable.Extractor)

			clusterConfigExtractor := logicalMemTable.Extractor.(*plannercore.InspectionResultTableExtractor)
			if len(ca.rules) > 0 {
				require.EqualValues(t, ca.rules, clusterConfigExtractor.Rules, "SQL: %v", ca.sql)
			}
			if len(ca.items) > 0 {
				require.EqualValues(t, ca.items, clusterConfigExtractor.Items, "SQL: %v", ca.sql)
			}
			require.Equal(t, ca.skipInspection, clusterConfigExtractor.SkipInspection, "SQL: %v", ca.sql)
		} else {
			require.True(t, ca.skipInspection, "SQL: %v", ca.sql)
		}
	}
}

func TestInspectionSummaryTableExtractor(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)

	var cases = []struct {
		sql            string
		rules          set.StringSet
		names          set.StringSet
		quantiles      set.Float64Set
		skipInspection bool
	}{
		{
			sql: "select * from information_schema.inspection_summary",
		},
		{
			sql:   "select * from information_schema.inspection_summary where rule='ddl'",
			rules: set.NewStringSet("ddl"),
		},
		{
			sql:   "select * from information_schema.inspection_summary where 'ddl'=rule or rule='config'",
			rules: set.NewStringSet("ddl", "config"),
		},
		{
			sql:   "select * from information_schema.inspection_summary where 'ddl'=rule or rule='config' or rule='slow_query'",
			rules: set.NewStringSet("ddl", "config", "slow_query"),
		},
		{
			sql:   "select * from information_schema.inspection_summary where (rule='config' or rule='slow_query') and (metrics_name='metric_name3' or metrics_name='metric_name1')",
			rules: set.NewStringSet("config", "slow_query"),
			names: set.NewStringSet("metric_name3", "metric_name1"),
		},
		{
			sql:   "select * from information_schema.inspection_summary where rule in ('ddl', 'slow_query')",
			rules: set.NewStringSet("ddl", "slow_query"),
		},
		{
			sql:   "select * from information_schema.inspection_summary where rule in ('ddl', 'slow_query') and metrics_name='metric_name1'",
			rules: set.NewStringSet("ddl", "slow_query"),
			names: set.NewStringSet("metric_name1"),
		},
		{
			sql:   "select * from information_schema.inspection_summary where rule in ('ddl', 'slow_query') and metrics_name in ('metric_name1', 'metric_name2')",
			rules: set.NewStringSet("ddl", "slow_query"),
			names: set.NewStringSet("metric_name1", "metric_name2"),
		},
		{
			sql:   "select * from information_schema.inspection_summary where rule='ddl' and metrics_name in ('metric_name1', 'metric_name2')",
			rules: set.NewStringSet("ddl"),
			names: set.NewStringSet("metric_name1", "metric_name2"),
		},
		{
			sql:   "select * from information_schema.inspection_summary where rule='ddl' and metrics_name='metric_NAME3'",
			rules: set.NewStringSet("ddl"),
			names: set.NewStringSet("metric_name3"),
		},
		{
			sql:   "select * from information_schema.inspection_summary where rule='ddl' and rule in ('slow_query', 'ddl')",
			rules: set.NewStringSet("ddl"),
		},
		{
			sql:   "select * from information_schema.inspection_summary where rule in ('ddl', 'config') and rule in ('slow_query', 'config')",
			rules: set.NewStringSet("config"),
		},
		{
			sql: `select * from information_schema.inspection_summary
				where metrics_name in ('metric_name1', 'metric_name4')
				  and metrics_name in ('metric_name5', 'metric_name4')
				  and rule in ('ddl', 'config')
				  and rule in ('slow_query', 'config')
				  and quantile in (0.80, 0.90)`,
			rules:     set.NewStringSet("config"),
			names:     set.NewStringSet("metric_name4"),
			quantiles: set.NewFloat64Set(0.80, 0.90),
		},
		{
			sql: `select * from information_schema.inspection_summary
				where metrics_name in ('metric_name1', 'metric_name4')
				  and metrics_name in ('metric_name5', 'metric_name4')
				  and metrics_name in ('metric_name5', 'metric_name1')
				  and metrics_name in ('metric_name1', 'metric_name3')`,
			skipInspection: true,
		},
	}
	parser := parser.New()
	for _, ca := range cases {
		logicalMemTable, ok := getLogicalMemTable(t, dom, se, parser, ca.sql)
		if ok {
			require.NotNil(t, logicalMemTable.Extractor)

			clusterConfigExtractor := logicalMemTable.Extractor.(*plannercore.InspectionSummaryTableExtractor)
			if len(ca.rules) > 0 {
				require.EqualValues(t, ca.rules, clusterConfigExtractor.Rules, "SQL: %v", ca.sql)
			}
			if len(ca.names) > 0 {
				require.EqualValues(t, ca.names, clusterConfigExtractor.MetricNames, "SQL: %v", ca.sql)
			}
			require.Equal(t, ca.skipInspection, clusterConfigExtractor.SkipInspection, "SQL: %v", ca.sql)
		} else {
			require.True(t, ca.skipInspection, "SQL: %v", ca.sql)
		}
	}
}

func TestInspectionRuleTableExtractor(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)

	var cases = []struct {
		sql  string
		tps  set.StringSet
		skip bool
	}{
		{
			sql: "select * from information_schema.inspection_rules",
		},
		{
			sql: "select * from information_schema.inspection_rules where type='inspection'",
			tps: set.NewStringSet("inspection"),
		},
		{
			sql: "select * from information_schema.inspection_rules where type='inspection' or type='summary'",
			tps: set.NewStringSet("inspection", "summary"),
		},
		{
			sql:  "select * from information_schema.inspection_rules where type='inspection' and type='summary'",
			skip: true,
		},
	}
	parser := parser.New()
	for _, ca := range cases {
		logicalMemTable, ok := getLogicalMemTable(t, dom, se, parser, ca.sql)
		if ok {
			require.NotNil(t, logicalMemTable.Extractor, ca.sql)
			clusterConfigExtractor := logicalMemTable.Extractor.(*plannercore.InspectionRuleTableExtractor)
			if len(ca.tps) > 0 {
				require.EqualValues(t, ca.tps, clusterConfigExtractor.Types, "SQL: %v", ca.sql)
			}
			require.Equal(t, ca.skip, clusterConfigExtractor.SkipRequest, "SQL: %v", ca.sql)
		} else {
			require.True(t, ca.skip, ca.sql)
		}
	}
}

func TestTiDBHotRegionsHistoryTableExtractor(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	se.GetSessionVars().TimeZone = time.Local
	se.GetSessionVars().StmtCtx.SetTimeZone(time.Local)

	var cases = []struct {
		sql                          string
		skipRequest                  bool
		startTime, endTime           int64
		regionIDs, storeIDs, peerIDs []uint64
		isLearners, isLeaders        []bool
		hotRegionTypes               set.StringSet
	}{
		// Test full data, it will not call Extract() and executor(retriver) will panic and remind user to add conditions to save network IO.
		{
			sql: "select * from information_schema.tidb_hot_regions_history",
		},
		// Test startTime and endTime.
		{
			// Test for invalid update_time.
			sql: "select * from information_schema.tidb_hot_regions_history where update_time='2019-10-10 10::10'",
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where update_time='2019-10-10 10:10:10'",
			startTime:      timestamp(t, "2019-10-10 10:10:10"),
			endTime:        timestamp(t, "2019-10-10 10:10:10"),
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where update_time>='2019-10-10 10:10:10' and update_time<='2019-10-11 10:10:10'",
			startTime:      timestamp(t, "2019-10-10 10:10:10"),
			endTime:        timestamp(t, "2019-10-11 10:10:10"),
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where update_time>'2019-10-10 10:10:10' and update_time<'2019-10-11 10:10:10'",
			startTime:      timestamp(t, "2019-10-10 10:10:10") + 1,
			endTime:        timestamp(t, "2019-10-11 10:10:10") - 1,
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where update_time>='2019-10-10 10:10:10' and update_time<'2019-10-11 10:10:10'",
			startTime:      timestamp(t, "2019-10-10 10:10:10"),
			endTime:        timestamp(t, "2019-10-11 10:10:10") - 1,
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where update_time>='2019-10-12 10:10:10' and update_time<'2019-10-11 10:10:10'",
			startTime:      timestamp(t, "2019-10-12 10:10:10"),
			endTime:        timestamp(t, "2019-10-11 10:10:10") - 1,
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
			skipRequest:    true,
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where update_time>='2019-10-10 10:10:10'",
			startTime:      timestamp(t, "2019-10-10 10:10:10"),
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where update_time>='2019-10-10 10:10:10' and  update_time>='2019-10-11 10:10:10' and  update_time>='2019-10-12 10:10:10'",
			startTime:      timestamp(t, "2019-10-12 10:10:10"),
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where update_time>='2019-10-10 10:10:10' and  update_time>='2019-10-11 10:10:10' and  update_time>='2019-10-12 10:10:10' and update_time='2019-10-13 10:10:10'",
			startTime:      timestamp(t, "2019-10-13 10:10:10"),
			endTime:        timestamp(t, "2019-10-13 10:10:10"),
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where update_time<='2019-10-10 10:10:10' and update_time='2019-10-13 10:10:10'",
			startTime:      timestamp(t, "2019-10-13 10:10:10"),
			endTime:        timestamp(t, "2019-10-10 10:10:10"),
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
			skipRequest:    true,
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where update_time='2019-10-10 10:10:10' and update_time<='2019-10-13 10:10:10'",
			startTime:      timestamp(t, "2019-10-10 10:10:10"),
			endTime:        timestamp(t, "2019-10-10 10:10:10"),
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},

		// Test `region_id`, `store_id`, `peer_id` columns.
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where region_id=100",
			regionIDs:      []uint64{100},
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where 100=region_id",
			regionIDs:      []uint64{100},
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where 100=region_id or region_id=101",
			regionIDs:      []uint64{100, 101},
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where 100=region_id or region_id=101 or region_id=102 or 103 = region_id",
			regionIDs:      []uint64{100, 101, 102, 103},
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where (region_id=100 or region_id=101) and (store_id=200 or store_id=201)",
			regionIDs:      []uint64{100, 101},
			storeIDs:       []uint64{200, 201},
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where region_id in (100, 101)",
			regionIDs:      []uint64{100, 101},
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where region_id in (100, 101) and store_id=200",
			regionIDs:      []uint64{100, 101},
			storeIDs:       []uint64{200},
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where region_id in (100, 101) and store_id in (200, 201)",
			regionIDs:      []uint64{100, 101},
			storeIDs:       []uint64{200, 201},
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where region_id=100 and store_id in (200, 201)",
			regionIDs:      []uint64{100},
			storeIDs:       []uint64{200, 201},
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where region_id=100 and store_id=200",
			regionIDs:      []uint64{100},
			storeIDs:       []uint64{200},
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql:         "select * from information_schema.tidb_hot_regions_history where region_id=100 and region_id=101",
			skipRequest: true,
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where region_id=100 and region_id in (100,101)",
			regionIDs:      []uint64{100},
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where region_id=100 and region_id in (100,101) and store_id=200 and store_id in (200,201)",
			regionIDs:      []uint64{100},
			storeIDs:       []uint64{200},
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql:         "select * from information_schema.tidb_hot_regions_history where region_id=100 and region_id in (101,102)",
			skipRequest: true,
		},
		{
			sql:         "select * from information_schema.tidb_hot_regions_history where region_id=100 and region_id in (101,102) and store_id=200 and store_id in (200,201)",
			skipRequest: true,
		},
		{
			sql:         "select * from information_schema.tidb_hot_regions_history where region_id=100 and region_id in (100,101) and store_id=200 and store_id in (201,202)",
			skipRequest: true,
		},
		{
			sql: `select * from information_schema.tidb_hot_regions_history
								where region_id=100 and region_id in (100,101)
								and store_id=200 and store_id in (201,202)`,
			skipRequest: true,
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where region_id in (100,101) and region_id in (101,102)",
			regionIDs:      []uint64{101},
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql: `select * from information_schema.tidb_hot_regions_history
							where region_id in (100,101)
							and region_id in (101,102)
							and store_id in (200,201)
							and store_id in (201,202)`,
			regionIDs:      []uint64{101},
			storeIDs:       []uint64{201},
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql: `select * from information_schema.tidb_hot_regions_history
							where region_id in (100,101)
							and region_id in (101,102)
							and store_id in (200,201)
							and store_id in (201,202)
							and peer_id in (3000,3001)
							and peer_id in (3001,3002)`,
			regionIDs:      []uint64{101},
			storeIDs:       []uint64{201},
			peerIDs:        []uint64{3001},
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql: `select * from information_schema.tidb_hot_regions_history
							where region_id in (100,101)
							and region_id in (100,102)
							and region_id in (102,103)
							and region_id in (103,104)`,
			skipRequest: true,
		},
		// Test `type` column.
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where type='read'",
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead),
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where type in('read')",
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead),
		},
		{
			sql:         "select * from information_schema.tidb_hot_regions_history where type='read' and type='write'",
			skipRequest: true,
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where type in ('read', 'write')",
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where type='read' and type in ('read', 'write')",
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead),
		},
		{
			sql:         "select * from information_schema.tidb_hot_regions_history where type in ('read') and type in ('write')",
			skipRequest: true,
		},
		// Test `is_learner`, `is_leaeder` columns.
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where is_learner=1",
			isLearners:     []bool{true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where is_leader=0",
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where is_learner=true",
			isLearners:     []bool{true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql:        "select * from information_schema.tidb_hot_regions_history where is_learner in(0,1)",
			isLearners: []bool{false, true},
			isLeaders:  []bool{false, true},
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where is_learner in(true,false)",
			isLearners:     []bool{false, true},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where is_learner in(3,4)",
			isLearners:     []bool{false},
			isLeaders:      []bool{false, true},
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
		},
		{
			sql:            "select * from information_schema.tidb_hot_regions_history where is_learner in(3,4) and is_leader in(0,1,true,false,3,4)",
			hotRegionTypes: set.NewStringSet(plannercore.HotRegionTypeRead, plannercore.HotRegionTypeWrite),
			isLearners:     []bool{false},
			isLeaders:      []bool{false, true},
		},
		{
			sql:         "select * from information_schema.tidb_hot_regions_history where is_learner=1 and is_learner=0",
			skipRequest: true,
		},
		{
			sql:         "select * from information_schema.tidb_hot_regions_history where is_learner=3 and is_learner=false",
			skipRequest: true,
		},
		{
			sql:         "select * from information_schema.tidb_hot_regions_history where is_learner=3 and is_learner=4",
			skipRequest: true,
		},
	}

	parser := parser.New()
	for _, ca := range cases {
		logicalMemTable, ok := getLogicalMemTable(t, dom, se, parser, ca.sql)
		if ok {
			require.NotNil(t, logicalMemTable.Extractor, "SQL: %v", ca.sql)

			hotRegionsHistoryExtractor := logicalMemTable.Extractor.(*plannercore.HotRegionsHistoryTableExtractor)
			if ca.startTime > 0 {
				require.Equal(t, ca.startTime, hotRegionsHistoryExtractor.StartTime, "SQL: %v", ca.sql)
			}
			if ca.endTime > 0 {
				require.Equal(t, ca.endTime, hotRegionsHistoryExtractor.EndTime, "SQL: %v", ca.sql)
			}
			require.EqualValues(t, ca.skipRequest, hotRegionsHistoryExtractor.SkipRequest, "SQL: %v", ca.sql)
			if len(ca.isLearners) > 0 {
				require.EqualValues(t, ca.isLearners, hotRegionsHistoryExtractor.IsLearners, "SQL: %v", ca.sql)
			}
			if len(ca.isLeaders) > 0 {
				require.EqualValues(t, ca.isLeaders, hotRegionsHistoryExtractor.IsLeaders, "SQL: %v", ca.sql)
			}
			if ca.hotRegionTypes.Count() > 0 {
				require.EqualValues(t, ca.hotRegionTypes, hotRegionsHistoryExtractor.HotRegionTypes, "SQL: %v", ca.sql)
			}
			if len(ca.regionIDs) > 0 {
				require.EqualValues(t, ca.regionIDs, hotRegionsHistoryExtractor.RegionIDs, "SQL: %v", ca.sql)
			}
			if len(ca.storeIDs) > 0 {
				require.EqualValues(t, ca.storeIDs, hotRegionsHistoryExtractor.StoreIDs, "SQL: %v", ca.sql)
			}
			if len(ca.peerIDs) > 0 {
				require.EqualValues(t, ca.peerIDs, hotRegionsHistoryExtractor.PeerIDs, "SQL: %v", ca.sql)
			}
		} else {
			require.True(t, ca.skipRequest, ca.sql)
		}
	}
}

func TestTikvRegionPeersExtractor(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)

	var cases = []struct {
		sql                 string
		regionIDs, storeIDs []uint64
		skipRequest         bool
	}{
		// Test `region_id`, `store_id` columns.
		{
			sql:       "select * from information_schema.tikv_region_peers where region_id=100",
			regionIDs: []uint64{100},
		},
		{
			sql:       "select * from information_schema.tikv_region_peers where 100=region_id",
			regionIDs: []uint64{100},
		},
		{
			sql:       "select * from information_schema.tikv_region_peers where 100=region_id or region_id=101",
			regionIDs: []uint64{100, 101},
		},
		{
			sql:       "select * from information_schema.tikv_region_peers where 100=region_id or region_id=101 or region_id=102 or 103 = region_id",
			regionIDs: []uint64{100, 101, 102, 103},
		},
		{
			sql:       "select * from information_schema.tikv_region_peers where (region_id=100 or region_id=101) and (store_id=200 or store_id=201)",
			regionIDs: []uint64{100, 101},
			storeIDs:  []uint64{200, 201},
		},
		{
			sql:       "select * from information_schema.tikv_region_peers where region_id in (100, 101)",
			regionIDs: []uint64{100, 101},
		},
		{
			sql:       "select * from information_schema.tikv_region_peers where region_id in (100, 101) and store_id=200",
			regionIDs: []uint64{100, 101},
			storeIDs:  []uint64{200},
		},
		{
			sql:       "select * from information_schema.tikv_region_peers where region_id in (100, 101) and store_id in (200, 201)",
			regionIDs: []uint64{100, 101},
			storeIDs:  []uint64{200, 201},
		},
		{
			sql:       "select * from information_schema.tikv_region_peers where region_id=100 and store_id in (200, 201)",
			regionIDs: []uint64{100},
			storeIDs:  []uint64{200, 201},
		},
		{
			sql:       "select * from information_schema.tikv_region_peers where region_id=100 and store_id=200",
			regionIDs: []uint64{100},
			storeIDs:  []uint64{200},
		},
		{
			sql:         "select * from information_schema.tikv_region_peers where region_id=100 and region_id=101",
			skipRequest: true,
		},
		{
			sql:       "select * from information_schema.tikv_region_peers where region_id=100 and region_id in (100,101)",
			regionIDs: []uint64{100},
		},
		{
			sql:       "select * from information_schema.tikv_region_peers where region_id=100 and region_id in (100,101) and store_id=200 and store_id in (200,201)",
			regionIDs: []uint64{100},
			storeIDs:  []uint64{200},
		},
		{
			sql:         "select * from information_schema.tikv_region_peers where region_id=100 and region_id in (101,102)",
			skipRequest: true,
		},
		{
			sql:         "select * from information_schema.tikv_region_peers where region_id=100 and region_id in (101,102) and store_id=200 and store_id in (200,201)",
			skipRequest: true,
		},
		{
			sql:         "select * from information_schema.tikv_region_peers where region_id=100 and region_id in (100,101) and store_id=200 and store_id in (201,202)",
			skipRequest: true,
		},
		{
			sql: `select * from information_schema.tikv_region_peers
								where region_id=100 and region_id in (100,101)
								and store_id=200 and store_id in (201,202)`,
			skipRequest: true,
		},
		{
			sql:       "select * from information_schema.tikv_region_peers where region_id in (100,101) and region_id in (101,102)",
			regionIDs: []uint64{101},
		},
		{
			sql: `select * from information_schema.tikv_region_peers
							where region_id in (100,101)
							and region_id in (101,102)
							and store_id in (200,201)
							and store_id in (201,202)`,
			regionIDs: []uint64{101},
			storeIDs:  []uint64{201},
		},
		{
			sql: `select * from information_schema.tikv_region_peers
							where region_id in (100,101)
							and region_id in (100,102)
							and region_id in (102,103)
							and region_id in (103,104)`,
			skipRequest: true,
		},
		// Test columns that is not extracted by TikvRegionPeersExtractor
		{
			sql: `select * from information_schema.tikv_region_peers
							where peer_id=100
							and is_learner=0
							and is_leader=1
							and status='NORMAL'
							and down_seconds=1000`,
		},
	}
	parser := parser.New()
	for _, ca := range cases {
		logicalMemTable, ok := getLogicalMemTable(t, dom, se, parser, ca.sql)
		if ok {
			require.NotNil(t, logicalMemTable.Extractor)

			tikvRegionPeersExtractor := logicalMemTable.Extractor.(*plannercore.TikvRegionPeersExtractor)
			if len(ca.regionIDs) > 0 {
				require.EqualValues(t, ca.regionIDs, tikvRegionPeersExtractor.RegionIDs, "SQL: %v", ca.sql)
			}
			if len(ca.storeIDs) > 0 {
				require.EqualValues(t, ca.storeIDs, tikvRegionPeersExtractor.StoreIDs, "SQL: %v", ca.sql)
			}
		} else {
			require.True(t, ca.skipRequest)
		}
	}
}

func TestColumns(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)

	var cases = []struct {
		sql                string
		columnName         set.StringSet
		tableSchema        set.StringSet
		tableName          set.StringSet
		columnNamePattern  []string
		tableSchemaPattern []string
		tableNamePattern   []string
		skipRequest        bool
	}{
		{
			sql:        `select * from INFORMATION_SCHEMA.COLUMNS where lower(column_name)=lower('T');`,
			columnName: set.NewStringSet("t"),
		},
		{
			sql:        `select * from INFORMATION_SCHEMA.COLUMNS where column_name=lower('T');`,
			columnName: set.NewStringSet("t"),
		},
		{
			sql:        `select * from INFORMATION_SCHEMA.COLUMNS where column_name='T';`,
			columnName: set.NewStringSet("t"),
		},
		{
			sql:         `select * from INFORMATION_SCHEMA.COLUMNS where table_schema='TEST';`,
			tableSchema: set.NewStringSet("test"),
		},
		{
			sql:       `select * from INFORMATION_SCHEMA.COLUMNS where table_name='TEST';`,
			tableName: set.NewStringSet("test"),
		},
		{
			sql:        "select * from information_schema.COLUMNS where table_name in ('TEST','t') and column_name in ('A','b')",
			columnName: set.NewStringSet("a", "b"),
			tableName:  set.NewStringSet("test", "t"),
		},
		{
			sql:       `select * from information_schema.COLUMNS where table_name='a' and table_name in ('a', 'B');`,
			tableName: set.NewStringSet("a"),
		},
		{
			sql:         `select * from information_schema.COLUMNS where table_name='a' and table_name='B';`,
			skipRequest: true,
		},
		{
			sql:              `select * from information_schema.COLUMNS where table_name like 'T%';`,
			tableNamePattern: []string{"t%"},
		},
		{
			sql:               `select * from information_schema.COLUMNS where column_name like 'T%';`,
			columnNamePattern: []string{"t%"},
		},
		{
			sql:               `select * from information_schema.COLUMNS where column_name like 'i%';`,
			columnNamePattern: []string{"i%"},
		},
		{
			sql:               `select * from information_schema.COLUMNS where column_name like 'abc%' or column_name like "def%";`,
			columnNamePattern: []string{},
		},
		{
			sql:               `select * from information_schema.COLUMNS where column_name like 'abc%' and column_name like "%def";`,
			columnNamePattern: []string{"abc%", "%def"},
		},
	}
	parser := parser.New()
	for _, ca := range cases {
		logicalMemTable, ok := getLogicalMemTable(t, dom, se, parser, ca.sql)
		if ok {
			require.NotNil(t, logicalMemTable.Extractor)

			columnsTableExtractor := logicalMemTable.Extractor.(*plannercore.InfoSchemaColumnsExtractor)
			require.Equal(t, ca.skipRequest, columnsTableExtractor.SkipRequest, "SQL: %v", ca.sql)

			require.Equal(t, ca.columnName.Count(), columnsTableExtractor.ColPredicates["column_name"].Count())
			if ca.columnName.Count() > 0 && columnsTableExtractor.ColPredicates["column_name"].Count() > 0 {
				require.EqualValues(t, ca.columnName, columnsTableExtractor.ColPredicates["column_name"], "SQL: %v", ca.sql)
			}

			require.Equal(t, ca.tableSchema.Count(), columnsTableExtractor.ColPredicates["table_schema"].Count())
			if ca.tableSchema.Count() > 0 && columnsTableExtractor.ColPredicates["table_schema"].Count() > 0 {
				require.EqualValues(t, ca.tableSchema, columnsTableExtractor.ColPredicates["table_schema"], "SQL: %v", ca.sql)
			}
			require.Equal(t, ca.tableName.Count(), columnsTableExtractor.ColPredicates["table_name"].Count())
			if ca.tableName.Count() > 0 && columnsTableExtractor.ColPredicates["table_name"].Count() > 0 {
				require.EqualValues(t, ca.tableName, columnsTableExtractor.ColPredicates["table_name"], "SQL: %v", ca.sql)
			}
			require.Equal(t, len(ca.tableNamePattern), len(columnsTableExtractor.LikePatterns["table_name"]))
			if len(ca.tableNamePattern) > 0 && len(columnsTableExtractor.LikePatterns["table_name"]) > 0 {
				require.EqualValues(t, ca.tableNamePattern, columnsTableExtractor.LikePatterns["table_name"], "SQL: %v", ca.sql)
			}
			require.Equal(t, len(ca.columnNamePattern), len(columnsTableExtractor.LikePatterns["column_name"]))
			if len(ca.columnNamePattern) > 0 && len(columnsTableExtractor.LikePatterns["column_name"]) > 0 {
				require.EqualValues(t, ca.columnNamePattern, columnsTableExtractor.LikePatterns["column_name"], "SQL: %v", ca.sql)
			}
			require.Equal(t, len(ca.tableSchemaPattern), len(columnsTableExtractor.LikePatterns["table_schema"]))
			if len(ca.tableSchemaPattern) > 0 && len(columnsTableExtractor.LikePatterns["table_schema"]) > 0 {
				require.EqualValues(t, ca.tableSchemaPattern, columnsTableExtractor.LikePatterns["table_schema"], "SQL: %v", ca.sql)
			}
		} else {
			require.True(t, ca.skipRequest, "SQL: %v", ca.sql)
		}
	}
}

func TestTikvRegionStatusExtractor(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE p (id int(11), unique index(id))
PARTITION BY RANGE COLUMNS ( id ) (
		PARTITION p0 VALUES LESS THAN (6),
		PARTITION p1 VALUES LESS THAN (11),
		PARTITION p3 VALUES LESS THAN (21)
)`)
	res := tk.MustQuery("select * from information_schema.tables where table_name = 'p'")
	idStr := res.Rows()[0][21]
	id, err := strconv.Atoi(idStr.(string))
	require.NoError(t, err)
	sSQL := fmt.Sprintf("select * from information_schema.TIKV_REGION_STATUS where table_id = %v", id)

	var cases = []struct {
		sql      string
		tableIDs []int64
	}{
		{
			sql:      "select * from information_schema.TIKV_REGION_STATUS where table_id = 1",
			tableIDs: []int64{1},
		},
		{
			sql:      "select * from information_schema.TIKV_REGION_STATUS where table_id = 1 or table_id = 2",
			tableIDs: []int64{1, 2},
		},
		{
			sql:      "select * from information_schema.TIKV_REGION_STATUS where table_id in (1,2,3)",
			tableIDs: []int64{1, 2, 3},
		},
		{
			sql:      sSQL,
			tableIDs: []int64{int64(id)},
		},
	}
	parser := parser.New()
	for _, ca := range cases {
		logicalMemTable, _ := getLogicalMemTable(t, dom, se, parser, ca.sql)
		require.NotNil(t, logicalMemTable.Extractor)
		rse := logicalMemTable.Extractor.(*plannercore.TiKVRegionStatusExtractor)
		tableids := rse.GetTablesID()
		slices.Sort(tableids)
		require.Equal(t, ca.tableIDs, tableids)
	}
}

func TestExtractorInPreparedStmt(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	var cases = []struct {
		prepared string
		userVars []any
		params   []any
		checker  func(extractor base.MemTablePredicateExtractor)
	}{
		{
			prepared: "select * from information_schema.TIKV_REGION_STATUS where table_id = ?",
			userVars: []any{1},
			params:   []any{1},
			checker: func(extractor base.MemTablePredicateExtractor) {
				rse := extractor.(*plannercore.TiKVRegionStatusExtractor)
				tableids := rse.GetTablesID()
				slices.Sort(tableids)
				require.Equal(t, []int64{1}, tableids)
			},
		},
		{
			prepared: "select * from information_schema.TIKV_REGION_STATUS where table_id = ? or table_id = ?",
			userVars: []any{1, 2},
			params:   []any{1, 2},
			checker: func(extractor base.MemTablePredicateExtractor) {
				rse := extractor.(*plannercore.TiKVRegionStatusExtractor)
				tableids := rse.GetTablesID()
				slices.Sort(tableids)
				require.Equal(t, []int64{1, 2}, tableids)
			},
		},
		{
			prepared: "select * from information_schema.TIKV_REGION_STATUS where table_id in (?,?)",
			userVars: []any{1, 2},
			params:   []any{1, 2},
			checker: func(extractor base.MemTablePredicateExtractor) {
				rse := extractor.(*plannercore.TiKVRegionStatusExtractor)
				tableids := rse.GetTablesID()
				slices.Sort(tableids)
				require.Equal(t, []int64{1, 2}, tableids)
			},
		},
		{
			prepared: "select * from information_schema.COLUMNS where table_name like ?",
			userVars: []any{`"a%"`},
			params:   []any{"a%"},
			checker: func(extractor base.MemTablePredicateExtractor) {
				rse := extractor.(*plannercore.InfoSchemaColumnsExtractor)
				require.EqualValues(t, []string{"a%"}, rse.LikePatterns["table_name"])
			},
		},
		{
			prepared: "select * from information_schema.tidb_hot_regions_history where update_time>=?",
			userVars: []any{"cast('2019-10-10 10:10:10' as datetime)"},
			params: []any{func() types.Time {
				tt, err := types.ParseTimestamp(tk.Session().GetSessionVars().StmtCtx.TypeCtx(), "2019-10-10 10:10:10")
				require.NoError(t, err)
				return tt
			}()},
			checker: func(extractor base.MemTablePredicateExtractor) {
				rse := extractor.(*plannercore.HotRegionsHistoryTableExtractor)
				require.Equal(t, timestamp(t, "2019-10-10 10:10:10"), rse.StartTime)
			},
		},
	}

	// text protocol
	parser := parser.New()
	for _, ca := range cases {
		tk.MustExec(fmt.Sprintf("prepare stmt from '%s'", ca.prepared))
		setStmt := "set "
		exec := "execute stmt using "
		for i, uv := range ca.userVars {
			name := fmt.Sprintf("@a%d", i)
			setStmt += fmt.Sprintf("%s=%v", name, uv)
			exec += name
			if i != len(ca.userVars)-1 {
				setStmt += ","
				exec += ","
			}
		}
		tk.MustExec(setStmt)
		stmt, err := parser.ParseOneStmt(exec, "", "")
		require.NoError(t, err)
		nodeW := resolve.NewNodeW(stmt)
		plan, _, err := planner.OptimizeExecStmt(context.Background(), tk.Session(), nodeW, dom.InfoSchema())
		require.NoError(t, err)
		extractor := plan.(*plannercore.Execute).Plan.(*physicalop.PhysicalMemTable).Extractor
		ca.checker(extractor)
	}

	// binary protocol
	for _, ca := range cases {
		id, _, _, err := tk.Session().PrepareStmt(ca.prepared)
		require.NoError(t, err)
		prepStmt, err := tk.Session().GetSessionVars().GetPreparedStmtByID(id)
		require.NoError(t, err)
		params := expression.Args2Expressions4Test(ca.params...)
		execStmt := &ast.ExecuteStmt{
			BinaryArgs: params,
			PrepStmt:   prepStmt,
		}
		nodeW := resolve.NewNodeW(execStmt)
		plan, _, err := planner.OptimizeExecStmt(context.Background(), tk.Session(), nodeW, dom.InfoSchema())
		require.NoError(t, err)
		extractor := plan.(*plannercore.Execute).Plan.(*physicalop.PhysicalMemTable).Extractor
		ca.checker(extractor)
	}
}

func TestInfoSchemaTableExtract(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	var cases = []struct {
		sql           string
		skipRequest   bool
		colPredicates map[string]set.StringSet
		dual          bool
	}{
		{
			sql:         `select * from INFORMATION_SCHEMA.TABLES where table_schema='test';`,
			skipRequest: false,
			colPredicates: map[string]set.StringSet{
				"table_schema": set.NewStringSet("test"),
			},
		},
		{
			sql:         `select * from INFORMATION_SCHEMA.TABLES where table_name='t';`,
			skipRequest: false,
			colPredicates: map[string]set.StringSet{
				"table_name": set.NewStringSet("t"),
			},
		},
		{
			sql:         `select * from INFORMATION_SCHEMA.TABLES where tidb_table_id=111;`,
			skipRequest: false,
			colPredicates: map[string]set.StringSet{
				"tidb_table_id": set.NewStringSet("111"),
			},
		},
		{
			sql:         `select * from INFORMATION_SCHEMA.TABLES where lower(table_name)='T';`,
			skipRequest: false,
			colPredicates: map[string]set.StringSet{
				"table_name": set.NewStringSet("t"),
			},
		},
		{
			sql:         `select * from INFORMATION_SCHEMA.TABLES where table_name=lower('T');`,
			skipRequest: false,
			colPredicates: map[string]set.StringSet{
				"table_name": set.NewStringSet("t"),
			},
		},
		{
			sql:         `select * from INFORMATION_SCHEMA.TABLES where lower(table_name)=lower('T');`,
			skipRequest: false,
			colPredicates: map[string]set.StringSet{
				"table_name": set.NewStringSet("t"),
			},
		},
		{
			sql:         `select * from INFORMATION_SCHEMA.TABLES where upper(table_name)=upper('T');`,
			skipRequest: false,
			colPredicates: map[string]set.StringSet{
				"table_name": set.NewStringSet("t"),
			},
		},
		{
			sql:         `select * from INFORMATION_SCHEMA.TABLES where table_schema='TS';`,
			skipRequest: false,
			colPredicates: map[string]set.StringSet{
				"table_schema": set.NewStringSet("ts"),
			},
		},
		{
			sql:         "select * from information_schema.TABLES where table_name in ('TEST','t') and table_schema in ('A','b')",
			skipRequest: false,
			colPredicates: map[string]set.StringSet{
				"table_name":   set.NewStringSet("test", "t"),
				"table_schema": set.NewStringSet("a", "b"),
			},
		},
		{
			sql:         "select * from information_schema.TABLES where table_name ='t' or table_name ='A'",
			skipRequest: false,
			colPredicates: map[string]set.StringSet{
				"table_name": set.NewStringSet("t", "a"),
			},
		},
		{
			sql:         "select * from information_schema.REFERENTIAL_CONSTRAINTS where table_name ='t' or table_name ='A'",
			skipRequest: false,
			colPredicates: map[string]set.StringSet{
				"table_name": set.NewStringSet("t", "a"),
			},
		},
		{
			sql:         "select * from information_schema.REFERENTIAL_CONSTRAINTS where constraint_schema ='t'",
			skipRequest: false,
			colPredicates: map[string]set.StringSet{
				"constraint_schema": set.NewStringSet("t"),
			},
		},
		{
			sql:         "select * from information_schema.REFERENTIAL_CONSTRAINTS where constraint_schema ='t' and constraint_name = 'cc'",
			skipRequest: false,
			colPredicates: map[string]set.StringSet{
				"constraint_schema": set.NewStringSet("t"),
				"constraint_name":   set.NewStringSet("cc"),
			},
		},
		{
			sql:         "select * from information_schema.CHECK_CONSTRAINTS where constraint_schema ='t' and constraint_name = 'cc'",
			skipRequest: false,
			colPredicates: map[string]set.StringSet{
				"constraint_schema": set.NewStringSet("t"),
				"constraint_name":   set.NewStringSet("cc"),
			},
		},
		{
			sql:         "select * from information_schema.TIDB_CHECK_CONSTRAINTS where constraint_schema ='t' and constraint_name = 'cc'",
			skipRequest: false,
			colPredicates: map[string]set.StringSet{
				"constraint_schema": set.NewStringSet("t"),
				"constraint_name":   set.NewStringSet("cc"),
			},
		},
		{
			sql:         "select * from information_schema.TABLE_CONSTRAINTS where constraint_schema ='t' and table_name = 'cc'",
			skipRequest: false,
			colPredicates: map[string]set.StringSet{
				"constraint_schema": set.NewStringSet("t"),
				"table_name":        set.NewStringSet("cc"),
			},
		},
		{
			sql:         "select * from information_schema.KEY_COLUMN_USAGE where table_name ='t' or table_name ='A'",
			skipRequest: false,
			colPredicates: map[string]set.StringSet{
				"table_name": set.NewStringSet("t", "a"),
			},
		},
		{
			sql:         "select * from information_schema.STATISTICS where table_name ='t' or table_name ='A'",
			skipRequest: false,
			colPredicates: map[string]set.StringSet{
				"table_name": set.NewStringSet("t", "a"),
			},
		},
		{
			sql:           "select * from information_schema.STATISTICS where table_name ='t' and table_name ='A'",
			skipRequest:   true,
			colPredicates: map[string]set.StringSet{},
			dual:          true,
		},
		{
			sql:           "select * from information_schema.STATISTICS where table_name ='t' and table_schema ='A' and table_schema = 'b'",
			skipRequest:   true,
			colPredicates: map[string]set.StringSet{},
			dual:          true,
		},
		{
			sql:           "select * from information_schema.STATISTICS where table_schema ='A' or lower(table_schema) = 'a'",
			skipRequest:   false,
			colPredicates: map[string]set.StringSet{},
		},
		{
			sql:         "select * from information_schema.STATISTICS where (table_schema ='A' or lower(table_schema) = 'a') and table_name='t'",
			skipRequest: false,
			colPredicates: map[string]set.StringSet{
				"table_name": set.NewStringSet("t"),
			},
		},
		{
			sql:           "select * from information_schema.STATISTICS where table_schema ='A' or lower(table_schema) = 'b'",
			skipRequest:   false,
			colPredicates: map[string]set.StringSet{},
		},
		{
			sql:           "select * from information_schema.STATISTICS where table_schema ='A' or lower(table_schema) = 'b'",
			skipRequest:   false,
			colPredicates: map[string]set.StringSet{},
		},
		{
			sql:         "select * from information_schema.SEQUENCES where sequence_schema ='a' and sequence_name='b'",
			skipRequest: false,
			colPredicates: map[string]set.StringSet{
				"sequence_schema": set.NewStringSet("a"),
				"sequence_name":   set.NewStringSet("b"),
			},
		},
		{
			sql:         "select * from information_schema.TIDB_INDEX_USAGE where table_schema ='a' and table_name='b' and index_name='c'",
			skipRequest: false,
			colPredicates: map[string]set.StringSet{
				"table_schema": set.NewStringSet("a"),
				"table_name":   set.NewStringSet("b"),
				"index_name":   set.NewStringSet("c"),
			},
		},
		{
			sql:         "select * from information_schema.PARTITIONS where table_schema ='a' and table_name='b' and partition_name='c'",
			skipRequest: false,
			colPredicates: map[string]set.StringSet{
				"table_schema":   set.NewStringSet("a"),
				"table_name":     set.NewStringSet("b"),
				"partition_name": set.NewStringSet("c"),
			},
		},
		{
			sql:         "select * from information_schema.TIDB_INDEXES where table_schema ='a' and table_name='b'",
			skipRequest: false,
			colPredicates: map[string]set.StringSet{
				"table_schema": set.NewStringSet("a"),
				"table_name":   set.NewStringSet("b"),
			},
		},
		{
			sql:         "select * from information_schema.VIEWS where table_schema ='a' and table_name='b'",
			skipRequest: false,
			colPredicates: map[string]set.StringSet{
				"table_schema": set.NewStringSet("a"),
				"table_name":   set.NewStringSet("b"),
			},
		},
		{
			sql:         "select * from information_schema.TABLE_CONSTRAINTS where table_schema ='a' and table_name='b'",
			skipRequest: false,
			colPredicates: map[string]set.StringSet{
				"table_schema": set.NewStringSet("a"),
				"table_name":   set.NewStringSet("b"),
			},
		},
		{
			sql:         "select * from information_schema.SCHEMATA where schema_name ='a'",
			skipRequest: false,
			colPredicates: map[string]set.StringSet{
				"schema_name": set.NewStringSet("a"),
			},
		},
	}
	parser := parser.New()
	for _, ca := range cases {
		logicalMemTable, ok := getLogicalMemTable(t, dom, se, parser, ca.sql)
		if ca.dual {
			require.False(t, ok, ca.sql)
		} else {
			require.True(t, ok, ca.sql)
			require.NotNil(t, logicalMemTable.Extractor)
			ex, ok := logicalMemTable.Extractor.(interface {
				GetBase() *plannercore.InfoSchemaBaseExtractor
			})
			require.True(t, ok)
			require.Equal(t, ca.skipRequest, ex.GetBase().SkipRequest, "SQL: %v", ca.sql)
			require.Equal(t, ca.colPredicates, ex.GetBase().ColPredicates, "SQL: %v", ca.sql)
		}
	}
}
