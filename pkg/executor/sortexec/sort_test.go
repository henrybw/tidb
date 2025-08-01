// Copyright 2020 PingCAP, Inc.
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

package sortexec_test

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/session/sessmgr"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// TODO remove spill as it should be tested in sort_spill_test.go
// TODO add some new fail points, as some fail point may be aborted after the refine
func TestSortInDisk(t *testing.T) {
	testSortInDisk(t, false)
	testSortInDisk(t, true)
}

// TODO remove failpoint
func testSortInDisk(t *testing.T, removeDir bool) {
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TempStoragePath = t.TempDir()
		conf.Performance.EnableStatsCacheMemQuota = true
	})
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/sortexec/testSortedRowContainerSpill", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/sortexec/testSortedRowContainerSpill"))
	}()
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	defer tk.MustExec("SET GLOBAL tidb_mem_oom_action = DEFAULT")
	tk.MustExec("SET GLOBAL tidb_mem_oom_action='LOG'")
	tk.MustExec("use test")

	sm := &testkit.MockSessionManager{
		PS: make([]*sessmgr.ProcessInfo, 0),
	}
	tk.Session().SetSessionManager(sm)
	dom.ExpensiveQueryHandle().SetSessionManager(sm)

	if removeDir {
		require.Nil(t, os.RemoveAll(config.GetGlobalConfig().TempStoragePath))
		defer func() {
			_, err := os.Stat(config.GetGlobalConfig().TempStoragePath)
			if err != nil {
				require.True(t, os.IsExist(err))
			}
		}()
	}

	tk.MustExec("set @@tidb_mem_quota_query=1;")
	tk.MustExec("set @@tidb_max_chunk_size=32;")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(c1 int, c2 int, c3 int)")
	var buf bytes.Buffer
	buf.WriteString("insert into t values ")
	for i := range 5 {
		for j := i; j < 1024; j += 5 {
			if j > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(fmt.Sprintf("(%v, %v, %v)", j, j, j))
		}
	}
	tk.MustExec(buf.String())
	result := tk.MustQuery("select * from t order by c1")
	for i := range 1024 {
		require.Equal(t, fmt.Sprint(i), result.Rows()[i][0].(string))
		require.Equal(t, fmt.Sprint(i), result.Rows()[i][1].(string))
		require.Equal(t, fmt.Sprint(i), result.Rows()[i][2].(string))
	}
	require.Equal(t, int64(0), tk.Session().GetSessionVars().StmtCtx.MemTracker.BytesConsumed())
	require.Greater(t, tk.Session().GetSessionVars().StmtCtx.MemTracker.MaxConsumed(), int64(0))
	require.Equal(t, int64(0), tk.Session().GetSessionVars().StmtCtx.DiskTracker.BytesConsumed())
	require.Greater(t, tk.Session().GetSessionVars().StmtCtx.DiskTracker.MaxConsumed(), int64(0))
}

func TestIssue16696(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TempStoragePath = t.TempDir()
		conf.Performance.EnableStatsCacheMemQuota = true
	})
	alarmRatio := vardef.MemoryUsageAlarmRatio.Load()
	vardef.MemoryUsageAlarmRatio.Store(0.0)
	defer vardef.MemoryUsageAlarmRatio.Store(alarmRatio)

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/sortexec/testSortedRowContainerSpill", "return(true)"))
	defer require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/sortexec/testSortedRowContainerSpill"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/join/testRowContainerSpill", "return(true)"))
	defer require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/join/testRowContainerSpill"))
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	defer tk.MustExec("SET GLOBAL tidb_mem_oom_action = DEFAULT")
	tk.MustExec("SET GLOBAL tidb_mem_oom_action='LOG'")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE `t` (`a` int(11) DEFAULT NULL,`b` int(11) DEFAULT NULL)")
	tk.MustExec("insert into t values (1, 1)")
	for range 6 {
		tk.MustExec("insert into t select * from t")
	}
	tk.MustExec("set tidb_mem_quota_query = 1;")
	rows := tk.MustQuery("explain analyze  select t1.a, t1.a +1 from t t1 join t t2 join t t3 order by t1.a").Rows()
	for _, row := range rows {
		length := len(row)
		line := fmt.Sprintf("%v", row)
		disk := fmt.Sprintf("%v", row[length-1])
		if strings.Contains(line, "Sort") || strings.Contains(line, "HashJoin") {
			require.NotContains(t, disk, "0 Bytes")
			require.True(t, strings.Contains(disk, "MB") ||
				strings.Contains(disk, "KB") ||
				strings.Contains(disk, "Bytes"))
		}
	}
}
