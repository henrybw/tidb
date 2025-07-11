// Copyright 2018 PingCAP, Inc.
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

package domain

import (
	"time"

	"github.com/pingcap/tidb/pkg/infoschema/validatorapi"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	atomicutil "go.uber.org/atomic"
)

// SchemaChecker is used for checking schema-validity.
type SchemaChecker struct {
	validatorapi.Validator
	schemaVer       int64
	relatedTableIDs []int64
	needCheckSchema bool
}

type intSchemaVer int64

func (i intSchemaVer) SchemaMetaVersion() int64 {
	return int64(i)
}

var (
	// SchemaOutOfDateRetryInterval is the backoff time before retrying.
	SchemaOutOfDateRetryInterval = atomicutil.NewDuration(500 * time.Millisecond)
	// SchemaOutOfDateRetryTimes is the max retry count when the schema is out of date.
	SchemaOutOfDateRetryTimes = atomicutil.NewInt32(10)
)

// NewSchemaChecker creates a new schema checker.
func NewSchemaChecker(validator validatorapi.Validator, schemaVer int64, relatedTableIDs []int64, needCheckSchema bool) *SchemaChecker {
	return &SchemaChecker{
		Validator:       validator,
		schemaVer:       schemaVer,
		relatedTableIDs: relatedTableIDs,
		needCheckSchema: needCheckSchema,
	}
}

// Check checks the validity of the schema version.
func (s *SchemaChecker) Check(txnTS uint64) (*transaction.RelatedSchemaChange, error) {
	return s.CheckBySchemaVer(txnTS, intSchemaVer(s.schemaVer))
}

// CheckBySchemaVer checks if the schema version valid or not at txnTS.
func (s *SchemaChecker) CheckBySchemaVer(txnTS uint64, startSchemaVer tikv.SchemaVer) (*transaction.RelatedSchemaChange, error) {
	schemaOutOfDateRetryInterval := SchemaOutOfDateRetryInterval.Load()
	schemaOutOfDateRetryTimes := int(SchemaOutOfDateRetryTimes.Load())
	for range schemaOutOfDateRetryTimes {
		relatedChange, checkResult := s.Validator.Check(txnTS, startSchemaVer.SchemaMetaVersion(), s.relatedTableIDs, s.needCheckSchema)
		switch checkResult {
		case validatorapi.ResultSucc:
			return nil, nil
		case validatorapi.ResultFail:
			metrics.SchemaLeaseErrorCounter.WithLabelValues("changed").Inc()
			return relatedChange, ErrInfoSchemaChanged
		case validatorapi.ResultUnknown:
			time.Sleep(schemaOutOfDateRetryInterval)
		}
	}
	metrics.SchemaLeaseErrorCounter.WithLabelValues("outdated").Inc()
	return nil, ErrInfoSchemaExpired
}
