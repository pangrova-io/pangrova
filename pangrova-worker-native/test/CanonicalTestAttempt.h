// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "engine/executor/driver/TaskExecutor.h"
#include "pangrova/protocol/v2/runtime.pb.h"

namespace pangrova::test {

// Explicit canonical context for unit tests constructing an executor without parsing a TaskRequest.
inline void bindTestAttempt(executor::TaskExecutorConfig& config) {
  protocol::v2::ExecutionCallbackContext context;
  auto* attempt = context.mutable_attempt();
  attempt->set_query_or_job_id(config.query_id);
  attempt->set_stage_id(config.stage_id);
  attempt->set_partition_id(config.partition_id);
  attempt->set_plan_epoch(config.plan_epoch);
  attempt->set_attempt_id(config.attempt_id);
  attempt->set_fencing_token(config.fencing_token);
  attempt->set_task_id(config.task_id);
  attempt->set_control_epoch(config.control_epoch);
  auto* owner = attempt->mutable_execution_owner();
  owner->set_owner_id(config.owner_id);
  owner->set_owner_incarnation(config.owner_incarnation);
  owner->set_fencing_token(config.owner_fencing_token);
  config.callback_context = context.SerializeAsString();
}

}  // namespace pangrova::test
