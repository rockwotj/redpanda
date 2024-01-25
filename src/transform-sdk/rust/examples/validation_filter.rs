// Copyright 2024 Redpanda Data, Inc.
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

use anyhow::Result;
use redpanda_transform_sdk::*;

// This example shows a filter that outputs only valid JSON to the output topic.
fn main() {
    on_record_written(filter_valid_json);
}

fn filter_valid_json(event: WriteEvent, writer: &mut RecordWriter) -> Result<()> {
    let value = event.record.value().unwrap_or_default();
    if serde_json::from_slice::<serde_json::Value>(value).is_ok() {
        writer.write(event.record)?;
    }
    Ok(())
}
