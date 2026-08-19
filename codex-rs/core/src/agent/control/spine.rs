use std::time::Duration;

use codex_protocol::ThreadId;
use codex_protocol::error::Result as CodexResult;
use codex_protocol::user_input::UserInput;

use super::AgentControl;

impl AgentControl {
    pub(crate) async fn wait_for_spine_spawn_turn_idle(
        &self,
        thread_id: ThreadId,
    ) -> CodexResult<()> {
        let state = self.upgrade()?;
        let thread = state.get_thread(thread_id).await?;
        loop {
            if thread.codex.session.active_turn.lock().await.is_none() {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    /// Send a continuation after the caller has atomically reserved and committed a Spine slot.
    pub(crate) async fn send_spine_spawn_continuation(
        &self,
        thread_id: ThreadId,
        input: Vec<UserInput>,
    ) -> CodexResult<String> {
        let state = match self.upgrade() {
            Ok(state) => state,
            Err(error) => {
                self.release_execution_reservation(thread_id);
                return Err(error);
            }
        };
        let result = self
            .send_input_after_capacity_check(thread_id, &state, input)
            .await;
        if result.is_err() {
            self.release_execution_reservation(thread_id);
        }
        result
    }
}
