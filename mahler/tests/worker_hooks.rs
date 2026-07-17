use mahler::extract::{Target, View};
use mahler::job::update;
use mahler::state::State;
use mahler::task::{enforce, with_io, IO};
use mahler::worker::{SeekStatus, Worker};

// The motivating case for worker hooks: an internal flag toggled by regular
// tasks triggers a "positional" task (e.g. a reboot) at the end of the apply.
// Internal fields are stripped from the current/target distance, so a regular
// planner job can never be selected by the flag.
#[derive(State, Debug, Clone, PartialEq)]
struct SystemState {
    value: i32,
    #[mahler(internal)]
    needs_reboot: bool,
}

fn increment(mut state: View<SystemState>, Target(tgt): Target<SystemState>) -> IO<SystemState> {
    if state.value < tgt.value {
        state.value += 1;
        // any change to the value requires a reboot at the end of the apply
        state.needs_reboot = true;
    }

    with_io(state, |state| async { Ok(state) })
}

fn reboot(mut state: View<SystemState>) -> IO<SystemState> {
    enforce!(state.needs_reboot);
    // clear the trigger so the hook only runs on this apply
    state.needs_reboot = false;

    with_io(state, |state| async {
        // this is where the actual reboot would be triggered
        Ok(state)
    })
}

#[tokio::test]
async fn hook_triggered_by_internal_flag_runs_at_the_end_of_the_apply() {
    let worker = Worker::new()
        .job("", update(increment))
        .hook(reboot)
        .initial_state(SystemState {
            value: 0,
            needs_reboot: false,
        })
        .unwrap();

    let (state, status) = worker
        .seek_target(SystemStateTarget { value: 2 })
        .await
        .unwrap();

    assert_eq!(status, SeekStatus::Success);
    // the increments set the internal flag and the hook cleared it after running
    assert_eq!(
        state,
        SystemState {
            value: 2,
            needs_reboot: false,
        }
    );
}

#[tokio::test]
async fn pending_hook_from_a_previous_run_fires_on_a_noop_apply() {
    // simulate a previous apply that set the flag but was interrupted
    // before the hook could run
    let worker = Worker::new()
        .job("", update(increment))
        .hook(reboot)
        .initial_state(SystemState {
            value: 2,
            needs_reboot: true,
        })
        .unwrap();

    // the target is already met, so the main workflow is empty,
    // but the pending hook still runs
    let (state, status) = worker
        .seek_target(SystemStateTarget { value: 2 })
        .await
        .unwrap();

    assert_eq!(status, SeekStatus::Success);
    assert_eq!(
        state,
        SystemState {
            value: 2,
            needs_reboot: false,
        }
    );
}
