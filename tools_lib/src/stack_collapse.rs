use std::collections::{HashMap, HashSet};
use std::time::{Duration, SystemTime};
use std::convert::TryInto;

use measureme::{Event, ProfilingData};


fn find_all_thread_ids_and_start_times(profiling_data: &ProfilingData) -> HashMap<u64, SystemTime> {

    let mut thread_ids = HashMap::new();

    for event in profiling_data.iter().filter(|e| !e.timestamp.is_instant()) {
        thread_ids
            .entry(event.thread_id)
            .and_modify(|start| {
                *start = std::cmp::min(event.timestamp.start(), *start);
            })
            .or_insert(event.timestamp.start());
    }

    thread_ids
}

fn process_thread(
    thread_id: u64,
    thread_start_time: SystemTime,
    profiling_data: &ProfilingData,
    sampling_interval: Duration,
    counters: &mut HashMap<String, u64>,
) {

    let mut events = profiling_data
        .iter()
        .rev()
        .filter(|e| !e.timestamp.is_instant())
        .filter(|e| e.thread_id == thread_id)
        .peekable();

    let mut sample_cursor = if let Some(last_event) = events.peek() {
        // Start sample (backwards) at the last position that we would hit if
        // we sampled forward starting from `thread_start_time`.

        let max_sampled_timestamp = last_event.timestamp.end() - Duration::from_nanos(1);

        if max_sampled_timestamp < thread_start_time {
            // Just ignore this zero-length thread
            return
        }

        let sampling_interval_nanos = sampling_interval.as_nanos();

        let thread_duration_nanos = max_sampled_timestamp.duration_since(thread_start_time).unwrap().as_nanos();
        let samples = thread_duration_nanos / sampling_interval_nanos;
        thread_start_time + Duration::from_nanos((samples * sampling_interval_nanos).try_into().unwrap())
    } else {
        // No events? Ignore the thread.
        return
    };

    let mut stack: Vec<Event<'_>> = vec![];
    // The id is updated in sync we `stack`
    let mut stack_id = "rustc".to_owned();

    loop {

        // Pop things from the stack that we've moved past
        while let Some(top) = stack.last().cloned() {
            if top.timestamp.contains(sample_cursor) {
                break
            } else {
                let popped = stack.pop().unwrap();
                let new_stack_id_len = stack_id.len() - (popped.label.len() + 1);
                stack_id.truncate(new_stack_id_len);
            }
        }

        // Push things onto the stack that are under the cursor now
        while let Some(event) = events.peek().cloned() {
            if sample_cursor >= event.timestamp.end() {
                // The next event is not under the cursor, so break
                break
            }

            // Check if the event is under the cursor. It could also be an
            // event that the cursor has skipped entirely
            if event.timestamp.contains(sample_cursor) {
                stack_id.push(';');
                stack_id.push_str(&event.label[..]);
                stack.push(event);
            }

            events.next();
        }

        if events.peek().is_none() && stack.is_empty() {
            break
        }

        eprintln!("{}", stack_id);
        *counters.entry(stack_id.clone()).or_insert(0) += 1;

        sample_cursor -= sampling_interval;
    }
}

pub fn collapse_stacks<'a>(
    profiling_data: &ProfilingData,
    sampling_interval: Duration,
) -> HashMap<String, u64> {
    let thread_ids = find_all_thread_ids_and_start_times(profiling_data);
    let mut counters = HashMap::new();

    for (thread_id, thread_start_time) in thread_ids {
        process_thread(thread_id, thread_start_time, profiling_data, sampling_interval, &mut counters);
    }

    counters
}

#[cfg(test)]
mod test {
    use measureme::{Event, ProfilingDataBuilder};
    use std::collections::HashMap;
    use std::time::{Duration, SystemTime};

    #[test]
    fn basic_test() {


        let mut b = ProfilingDataBuilder::new();

        //                       <--------->
        //   <------------><-------------------->
        //   100           300   400       600   700

        b.interval("Query", "EventA", 0, 100, 300, |_| {});
        b.interval("Query", "EventB", 0, 300, 700, |b| {
            b.interval("Query", "EventA", 0, 400, 600, |_| {});
        });

        let profiling_data = b.into_profiling_data();

        let recorded_stacks = super::collapse_stacks(&profiling_data, Duration::from_nanos(1));

        let mut expected_stacks = HashMap::<String, u64>::new();
        expected_stacks.insert("rustc;EventB;EventA".into(), 200);
        expected_stacks.insert("rustc;EventB".into(), 200);
        expected_stacks.insert("rustc;EventA".into(), 200);

        assert_eq!(expected_stacks, recorded_stacks);
    }

    // #[test]
    // fn multi_threaded_test() {
    //     let events = [
    //         Event {
    //             event_kind: "Query".into(),
    //             label: "EventA".into(),
    //             additional_data: &[],
    //             timestamp: SystemTime::UNIX_EPOCH + Duration::from_secs(1),
    //             timestamp_kind: TimestampKind::Start,
    //             thread_id: 1,
    //         },
    //         Event {
    //             event_kind: "Query".into(),
    //             label: "EventB".into(),
    //             additional_data: &[],
    //             timestamp: SystemTime::UNIX_EPOCH + Duration::from_secs(3),
    //             timestamp_kind: TimestampKind::Start,
    //             thread_id: 2,
    //         },
    //         Event {
    //             event_kind: "Query".into(),
    //             label: "EventA".into(),
    //             additional_data: &[],
    //             timestamp: SystemTime::UNIX_EPOCH + Duration::from_secs(2),
    //             timestamp_kind: TimestampKind::End,
    //             thread_id: 1,
    //         },
    //         Event {
    //             event_kind: "Query".into(),
    //             label: "EventA".into(),
    //             additional_data: &[],
    //             timestamp: SystemTime::UNIX_EPOCH + Duration::from_secs(4),
    //             timestamp_kind: TimestampKind::Start,
    //             thread_id: 2,
    //         },
    //         Event {
    //             event_kind: "Query".into(),
    //             label: "EventA".into(),
    //             additional_data: &[],
    //             timestamp: SystemTime::UNIX_EPOCH + Duration::from_secs(5),
    //             timestamp_kind: TimestampKind::End,
    //             thread_id: 2,
    //         },
    //         Event {
    //             event_kind: "Query".into(),
    //             label: "EventB".into(),
    //             additional_data: &[],
    //             timestamp: SystemTime::UNIX_EPOCH + Duration::from_secs(6),
    //             timestamp_kind: TimestampKind::End,
    //             thread_id: 2,
    //         },
    //     ];

    //     let recorded_stacks = super::collapse_stacks(events.iter().cloned(), 1000);

    //     let mut expected_stacks = HashMap::<String, usize>::new();
    //     expected_stacks.insert("rustc;EventB;EventA".into(), 1);
    //     expected_stacks.insert("rustc;EventB".into(), 2);
    //     expected_stacks.insert("rustc;EventA".into(), 1);

    //     assert_eq!(expected_stacks, recorded_stacks);
    // }
}
