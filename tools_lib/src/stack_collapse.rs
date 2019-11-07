use std::cmp;
use std::collections::HashMap;
use std::time::Duration;
use std::time::SystemTime;

use measureme::{Event, ProfilingData};

pub fn collapse_stacks<'a>(
    profiling_data: &ProfilingData,
    sampling_interval: Duration,
) -> HashMap<String, u64> {
    let mut counters = HashMap::new();
    let mut threads = HashMap::<_, (Vec<Event>, String, SystemTime, SystemTime, u64)>::new();

    for current_event in profiling_data
        .iter()
        .rev()
        .filter(|e| !e.timestamp.is_instant())
    {
        let (thread_stack, stack_id, thread_start, _thread_end, total_event_time) =
            threads.entry(current_event.thread_id).or_insert((
                Vec::new(),
                "rustc".to_owned(),
                current_event.timestamp.start(),
                current_event.timestamp.end(),
                0,
            ));

        *thread_start = cmp::min(*thread_start, current_event.timestamp.start());

        // Pop all events from the stack that are not parents of the
        // current event.
        while let Some(current_top) = thread_stack.last().cloned() {
            if current_top.contains(&current_event) {
                break;
            }

            let popped = thread_stack.pop().unwrap();
            let new_stack_id_len = stack_id.len() - (popped.label.len() + 1);
            stack_id.truncate(new_stack_id_len);
        }

        if !thread_stack.is_empty() {
            // If there is something on the stack, subtract the current
            // interval from it.
            counters.entry(stack_id.clone()).and_modify(|self_time| {
                eprintln!("sub {}", *self_time);
                *self_time -= current_event.duration().unwrap().as_nanos() as u64;
            });
        }else{
            // Update the total_event_time counter as the current event is on top level
            *total_event_time += current_event.duration().unwrap().as_nanos() as u64;
        }

        // add this event to the stack_id
        stack_id.push(';');
        stack_id.push_str(&current_event.label[..]);

        // update current events self time
        let self_time = counters.entry(stack_id.clone()).or_default();
        *self_time += current_event.duration().unwrap().as_nanos() as u64;

        // Bring the stack up-to-date
        thread_stack.push(current_event)
    }

    let mut rustc_time = 0;
    for (_,(_, _, thread_start, thread_end, total_event_time)) in threads.iter() {
        rustc_time +=
            thread_end.duration_since(*thread_start).unwrap().as_nanos() as u64 - *total_event_time;
    }
    counters.insert("rustc".to_owned(), rustc_time);

    let devisor = sampling_interval.as_nanos() as u64;
    for (_, self_time) in counters.iter_mut() {
        *self_time /= devisor;
    }

    counters
}

#[cfg(test)]
mod test {
    use measureme::ProfilingDataBuilder;
    use std::collections::HashMap;
    use std::time::Duration;

    #[test]
    fn basic_test() {
        let mut b = ProfilingDataBuilder::new();

        //                                         <--e1-->
        //                 <--e1-->        <----------e2---------->
        //              T2 1       2       3       4       5       6
        // sample interval |   |   |   |   |   |   |   |   |   |   |
        // stacks count:
        // rustc                       1   2
        // rustc;e1            1   2
        // rustc;e2                            1   2           3   4
        // rustc;e2;e1                                 1   2

        b.interval("Query", "e1", 0, 100, 200, |_| {});
        b.interval("Query", "e2", 0, 300, 600, |b| {
            b.interval("Query", "e1", 0, 400, 500, |_| {});
        });

        let profiling_data = b.into_profiling_data();

        let recorded_stacks = super::collapse_stacks(&profiling_data, Duration::from_nanos(50));

        let mut expected_stacks = HashMap::<String, u64>::new();
        expected_stacks.insert("rustc;e2;e1".into(), 2);
        expected_stacks.insert("rustc;e2".into(), 4);
        expected_stacks.insert("rustc;e1".into(), 2);
        expected_stacks.insert("rustc".into(), 2);

        assert_eq!(expected_stacks, recorded_stacks);
    }

    #[test]
    fn multi_threaded_test() {
        let mut b = ProfilingDataBuilder::new();

        //                 <--e1-->        <--e1-->
        //              T1 1       2       3       4       5
        //                                 <--e3-->
        //                 <--e1--><----------e2---------->
        //              T2 1       2       3       4       5
        // sample interval |       |       |       |       |
        // stacks count:
        // rustc                           1
        // rustc;e1                2               3
        // rustc;e2                        1               2
        // rustc;e2;e3                             1

        b.interval("Query", "e1", 1, 1, 2, |_| {});
        b.interval("Query", "e1", 1, 3, 4, |_| {});
        b.interval("Query", "e1", 2, 1, 2, |_| {});
        b.interval("Query", "e2", 2, 2, 5, |b| {
            b.interval("Query", "e3", 2, 3, 4, |_| {});
        });

        let profiling_data = b.into_profiling_data();

        let recorded_stacks = super::collapse_stacks(&profiling_data, Duration::from_nanos(1));

        let mut expected_stacks = HashMap::<String, u64>::new();
        expected_stacks.insert("rustc;e2;e3".into(), 1);
        expected_stacks.insert("rustc;e2".into(), 2);
        expected_stacks.insert("rustc;e1".into(), 3);
        expected_stacks.insert("rustc".into(), 1);

        assert_eq!(expected_stacks, recorded_stacks);
    }
}
