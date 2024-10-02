use std::cell::Cell;

use differential_dataflow::input::Input;
use differential_dataflow::operators::arrange::ArrangeByKey;
use serde::{Deserialize, Serialize};

thread_local! {
    pub static CLONE_COUNT: Cell<usize> = Cell::new(0);
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
struct CloneTracker(u32);

impl Clone for CloneTracker {
    fn clone(&self) -> Self {
        CLONE_COUNT.set(CLONE_COUNT.get() + 1);
        CloneTracker(self.0)
    }
}

fn main() {
    let mut args = std::env::args();
    args.next();

    let step_size: usize = args.next().unwrap().parse().unwrap();

    timely::execute_from_args(args, move |worker| {
        let (inputs, probe) = worker.dataflow::<u32, _, _>(|scope| {
            let (input_a, a) = scope.new_collection::<((), CloneTracker), i32>();
            let (input_b, b) = scope.new_collection::<((), u32), i32>();

            let a = a.arrange_by_key();
            let b = b.arrange_by_key();

            let probe = a.join_core(&b, |_key, a, &b| Some((a.clone(), b))).probe();

            ((input_a, input_b), probe)
        });

        let (mut input_a, mut input_b) = inputs;

        for i in 0..1000 {
            if i > 0 {
                input_a.update_at(((), CloneTracker(i - 1)), i, -1);
                input_b.update_at(((), i - 1), i, -1);
            }
            input_a.update_at(((), CloneTracker(i)), i, 1);
            input_b.update_at(((), i), i, 1);
        }
        input_b.advance_to(1000);
        input_b.flush();

        for i in (0..1000).step_by(step_size) {
            input_a.advance_to(i + 1);
            input_a.flush();
            worker.step_while(|| probe.less_than(input_a.time()));
        }

        println!("Value cloned {} times", CLONE_COUNT.get());
    })
    .unwrap();
}
