extern crate timely;
extern crate differential_dataflow;
extern crate dogsdogsdogs;

use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::broadcast::Broadcast;
use timely::dataflow::operators::Map;

use differential_dataflow::AsCollection;
use differential_dataflow::Hashable;
use differential_dataflow::input::Input;
use differential_dataflow::operators::arrange::Arrange;
use differential_dataflow::trace::implementations::ord::OrdValSpine;

use dogsdogsdogs::operators::half_join;

fn main() {
    timely::execute_from_args(::std::env::args(), move |worker| {
        let worker_idx = worker.index();
        let (mut handle1, mut handle2, probe) = worker.dataflow::<u64, _, _>(|scope| {
            let (handle1, input1) = scope.new_collection();
            let (handle2, input2) = scope.new_collection();

            let input1 = input1.inspect(move |x| println!("input1 from worker {worker_idx}: {x:?}"));
            let input2 = input2.inspect(move |x| println!("input2 from worker {worker_idx}: {x:?}"));

            // Here we create an arrangement on `((), V)` values *but* we force the data to be
            // distributed among workers by using `arrange_core` with an exchange function that
            // exchanges by V
            let arranged1 = input1.arrange_core::<_, OrdValSpine<_, _, _, _>>(
                Exchange::new(|update: &(((), u64), _, _)| update.0.1.hashed()),
                "DistributeData");
            let arranged2 = input2.arrange_core::<_, OrdValSpine<_, _, _, _>>(
                Exchange::new(|update: &(((), String), _, _)| update.0.1.hashed()),
                "DistributeData");

            // Grab the stream of changes, stash the initial time as payload, and broadcast to all workers
            let changes1 = input1.inner.map(|((k,v),t,r)| ((k,v,t.clone()),t,r)).broadcast().as_collection();
            let changes2 = input2.inner.map(|((k,v),t,r)| ((k,v,t.clone()),t,r)).broadcast().as_collection();

            // At this point each workers has a shard of the arranged data and the two broadcasted streams

            let path1 = half_join(
                &changes1,
                arranged2,
                |t| t.clone(),
                |t1,t2| t1.lt(t2),  // This one ignores concurrent updates.
                |key, val1, val2| (key.clone(), (val1.clone(), val2.clone())),
            );

            let path2 = half_join(
                &changes2,
                arranged1,
                |t| t.clone(),
                |t1,t2| t1.le(t2),  // This one can "see" concurrent updates.
                |key, val1, val2| (key.clone(), (val2.clone(), val1.clone())),
            );

            let cross_join = path1.concat(&path2);

            let probe = cross_join
                .inspect(move |d| {
                    println!("worker {} produced: {:?}", worker_idx, d);
                })
                .probe();

            (handle1, handle2, probe)
        });

        // Introduce (key, value) data through worker 0 where the key is the empty tuple
        if worker_idx == 0 {
            handle1.insert(((), 1u64));
            handle1.advance_to(1);
            handle1.insert(((), 2));
            handle1.advance_to(2);
            handle1.flush();

            handle2.insert(((), "apple".to_string()));
            handle2.advance_to(1);
            handle2.insert(((), "orange".to_string()));
            handle2.advance_to(2);
            handle2.flush();
        }

        while probe.less_than(handle1.time()) {
            worker.step();
        }
    }).unwrap();
}
