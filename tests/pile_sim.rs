use anybytes::Bytes;
use proptest::prelude::*;
use std::collections::HashMap;
use std::collections::HashSet;
use triblespace::core::blob::schemas::UnknownBlob;
use triblespace::core::repo::PushResult;
use triblespace::core::value::schemas::hash::Blake3;
use triblespace::prelude::blobschemas::SimpleArchive;
use triblespace::prelude::valueschemas::Handle;
use triblespace::prelude::*;

#[derive(Debug, Clone)]
enum Op {
    Put(Vec<u8>),
    Flush,
    Refresh,
    Get(usize),
    BranchUpdate { branch: usize, handle: usize },
    BranchHead(usize),
    BranchList,
}

#[derive(Debug, Clone)]
enum ActorOp {
    Run { actor: usize, op: Op },
    Check,
}

#[derive(Debug, Clone)]
struct Scenario {
    actors: usize,
    ops: Vec<ActorOp>,
}

fn actor_op_strategy(actors: usize, branches: usize) -> impl Strategy<Value = ActorOp> {
    let data = prop::collection::vec(any::<u8>(), 0..32);
    let idx = 0usize..20;
    prop_oneof![
        (0..actors, data.clone()).prop_map(|(actor, data)| ActorOp::Run {
            actor,
            op: Op::Put(data)
        }),
        (0..actors).prop_map(|actor| ActorOp::Run {
            actor,
            op: Op::Flush
        }),
        (0..actors).prop_map(|actor| ActorOp::Run {
            actor,
            op: Op::Refresh
        }),
        (0..actors, idx.clone()).prop_map(|(actor, i)| ActorOp::Run {
            actor,
            op: Op::Get(i)
        }),
        (0..actors, 0..branches, idx.clone()).prop_map(|(actor, branch, i)| ActorOp::Run {
            actor,
            op: Op::BranchUpdate { branch, handle: i }
        }),
        (0..actors, 0..branches).prop_map(|(actor, branch)| ActorOp::Run {
            actor,
            op: Op::BranchHead(branch)
        }),
        (0..actors).prop_map(|actor| ActorOp::Run {
            actor,
            op: Op::BranchList
        }),
        Just(ActorOp::Check),
    ]
}

fn scenario_strategy(max_actors: usize) -> impl Strategy<Value = Scenario> {
    (1..=max_actors, 1usize..=4).prop_flat_map(move |(actors, branches)| {
        let op = actor_op_strategy(actors, branches);
        prop::collection::vec(op, 1..20).prop_map(move |ops| Scenario { actors, ops })
    })
}

fn branch_id(idx: usize) -> Id {
    let mut raw = [0u8; 16];
    raw[0] = (idx as u8).saturating_add(1);
    Id::new(raw).unwrap()
}

proptest! {
    #[test]
    fn pile_operation_sequences_are_consistent(
        scenario in scenario_strategy(4)
    ) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sim.pile");
        let mut piles: Vec<Pile> =
            (0..scenario.actors).map(|_| Pile::open(&path).unwrap()).collect();
        let mut expected: HashMap<Value<Handle<Blake3, UnknownBlob>>, Vec<u8>> = HashMap::new();
        let mut handles: Vec<Value<Handle<Blake3, UnknownBlob>>> = Vec::new();
        let mut branches: HashMap<Id, Value<Handle<Blake3, SimpleArchive>>> = HashMap::new();

        for op in scenario.ops {
            match op {
                ActorOp::Run { actor, op } => match op {
                    Op::Put(data) => {
                        let blob: Blob<UnknownBlob> =
                            Blob::new(Bytes::from_source(data.clone()));
                        let handle = piles[actor].put(blob).unwrap();
                        expected.insert(handle, data);
                        handles.push(handle);
                    }
                    Op::Flush => {
                        piles[actor].flush().unwrap();
                    }
                    Op::Refresh => {
                        let _ = piles[actor].refresh();
                    }
                    Op::Get(i) => {
                        if !handles.is_empty() {
                            let handle = handles[i % handles.len()];
                            piles[actor].refresh().unwrap();
                            if let Ok(blob) = piles[actor]
                                .reader()
                                .unwrap()
                                .get::<Blob<UnknownBlob>, _>(handle)
                            {
                                prop_assert_eq!(
                                    blob.bytes.as_ref(),
                                    expected.get(&handle).unwrap().as_slice()
                                );
                            }
                        }
                    }
                    Op::BranchUpdate { branch, handle } => {
                        if !handles.is_empty() {
                            let id = branch_id(branch);
                            let h = handles[handle % handles.len()].transmute();
                            let old = branches.get(&id).copied();
                            let res = piles[actor].update(id, old, Some(h)).unwrap();
                            match res {
                                PushResult::Success() => {
                                    branches.insert(id, h);
                                }
                                PushResult::Conflict(c) => {
                                    prop_assert_eq!(c, old);
                                    branches.insert(id, h);
                                }
                            }
                        }
                    }
                    Op::BranchHead(branch) => {
                        let id = branch_id(branch);
                        piles[actor].refresh().unwrap();
                        let head = piles[actor].head(id).unwrap();
                        prop_assert_eq!(head, branches.get(&id).copied());
                    }
                    Op::BranchList => {
                        piles[actor].refresh().unwrap();
                        let iter = piles[actor].branches().unwrap();
                        let found: HashSet<Id> = iter.map(|r| r.unwrap()).collect();
                        let expected_ids: HashSet<Id> = branches.keys().copied().collect();
                        prop_assert_eq!(found, expected_ids);
                    }
                },
                ActorOp::Check => {
                    for pile in &mut piles {
                        pile.refresh().unwrap();
                    }
                    for pile in &mut piles {
                        let reader = pile.reader().unwrap();
                        for (handle, data) in &expected {
                            if let Ok(blob) = reader.get::<Blob<UnknownBlob>, _>(*handle) {
                                prop_assert_eq!(blob.bytes.as_ref(), data.as_slice());
                            }
                        }
                        let iter = pile.branches().unwrap();
                        let found: HashSet<Id> = iter.map(|r| r.unwrap()).collect();
                        let expected_ids: HashSet<Id> = branches.keys().copied().collect();
                        prop_assert_eq!(found, expected_ids);
                        for (id, head) in &branches {
                            let h = pile.head(*id).unwrap();
                            prop_assert_eq!(h, Some(*head));
                        }
                    }
                }
            }
        }

        for pile in &mut piles {
            pile.flush().unwrap();
            pile.refresh().unwrap();
        }
        for pile in piles {
            pile.close().unwrap();
        }
        let mut pile_final: Pile = Pile::open(&path).unwrap();
        pile_final.restore().unwrap();
        let reader = pile_final.reader().unwrap();
        for (handle, data) in &expected {
            let blob = reader.get::<Blob<UnknownBlob>, _>(*handle).unwrap();
            assert_eq!(blob.bytes.as_ref(), data.as_slice());
        }
        let iter = pile_final.branches().unwrap();
        let found: HashSet<Id> = iter.map(|r| r.unwrap()).collect();
        let expected_ids: HashSet<Id> = branches.keys().copied().collect();
        assert_eq!(found, expected_ids);
        for (id, head) in &branches {
            let h = pile_final.head(*id).unwrap();
            assert_eq!(h, Some(*head));
        }
        pile_final.close().unwrap();
    }
}
