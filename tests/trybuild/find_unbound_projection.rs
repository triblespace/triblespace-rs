use triblespace::prelude::*;

mod social {
    use triblespace::prelude::*;

    attributes! {
        "C2C8D4D6E3E5479EA6F4D71D979CD3CE" as friend: inlineschemas::GenId;
    }
}

fn main() {
    let mut kb = TribleSet::new();
    let alice = fucid();
    let bob = fucid();
    kb += entity! { &alice @ social::friend: &bob };

    let _ = find!(
        projected: Id,
        pattern!(&kb, [{ alice.id @ social::friend: &bob.id }])
    );
}
