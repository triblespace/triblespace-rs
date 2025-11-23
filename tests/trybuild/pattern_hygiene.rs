use triblespace::prelude::*;

mod ns {
    use triblespace::prelude::*;

    attributes! {
        "67D58765E2104ACCA9F26B6E186BAABC" as label: valueschemas::ShortString;
    }
}

fn main() {
    let base = TribleSet::new();
    let updated = base.clone();
    let delta = updated.difference(&base);

    let __ctx = ();
    let __a0 = ();

    let _ = (__ctx, __a0);

    let _: Vec<_> = find!(
        (__ctx: Value<valueschemas::GenId>, __a0: Value<valueschemas::ShortString>),
        pattern!(&base, [
            { ?__ctx @ ns::label: ?__a0 }
        ])
    )
    .collect::<Vec<_>>();

    let _: Vec<_> = find!(
        (__ctx: Value<valueschemas::GenId>, __a0: Value<valueschemas::ShortString>),
        pattern_changes!(&updated, &delta, [
            { ?__ctx @ ns::label: ?__a0 }
        ])
    )
    .collect::<Vec<_>>();
}
