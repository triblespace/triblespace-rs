use triblespace::prelude::*;

mod ns {
    use triblespace::prelude::*;

    attributes! {
        "71C9AB6D8DF645C894D4473ED04E12CC" as attr: valueschemas::GenId;
        "4E6F8648635C4F84A79F4C54602C1E5A" as label: valueschemas::ShortString;
        "32C5243958B147B6ACDEAB7D5EEAC669" as alias: valueschemas::ShortString;
    }
}

fn main() {
    let set = TribleSet::new();
    let delta = set.clone();

    let _: Vec<_> = find!((entity: Value<_>), pattern!(&set, [
        { ?entity @ ns::attr: _?value }
    ])).collect();

    let _: Vec<_> = find!((entity: Value<_>), pattern_changes!(&set, &delta, [
        { ?entity @ ns::attr: _?value }
    ])).collect();

    let _: Vec<_> = find!((entity: Value<_>), pattern!(&set, [
        { ?entity @ ns::label: _?text, ns::alias: _?text }
    ])).collect();
}
