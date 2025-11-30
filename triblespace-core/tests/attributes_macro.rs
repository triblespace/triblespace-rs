use triblespace_core::prelude::valueschemas::ShortString;
use triblespace_core::prelude::{attributes, entity, Attribute, Id, ToValue, Value};

attributes! {
    "11111111111111111111111111111111" as pub fixed: ShortString;
    pub derived: ShortString;
    private: ShortString;
}

#[test]
fn attributes_macro_accepts_hex_and_derived_ids() {
    let expected_fixed = Id::from_hex("11111111111111111111111111111111").expect("valid hex id");
    assert_eq!(fixed.id(), expected_fixed);

    let expected_derived = Attribute::<ShortString>::from_name("derived");
    assert_eq!(derived.id(), expected_derived.id());

    let expected_private = Attribute::<ShortString>::from_name("private");
    assert_eq!(private.id(), expected_private.id());
}

#[test]
fn attributes_macro_works_in_entity_macro() {
    let val: Value<_> = "hello".to_value();
    let entity = triblespace_core::id::fucid();
    let tribles = entity! { &entity @ derived: val };

    let attr = &*derived;
    let entries: Vec<_> = tribles.iter().collect();
    assert_eq!(entries.len(), 1);
    let t = entries[0];
    assert_eq!(*t.e(), *entity);
    assert_eq!(*t.a(), attr.id());
    assert_eq!(*t.v::<ShortString>(), val);
}
