// Copyright 2017 tokio-jsonrpc Developers
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! A server that responds with the current time
//!
//! A server listening on localhost:2345. It reponds to the „now“ method, returning the current
//! unix timestamp (number of seconds since 1.1. 1970). You can also subscribe to periodic time
//! updates.

#[macro_use]
extern crate tokio_jsonrpc_derive;
#[macro_use]
extern crate tokio_jsonrpc;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate serde_json;

use tokio_jsonrpc::RpcError;

#[test]
fn derive_structs() {
    #[derive(Debug, Deserialize, Params, PartialEq)]
    struct Subscribe {
        secs: u64,
        #[serde(default)]
        nsecs: u32,
    }

    fn _inner() -> Option<Result<(), RpcError>> {
        let params = params!({"secs": 76});
        let parsed: Subscribe = parse_params!(params);
        assert_eq!(parsed, Subscribe { secs: 76, nsecs: 0 });
        Some(Ok(()))
    }

    _inner().unwrap().unwrap();

    let params = params!({"secs": 8, "nsecs": 2});
    let parsed: Subscribe = try_parse_params!(params).unwrap();
    assert_eq!(parsed, Subscribe { secs: 8, nsecs: 2 });
}

#[test]
fn derive_simple_enums() {
    #[derive(Debug, Deserialize, Params, PartialEq)]
    enum Colors {
        Red,
        Blue,
        Green,
    }

    let params = params!("Blue");
    let parsed: Colors = try_parse_params!(params).unwrap();
    assert_eq!(parsed, Colors::Blue);
}

#[test]
fn derive_unit_enums_with_discriminant() {
    #[derive(Debug, Deserialize, Params, PartialEq)]
    enum Colors {
        Red = 1,
        Blue = 2,
        Green = 3,
    }

    let params = params!(2);
    let parsed: Colors = try_parse_params!(params).unwrap();
    assert_eq!(parsed, Colors::Blue);
}

#[test]
fn derive_untagged_struct_enums() {
    // Variants are matched in order, if A was first, we would fail
    #[derive(Debug, Deserialize, Params, PartialEq)]
    #[serde(untagged)]
    enum MyEnum {
        B { x: usize, y: usize },
        C { x: usize, y: String },
        A { x: usize },
    }

    let params = params!(8);
    let parsed: MyEnum = try_parse_params!(params).unwrap();
    assert_eq!(parsed, MyEnum::A { x: 8 });

    let params = params!({ "x": 8 });
    let parsed: MyEnum = try_parse_params!(params).unwrap();
    assert_eq!(parsed, MyEnum::A { x: 8 });

    let params = params!([9, 2]);
    let parsed: MyEnum = try_parse_params!(params).unwrap();
    assert_eq!(parsed, MyEnum::B { x: 9, y: 2 });


    let params = params!({ "x": 9, "y": 2 });
    let parsed: MyEnum = try_parse_params!(params).unwrap();
    assert_eq!(parsed, MyEnum::B { x: 9, y: 2 });

    let params = params!([1, "hello"]);
    let parsed: MyEnum = try_parse_params!(params).unwrap();
    assert_eq!(parsed, MyEnum::C { x: 1, y: "hello".to_owned() });

    let params = params!({ "x": 1, "y": "hello" });
    let parsed: MyEnum = try_parse_params!(params).unwrap();
    assert_eq!(parsed, MyEnum::C { x: 1, y: "hello".to_owned() });
}
