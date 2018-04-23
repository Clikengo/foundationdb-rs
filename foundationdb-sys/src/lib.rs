#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

include!("bindings.rs");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api_version() {
        assert_eq!(FDB_API_VERSION, 510);
    }
}
