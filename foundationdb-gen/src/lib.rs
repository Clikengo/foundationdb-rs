extern crate xml;
#[macro_use]
extern crate failure;

type Result<T> = std::result::Result<T, failure::Error>;

use std::fmt;
use std::fmt::Write;
use xml::attribute::OwnedAttribute;
use xml::reader::{EventReader, XmlEvent};

const TAB1: &str = "    ";
const TAB2: &str = "        ";
const TAB3: &str = "            ";
const TAB4: &str = "                ";

#[derive(Debug)]
struct FdbScope {
    name: String,
    options: Vec<FdbOption>,
}
impl FdbScope {
    fn gen_ty<W: fmt::Write>(&self, w: &mut W) -> fmt::Result {
        let with_ty = self.with_ty();

        if with_ty {
            writeln!(w, "#[derive(Clone, Debug)]")?;
        } else {
            writeln!(w, "#[derive(Clone, Copy, Debug)]")?;
        }
        writeln!(w, "#[non_exhaustive]")?;
        writeln!(w, "pub enum {name} {{", name = self.name)?;

        let with_ty = self.with_ty();
        for option in self.options.iter() {
            option.gen_ty(w, with_ty)?;
        }
        writeln!(w, "}}")
    }

    fn gen_impl<W: fmt::Write>(&self, w: &mut W) -> fmt::Result {
        writeln!(w, "impl {name} {{", name = self.name)?;
        self.gen_code(w)?;
        self.gen_apply(w)?;
        writeln!(w, "}}")
    }

    fn gen_code<W: fmt::Write>(&self, w: &mut W) -> fmt::Result {
        writeln!(
            w,
            "{t}pub fn code(&self) -> fdb_sys::FDB{name} {{",
            t = TAB1,
            name = self.name,
        )?;
        writeln!(w, "{t}match *self {{", t = TAB2)?;

        let enum_prefix = self.c_enum_prefix();
        let with_ty = self.with_ty();

        for option in self.options.iter() {
            writeln!(
                w,
                "{t}{scope}::{name}{param} => fdb_sys::{enum_prefix}{code},",
                t = TAB3,
                scope = self.name,
                name = option.name,
                param = if let (true, Some(..)) = (with_ty, option.get_ty()) {
                    "(..)"
                } else {
                    ""
                },
                enum_prefix = enum_prefix,
                code = option.c_name,
            )?;
        }

        writeln!(w, "{t}}}", t = TAB2)?;
        writeln!(w, "{t}}}", t = TAB1)
    }

    fn gen_apply<W: fmt::Write>(&self, w: &mut W) -> fmt::Result {
        let fn_name = match self.apply_fn_name() {
            Some(name) => name,
            _ => return Ok(()),
        };

        let first_arg = match self.apply_arg_name() {
            Some(name) => format!(", target: *mut fdb_sys::{}", name),
            None => String::new(),
        };

        writeln!(
            w,
            "{t}pub unsafe fn apply(&self{args}) -> FdbResult<()> {{",
            t = TAB1,
            args = first_arg
        )?;
        writeln!(w, "{t}let code = self.code();", t = TAB2)?;
        writeln!(w, "{t}let err = match *self {{", t = TAB2)?;

        let args = if first_arg.is_empty() {
            "code"
        } else {
            "target, code"
        };

        for option in self.options.iter() {
            write!(w, "{}{}::{}", TAB3, self.name, option.name)?;
            match option.param_type {
                FdbOptionTy::Empty => {
                    writeln!(
                        w,
                        " => fdb_sys::{}({}, std::ptr::null(), 0),",
                        fn_name, args
                    )?;
                }
                FdbOptionTy::Int => {
                    writeln!(w, "(v) => {{")?;
                    writeln!(
                        w,
                        "{}let data: [u8;8] = std::mem::transmute(v as i64);",
                        TAB4,
                    )?;
                    writeln!(
                        w,
                        "{}fdb_sys::{}({}, data.as_ptr() as *const u8, 8)",
                        TAB4, fn_name, args
                    )?;
                    writeln!(w, "{t}}}", t = TAB3)?;
                }
                FdbOptionTy::Bytes => {
                    writeln!(w, "(ref v) => {{")?;
                    writeln!(
                        w,
                        "{}fdb_sys::{}({}, v.as_ptr() as *const u8, \
                         i32::try_from(v.len()).expect(\"len to fit in i32\"))\n",
                        TAB4, fn_name, args
                    )?;
                    writeln!(w, "{t}}}", t = TAB3)?;
                }
                FdbOptionTy::Str => {
                    writeln!(w, "(ref v) => {{")?;
                    writeln!(
                        w,
                        "{}fdb_sys::{}({}, v.as_ptr() as *const u8, \
                         i32::try_from(v.len()).expect(\"len to fit in i32\"))\n",
                        TAB4, fn_name, args
                    )?;
                    writeln!(w, "{t}}}", t = TAB3)?;
                }
            }
        }

        writeln!(w, "{t}}};", t = TAB2)?;
        writeln!(
            w,
            "{t}if err != 0 {{ Err(FdbError::from_code(err)) }} else {{ Ok(()) }}",
            t = TAB2,
        )?;
        writeln!(w, "{t}}}", t = TAB1)
    }

    fn with_ty(&self) -> bool {
        self.apply_fn_name().is_some()
    }

    fn c_enum_prefix(&self) -> &'static str {
        match self.name.as_str() {
            "NetworkOption" => "FDBNetworkOption_FDB_NET_OPTION_",
            "ClusterOption" => "FDBClusterOption_FDB_CLUSTER_OPTION_",
            "DatabaseOption" => "FDBDatabaseOption_FDB_DB_OPTION_",
            "TransactionOption" => "FDBTransactionOption_FDB_TR_OPTION_",
            "StreamingMode" => "FDBStreamingMode_FDB_STREAMING_MODE_",
            "MutationType" => "FDBMutationType_FDB_MUTATION_TYPE_",
            "ConflictRangeType" => "FDBConflictRangeType_FDB_CONFLICT_RANGE_TYPE_",
            "ErrorPredicate" => "FDBErrorPredicate_FDB_ERROR_PREDICATE_",
            ty => panic!("unknown Scope name: `{}`", ty),
        }
    }

    fn apply_arg_name(&self) -> Option<&'static str> {
        let s = match self.name.as_str() {
            "ClusterOption" => "FDBCluster",
            "DatabaseOption" => "FDBDatabase",
            "TransactionOption" => "FDBTransaction",
            _ => return None,
        };
        Some(s)
    }

    fn apply_fn_name(&self) -> Option<&'static str> {
        let s = match self.name.as_str() {
            "NetworkOption" => "fdb_network_set_option",
            "ClusterOption" => "fdb_cluster_set_option",
            "DatabaseOption" => "fdb_database_set_option",
            "TransactionOption" => "fdb_transaction_set_option",
            _ => return None,
        };
        Some(s)
    }
}

#[derive(Clone, Copy, Debug)]
enum FdbOptionTy {
    Empty,
    Int,
    Str,
    Bytes,
}
impl std::default::Default for FdbOptionTy {
    fn default() -> Self {
        FdbOptionTy::Empty
    }
}

#[derive(Default, Debug)]
struct FdbOption {
    name: String,
    c_name: String,
    code: i32,
    param_type: FdbOptionTy,
    param_description: String,
    description: String,
    hidden: bool,
    default_for: Option<i32>,
    persistent: bool,
}

impl FdbOption {
    fn gen_ty<W: fmt::Write>(&self, w: &mut W, with_ty: bool) -> fmt::Result {
        if !self.param_description.is_empty() {
            writeln!(w, "{t}/// {desc}", t = TAB1, desc = self.param_description)?;
            writeln!(w, "{t}///", t = TAB1)?;
        }
        if !self.description.is_empty() {
            writeln!(w, "{t}/// {desc}", t = TAB1, desc = self.description)?;
        }

        if let (true, Some(ty)) = (with_ty, self.get_ty()) {
            writeln!(w, "{t}{name}({ty}),", t = TAB1, name = self.name, ty = ty)?;
        } else {
            writeln!(w, "{t}{name},", t = TAB1, name = self.name)?;
        }
        Ok(())
    }

    fn get_ty(&self) -> Option<&'static str> {
        match self.param_type {
            FdbOptionTy::Int => Some("i32"),
            FdbOptionTy::Str => Some("String"),
            FdbOptionTy::Bytes => Some("Vec<u8>"),
            FdbOptionTy::Empty => None,
        }
    }
}

fn to_rs_enum_name(v: &str) -> String {
    let mut is_start_of_word = true;
    v.chars()
        .filter_map(|c| {
            if c == '_' {
                is_start_of_word = true;
                None
            } else if is_start_of_word {
                is_start_of_word = false;
                Some(c.to_ascii_uppercase())
            } else {
                Some(c)
            }
        })
        .collect()
}

impl From<Vec<OwnedAttribute>> for FdbOption {
    fn from(attrs: Vec<OwnedAttribute>) -> Self {
        let mut opt = Self::default();
        for attr in attrs {
            let v = attr.value;
            match attr.name.local_name.as_str() {
                "name" => {
                    opt.name = to_rs_enum_name(v.as_str());
                    opt.c_name = v.to_uppercase();
                }
                "code" => {
                    opt.code = v.parse().expect("code to be a i32");
                }
                "paramType" => {
                    opt.param_type = match v.as_str() {
                        "Int" => FdbOptionTy::Int,
                        "String" => FdbOptionTy::Str,
                        "Bytes" => FdbOptionTy::Bytes,
                        "" => FdbOptionTy::Empty,
                        ty => panic!("unexpected param_type: {}", ty),
                    };
                }
                "paramDescription" => {
                    opt.param_description = v;
                }
                "description" => {
                    opt.description = v;
                }
                "hidden" => match v.as_str() {
                    "true" => opt.hidden = true,
                    "false" => opt.hidden = false,
                    _ => panic!("unexpected boolean value in 'hidden': {}", v),
                },
                "defaultFor" => {
                    opt.default_for = Some(v.parse().expect("defaultFor to be a i32"));
                }
                "persistent" => match v.as_str() {
                    "true" => opt.persistent = true,
                    "false" => opt.persistent = false,
                    _ => panic!("unexpected boolean value in 'persistent': {}", v),
                },
                attr => {
                    panic!("unexpected option attribute: {}", attr);
                }
            }
        }
        opt
    }
}

fn on_scope<I>(parser: &mut I) -> Result<Vec<FdbOption>>
where
    I: Iterator<Item = xml::reader::Result<XmlEvent>>,
{
    let mut options = Vec::new();
    for e in parser {
        let e = e?;
        match e {
            XmlEvent::StartElement {
                name, attributes, ..
            } => {
                ensure!(name.local_name == "Option", "unexpected token");

                let option = FdbOption::from(attributes.clone());
                if !option.hidden {
                    options.push(option);
                }
            }
            XmlEvent::EndElement { name, .. } => {
                if name.local_name == "Scope" {
                    return Ok(options);
                }
            }
            _ => {}
        }
    }

    bail!("unexpected end of token");
}

#[cfg(all(not(feature = "embedded-fdb-include"), target_os = "linux"))]
const OPTIONS_DATA: &[u8] = include_bytes!("/usr/include/foundationdb/fdb.options");

#[cfg(all(not(feature = "embedded-fdb-include"), target_os = "macos"))]
const OPTIONS_DATA: &[u8] = include_bytes!("/usr/local/include/foundationdb/fdb.options");

#[cfg(all(not(feature = "embedded-fdb-include"), target_os = "windows"))]
const OPTIONS_DATA: &[u8] =
    include_bytes!("C:/Program Files/foundationdb/include/foundationdb/fdb.options");

#[cfg(all(feature = "embedded-fdb-include", feature = "fdb-5_1"))]
const OPTIONS_DATA: &[u8] = include_bytes!("../include/510/fdb.options");
#[cfg(all(feature = "embedded-fdb-include", feature = "fdb-5_2"))]
const OPTIONS_DATA: &[u8] = include_bytes!("../include/520/fdb.options");
#[cfg(all(feature = "embedded-fdb-include", feature = "fdb-6_0"))]
const OPTIONS_DATA: &[u8] = include_bytes!("../include/600/fdb.options");
#[cfg(all(feature = "embedded-fdb-include", feature = "fdb-6_1"))]
const OPTIONS_DATA: &[u8] = include_bytes!("../include/610/fdb.options");
#[cfg(all(feature = "embedded-fdb-include", feature = "fdb-6_2"))]
const OPTIONS_DATA: &[u8] = include_bytes!("../include/620/fdb.options");

pub fn emit() -> Result<String> {
    let mut reader = OPTIONS_DATA;
    let parser = EventReader::new(&mut reader);
    let mut iter = parser.into_iter();
    let mut scopes = Vec::new();

    while let Some(e) = iter.next() {
        match e.unwrap() {
            XmlEvent::StartElement {
                name, attributes, ..
            } => {
                if name.local_name == "Scope" {
                    let scope_name = attributes
                        .into_iter()
                        .find(|attr| attr.name.local_name == "name")
                        .unwrap();

                    let options = on_scope(&mut iter).unwrap();
                    scopes.push(FdbScope {
                        name: scope_name.value,
                        options,
                    });
                }
            }
            XmlEvent::EndElement { .. } => {
                //
            }
            _ => {}
        }
    }

    let mut w = String::new();
    writeln!(w, "use std::convert::TryFrom;")?;
    writeln!(w, "use crate::{{FdbError, FdbResult}};")?;
    writeln!(w, "use foundationdb_sys as fdb_sys;")?;
    for scope in scopes.iter() {
        scope.gen_ty(&mut w)?;
        scope.gen_impl(&mut w)?;
    }

    Ok(w)
}
