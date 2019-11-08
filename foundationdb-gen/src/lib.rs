extern crate inflector;
extern crate xml;
#[macro_use]
extern crate failure;

type Result<T> = std::result::Result<T, failure::Error>;

use inflector::cases::classcase;
use inflector::cases::screamingsnakecase;
use xml::attribute::OwnedAttribute;
use xml::reader::{EventReader, XmlEvent};

#[derive(Debug)]
struct FdbScope {
    name: String,
    options: Vec<FdbOption>,
}
impl FdbScope {
    fn gen_ty(&self) -> String {
        let mut s = String::new();
        let with_ty = self.with_ty();

        if with_ty {
            s += "#[derive(Clone,Debug)]\n";
        } else {
            s += "#[derive(Clone,Copy,Debug)]\n";
        }
        s += "pub enum ";
        s += &self.name;
        s += "{\n";

        let with_ty = self.with_ty();
        for option in self.options.iter() {
            s += &option.gen_ty(with_ty);
        }
        s += "}\n";

        s
    }

    fn gen_impl(&self) -> String {
        let mut s = String::new();
        s += "impl ";
        s += &self.name;
        s += " {\n";

        s += &self.gen_code();
        s += &self.gen_apply();

        s += "}\n";
        s
    }

    fn gen_code(&self) -> String {
        let mut s = String::new();
        s += &format!("pub fn code(&self) -> fdb::FDB{} {{\n", self.name);
        s += "match *self {\n";

        let enum_prefix = self.c_enum_prefix();
        let with_ty = self.with_ty();

        for option in self.options.iter() {
            let rs_name = match option.name.as_ref() {
                "AppendIfFit" => "AppendIfFits",
                s => s
            };

            s += &format!("{}::{}", self.name, rs_name);

            if with_ty {
                if let Some(_ty) = option.get_ty() {
                    s += "(ref _v)"
                }
            }

            let mut enum_name = screamingsnakecase::to_screaming_snake_case(&option.name);
            if self.name != "MutationType" || option.name == "AppendIfFit" {
                enum_name = Self::fix_enum_name(&enum_name);
            }

            s += &format!(" => fdb::{}{},\n", enum_prefix, enum_name);
        }

        s += "}\n}\n";

        s
    }

    fn fix_enum_name(name: &str) -> String {
        let tab = [
            ("BYTE", "BYTES"),
            ("WATCH", "WATCHES"),
            ("PEER", "PEERS"),
            ("THREAD", "THREADS"),
            ("KEY", "KEYS"),
            ("FIT", "FITS"),
            ("PROXY", "PROXIES"),
        ];

        for &(ref from, ref to) in tab.iter() {
            if name.ends_with(from) {
                return format!("{}{}", &name[0..(name.len() - from.len())], to);
            }
        }
        name.to_owned()
    }

    fn gen_apply(&self) -> String {
        let fn_name = match self.apply_fn_name() {
            Some(name) => name,
            _ => return String::new(),
        };

        let first_arg = match self.apply_arg_name() {
            Some(name) => format!(", target: *mut fdb::{}", name),
            None => String::new(),
        };

        let mut s = String::new();
        s += &format!(
            "pub unsafe fn apply(&self{}) -> std::result::Result<(), error::Error> {{\n",
            first_arg
        );
        s += "let code = self.code();\n";
        s += "let err = match *self {\n";

        let args = if first_arg.is_empty() {
            "code"
        } else {
            "target, code"
        };

        for option in self.options.iter() {
            s += &format!("{}::{}", self.name, option.name);

            match option.param_type {
                FdbOptionTy::Empty => {
                    s += &format!(" => fdb::{}({}, std::ptr::null(), 0),\n", fn_name, args);
                }
                FdbOptionTy::Int => {
                    s += "(v) => {\n";
                    s += "let data: [u8;8] = std::mem::transmute(v as i64);\n";
                    s += &format!(
                        "fdb::{}({}, data.as_ptr() as *const u8, 8)\n",
                        fn_name, args
                    );
                    s += "},";
                }
                FdbOptionTy::Bytes => {
                    s += "(ref v) => {\n";
                    s += &format!(
                        "fdb::{}({}, v.as_ptr() as *const u8, v.len() as i32)\n",
                        fn_name, args
                    );
                    s += "},";
                }
                FdbOptionTy::Str => {
                    s += "(ref v) => {\n";
                    s += &format!(
                        "fdb::{}({}, v.as_ptr() as *const u8, v.len() as i32)\n",
                        fn_name, args
                    );
                    s += "},";
                }
            }
        }

        s += "};\n";
        s += "if err != 0 { Err(error::Error::from(err)) } else { Ok(()) }\n";
        s += "}\n";

        s
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
    code: i32,
    param_type: FdbOptionTy,
    param_description: String,
    description: String,
    hidden: bool,
}

impl FdbOption {
    fn gen_ty(&self, with_ty: bool) -> String {
        let mut s = String::new();

        if !self.param_description.is_empty() {
            s += "/// ";
            s += &self.param_description;
            s += "\n///\n";
        }
        if !self.description.is_empty() {
            s += "/// ";
            s += &self.description;
            s += "\n";
        }

        s += &self.name;
        if with_ty {
            if let Some(ty) = self.get_ty() {
                s += "(";
                s += ty;
                s += ")";
            }
        }
        s += ",\n";
        s
    }

    fn get_ty(&self) -> Option<&'static str> {
        match self.param_type {
            FdbOptionTy::Int => Some("u32"),
            FdbOptionTy::Str => Some("String"),
            FdbOptionTy::Bytes => Some("Vec<u8>"),
            FdbOptionTy::Empty => None,
        }
    }
}

impl From<Vec<OwnedAttribute>> for FdbOption {
    fn from(attrs: Vec<OwnedAttribute>) -> Self {
        let mut opt = Self::default();
        for attr in attrs {
            let v = attr.value;
            match attr.name.local_name.as_str() {
                "name" => {
                    opt.name = classcase::to_class_case(&v);
                    if opt.name == "AppendIfFit" {
                        opt.name = String::from("AppendIfFits");
                    };
                }
                "code" => {
                    opt.code = v.parse().unwrap();
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
                    _ => panic!("unexpected boolean value: {}", v),
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
    while let Some(e) = parser.next() {
        let e = e?;
        match e {
            XmlEvent::StartElement {
                name, attributes, ..
            } => {
                ensure!(name.local_name == "Option", "unexpected token");

                let option = FdbOption::from(attributes);
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

#[cfg(target_os = "linux")]
const OPTIONS_DATA: &[u8] = include_bytes!("/usr/include/foundationdb/fdb.options");

#[cfg(target_os = "macos")]
const OPTIONS_DATA: &[u8] = include_bytes!("/usr/local/include/foundationdb/fdb.options");

#[cfg(target_os = "windows")]
const OPTIONS_DATA: &[u8] = include_bytes!("C:/Program Files/foundationdb/include/foundationdb/fdb.options");

pub fn emit() -> Result<String> {
    let mut reader = OPTIONS_DATA.as_ref();
    let parser = EventReader::new(&mut reader);
    let mut iter = parser.into_iter();
    let mut scopes = Vec::new();

    while let Some(e) = iter.next() {
        match e? {
            XmlEvent::StartElement {
                name, attributes, ..
            } => {
                if name.local_name == "Scope" {
                    let scope_name = attributes
                        .into_iter()
                        .find(|attr| attr.name.local_name == "name")
                        .unwrap();

                    let options = on_scope(&mut iter)?;
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

    let mut result = format!(
        "{}\n{}\n{}\n\n",
        "use std;", "use error;", "use foundationdb_sys as fdb;"
    );

    for scope in scopes.iter() {
        result.push_str(&format!("{}", scope.gen_ty()));
        result.push_str(&format!("{}", scope.gen_impl()));
    }

    Ok(result)
}
