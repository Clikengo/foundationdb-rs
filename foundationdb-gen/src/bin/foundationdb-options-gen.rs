extern crate foundationdb_gen;

fn main() {
    let code = foundationdb_gen::emit().expect("couldn't generate options.rs code!");
    println!("{}", code);
}
