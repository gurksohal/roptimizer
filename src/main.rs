use crate::catalog::Catalog;

mod catalog;


fn main() {
    let catalog = Catalog::build().unwrap();
    println!("{catalog:?}");
}
