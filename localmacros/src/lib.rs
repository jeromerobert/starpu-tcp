extern crate proc_macro;
extern crate syn;
extern crate quote;
extern crate proc_macro2;
use proc_macro::TokenStream;
use syn::{parse_macro_input, ItemFn, parse_quote};
use quote::quote;

//https://www.gushiciku.cn/pl/pAaU
//https://github.com/dtolnay/no-panic/blob/master/src/lib.rs
#[proc_macro_attribute]
pub fn starpu_mpi_func(_attr: TokenStream, function: TokenStream) -> TokenStream {
    let mut function = parse_macro_input!(function as ItemFn);
    function.attrs.push(parse_quote!(#[no_mangle]));
    function.vis = parse_quote!(pub);
    function.sig.unsafety = parse_quote!(unsafe);
    // Add a prefix (not used anymore)
    // let newident = format!("starpu_mpi_{}", function.sig.ident);
    // function.sig.ident = syn::Ident::new(&newident, proc_macro2::Span::call_site());
    function.sig.abi = parse_quote!(extern "C");
    let gen = quote! {#function};
    // println!("{}", gen);
    return gen.into();
}