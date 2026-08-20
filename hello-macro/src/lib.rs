use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput, ItemFn};

#[proc_macro_derive(Hello)]
pub fn derive_hello(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;

    let expanded = quote! {
        impl Hello for #name {
            fn hello(&self) {
                println!("Hello from {}", stringify!(#name));
            }
        }
    };

    TokenStream::from(expanded)
}

#[proc_macro_attribute]
pub fn log_call(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(item as ItemFn);
    let name = &input_fn.sig.ident;
    let vis = &input_fn.vis;
    let sig = &input_fn.sig;
    let block = &input_fn.block;
    let attrs = &input_fn.attrs;

    let expanded = quote! {
        #(#attrs)*
        #vis #sig {
            println!("[log_call] 进入函数: {}", stringify!(#name));
            let __result = #block;
            println!("[log_call] 离开函数: {}", stringify!(#name));
            __result
        }
    };

    TokenStream::from(expanded)
}

#[proc_macro]
pub fn make_answer(_input: TokenStream) -> TokenStream {
    quote! {
        42
    }
    .into()
}
