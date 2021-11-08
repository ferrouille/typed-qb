use proc_macro2::{Span, TokenStream};
use quote::format_ident;
use syn::{Data, DeriveInput, LitStr};

pub fn gen(input: &DeriveInput) -> TokenStream {
    let ident = input.ident.clone();
    if let Data::Struct(s) = &input.data {
        let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

        let mut tokens = TokenStream::new();
        for field in s.fields.iter() {
            let fieldname = field
                .ident
                .as_ref()
                .expect("All fields must have names")
                .to_string();
            let ty = field.ty.clone();
            tokens.extend(quote::quote! {
                #[automatically_derived]
                impl #impl_generics ::typed_qb::FieldType<#fieldname> for #ident #ty_generics #where_clause {
                    type Ty = #ty;
                }
            });
        }

        let fieldidents = s
            .fields
            .iter()
            .map(|field| field.ident.as_ref().expect("All fields must have names"))
            .collect::<Vec<_>>();
        let fieldnames = s
            .fields
            .iter()
            .map(|field| field.ident.as_ref().expect("All fields must have names"))
            .map(|ident| LitStr::new(&ident.to_string(), Span::call_site()))
            .collect::<Vec<_>>();
        let fieldtypes = s
            .fields
            .iter()
            .map(|field| field.ty.clone())
            .collect::<Vec<_>>();
        let gs = s
            .fields
            .iter()
            .enumerate()
            .map(|(n, _)| format_ident!("K{}", n))
            .collect::<Vec<_>>();
        tokens.extend(quote::quote! {
            impl #impl_generics #ident #ty_generics #where_clause {
                pub fn __create_from_row<#(#gs: Into<#fieldtypes>,)*D: #(::typed_qb::WithField<#fieldnames, Output = #gs> +)*>(data: D) -> Self {
                    Self {
                        #(
                            #fieldidents: <D as ::typed_qb::WithField<{ #fieldnames }>>::value(&data).into(),
                        )*
                    }
                }
            }
        });

        tokens
    } else {
        unimplemented!()
    }
}
