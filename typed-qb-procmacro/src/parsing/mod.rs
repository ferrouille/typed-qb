macro_rules! gen_wrapper_struct {
    ($(#[$attrs:meta])* $enum_vis:vis , {
        $struct_name:ident { $($field_vis:vis $field_name:ident:$field_type:ty,)* },
        $($rest_struct_name:ident { $($rest_fields:tt)* },)*
    }) => {
        $(#[$attrs])* $enum_vis struct $struct_name {
            $(
                $field_vis $field_name: $field_type
            ),*
        }

        $crate::parsing::gen_wrapper_struct!($(#[$attrs])* $enum_vis, {
            $($rest_struct_name { $($rest_fields)* },)*
        });
    };
    ($(#[$attrs:meta])* $enum_vis:vis , {}) => {

    }
}

pub(crate) use gen_wrapper_struct;

macro_rules! gen_wrapper {
    ($(#[$attrs:meta])* $enum_vis:vis $enum_name:ident {
        $($struct_name:ident { $($field_vis:vis $field_name:ident:$field_type:ty),* $(,)* }),* $(,)*
    }) => {
        $(#[$attrs])*
        $enum_vis enum $enum_name {
            $($struct_name($struct_name)),*
        }

        $crate::parsing::gen_wrapper_struct!($(#[$attrs])* $enum_vis , {
            $($struct_name { $($field_vis $field_name: $field_type,)* },)*
        });
    }
}

pub(crate) use gen_wrapper;

pub mod codegen;
pub mod create;
pub mod expr;
pub mod naming;

use proc_macro2::{Delimiter, Span};
use syn::{
    Ident,
    __private::IntoSpans,
    parse::{Parse, ParseStream},
    token::{
        parsing::{peek_punct, punct},
        CustomToken,
    },
};

syn::custom_punctuation!(LeftRightArrow, <=>);

pub struct Backtick {
    _spans: [Span; 1],
}

#[allow(non_snake_case)]
pub fn Backtick<S: IntoSpans<[Span; 1]>>(s: S) -> Backtick {
    Backtick {
        _spans: s.into_spans(),
    }
}

impl CustomToken for Backtick {
    fn peek(cursor: syn::buffer::Cursor) -> bool {
        peek_punct(cursor, "`")
    }

    fn display() -> &'static str {
        "```"
    }
}

impl Parse for Backtick {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let spans: [Span; 1] = punct(input, "`")?;
        Ok(Backtick(spans))
    }
}

impl Default for Backtick {
    fn default() -> Self {
        Backtick([Span::call_site()])
    }
}

pub fn parse_keyword(s: &str, input: &ParseStream) -> syn::Result<Ident> {
    input.step(|cursor| match cursor.ident() {
        Some((ident, rest)) if ident.to_string().to_uppercase() == s.to_uppercase() => {
            Ok((ident, rest))
        }
        _ => Err(cursor.error(format!("expected {}", s))),
    })
}

pub fn parse_any(input: &ParseStream) -> syn::Result<()> {
    input.step(|cursor| match cursor.ident() {
        Some((_, rest)) => Ok(((), rest)),
        _ => match cursor.punct() {
            Some((_, rest)) => Ok(((), rest)),
            _ => match cursor.literal() {
                Some((_, rest)) => Ok(((), rest)),
                _ => match cursor.group(Delimiter::Parenthesis) {
                    Some((_, _, rest)) => Ok(((), rest)),
                    _ => Err(cursor.error("expected some token")),
                },
            },
        },
    })
}
