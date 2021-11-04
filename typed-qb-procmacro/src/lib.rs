mod iflift;
mod parsing;

use parsing::codegen::ToTokenStream;
use parsing::create::SqlStatements;
use parsing::expr::Expr;
use proc_macro2::TokenStream;
use quote::ToTokens;
use syn::{parse_macro_input, Block};

use crate::iflift::{ConditionTree, IfLifting};

#[proc_macro]
pub fn expr(tokens: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let decl = parse_macro_input!(tokens as Expr);

    let mut ts = TokenStream::new();
    decl.to_token_stream(&mut ts);

    ts.into()
}

#[proc_macro]
pub fn tables(tokens: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let tables = parse_macro_input!(tokens as SqlStatements);

    let mut ts = TokenStream::new();
    tables.to_token_stream(&mut ts);

    ts.into()
}

#[proc_macro]
pub fn lift_if(tokens: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let stmts = parse_macro_input!(tokens with Block::parse_within);
    let parsed = Block {
        brace_token: Default::default(),
        stmts,
    };

    let mut conditions = ConditionTree::new();
    parsed.extract_conditions(&mut conditions);

    let (num, binary_tree) = conditions.create_binary_tree(0);
    
    let mut choices = vec![true; num];
    let block = binary_tree.codegen(&mut choices, &parsed, &conditions);

    let mut ts = TokenStream::new();
    block.to_tokens(&mut ts);
    ts.into()
}
