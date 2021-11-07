mod iflift;
mod parsing;

use crate::iflift::{ConditionTree, IfLifting};
use parsing::codegen::ToTokenStream;
use parsing::create::SqlStatements;
use parsing::expr::Expr;
use proc_macro2::TokenStream;
use quote::ToTokens;
use syn::{parse_macro_input, Block};

/// Parses SQL syntax into a Value.
/// ```rust,ignore
/// expr!(table.field = 10 AND table.id < 100)
/// ```
#[proc_macro]
pub fn expr(tokens: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let decl = parse_macro_input!(tokens as Expr);

    let mut ts = TokenStream::new();
    decl.to_token_stream(&mut ts);

    ts.into()
}

/// Generate table definitions from `CREATE TABLE` statements
#[proc_macro]
pub fn tables(tokens: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let tables = parse_macro_input!(tokens as SqlStatements);

    let mut ts = TokenStream::new();
    tables.to_token_stream(&mut ts);

    ts.into()
}

/// Lifts the `if` and `match` statements to the top, duplicating code.
/// For example, the following code:
/// ```rust
/// # use typed_qb_procmacro::lift_if; let a = 3;
/// # fn do_things() { }
/// lift_if! {
///     let x = 10;
///     do_things();
///     if a == 5 {
///         println!("5");
///     } else {
///         println!("Not 5");
///     }
/// }
/// ```
/// Is expanded to:
/// ```rust
/// # fn do_things() { }; let a = 3;
/// if a == 5 {
///     let x = 10;
///     do_things();
///     println!("5");
/// } else {
///     let x = 10;
///     do_things();
///     println!("Not 5");
/// }
/// ```
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

    let mut choices = vec![0; num];
    let block = binary_tree.codegen(&mut choices, &parsed, &conditions);

    let mut ts = TokenStream::new();
    block.to_tokens(&mut ts);
    ts.into()
}
