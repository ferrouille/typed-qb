use crate::parsing::{
    create::*,
    expr::*,
    naming::{NamingStrategy, SnakeCaseNamingStrategy},
};
use proc_macro2::{Delimiter, Group, Ident, Literal, Punct, Spacing, Span, TokenStream, TokenTree};
use syn::__private::ToTokens;

pub trait ToTokenStream {
    fn to_token_stream(&self, w: &mut TokenStream);

    fn into_token_stream(&self) -> TokenStream {
        let mut w = TokenStream::new();
        self.to_token_stream(&mut w);
        w
    }
}

impl ToTokenStream for Expr {
    fn to_token_stream(&self, w: &mut TokenStream) {
        match self {
            Expr::Binary(b) => b.to_token_stream(w),
            Expr::Value(b) => b.to_token_stream(w),
            Expr::Unary(b) => b.to_token_stream(w),
            Expr::FunctionCall(b) => b.to_token_stream(w),
            Expr::Variable(b) => b.to_token_stream(w),
            Expr::Field(b) => b.to_token_stream(w),
            Expr::Parameter(b) => b.to_token_stream(w),
            Expr::Null(b) => b.to_token_stream(w),
            Expr::Rust(b) => b.to_token_stream(w),
            Expr::Star(b) => b.to_token_stream(w),
            Expr::Distinct(b) => b.to_token_stream(w),
            Expr::ParameterExpr(b) => b.to_token_stream(w),
            Expr::Is(b) => b.to_token_stream(w),
        }
    }
}

struct TokenPath<'a> {
    elements: &'a [&'a str],
    span: Span,
    colons_needed: usize,
}

impl<'a> TokenPath<'a> {
    pub fn new(span: Span, elements: &'a [&'a str]) -> Self {
        Self {
            elements,
            span,
            colons_needed: 2,
        }
    }
}

impl Iterator for TokenPath<'_> {
    type Item = TokenTree;

    fn next(&mut self) -> Option<Self::Item> {
        if self.colons_needed > 0 {
            self.colons_needed -= 1;

            let mut p = Punct::new(
                ':',
                if self.colons_needed > 1 {
                    Spacing::Joint
                } else {
                    Spacing::Joint
                },
            );
            p.set_span(self.span);

            Some(TokenTree::Punct(p))
        } else {
            match self.elements.split_first() {
                Some((el, rest)) => {
                    self.elements = rest;
                    self.colons_needed = if rest.len() > 0 { 2 } else { 0 };

                    Some(TokenTree::Ident(Ident::new(el, self.span)))
                }
                _ => None,
            }
        }
    }
}

struct FunctionCallTokens<'a> {
    path: TokenPath<'a>,
    args: TokenStream,
    done: bool,
}

impl<'a> FunctionCallTokens<'a> {
    pub fn new(span: Span, path: &'a [&'a str]) -> Self {
        Self {
            path: TokenPath::new(span, path),
            args: TokenStream::new(),
            done: false,
        }
    }

    pub fn arg<I: IntoIterator<Item = TokenTree>>(mut self, arg: I) -> Self {
        if !self.args.is_empty() {
            self.args
                .extend([TokenTree::Punct(Punct::new(',', Spacing::Alone))]);
        }

        self.args.extend(arg);
        self
    }
}

impl<'a> Iterator for FunctionCallTokens<'a> {
    type Item = TokenTree;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            None
        } else {
            match self.path.next() {
                Some(path) => Some(path),
                None => {
                    self.done = true;
                    Some(TokenTree::Group(Group::new(
                        Delimiter::Parenthesis,
                        self.args.clone(),
                    )))
                }
            }
        }
    }
}

impl ToTokenStream for BinOp {
    fn to_token_stream(&self, w: &mut TokenStream) {
        let (name, span) = match self {
            BinOp::Or(s) => ("Or", s.span()),
            BinOp::Xor(s) => ("Xor", s.span()),
            BinOp::And(s) => ("And", s.span()),
            BinOp::CmpEq(s) => ("CmpEq", s.span()),
            BinOp::CmpEqNullSafe(s1, s2) => ("CmpEqNullSafe", s1.span().join(s2.span()).unwrap()),
            BinOp::CmpGe(s) => ("CmpGe", s.span()),
            BinOp::CmpGt(s) => ("CmpGt", s.span()),
            BinOp::CmpLe(s) => ("CmpLe", s.span()),
            BinOp::CmpLt(s) => ("CmpLt", s.span()),
            BinOp::CmpNe(s) => ("CmpNe", s.span()),
            BinOp::CmpLike(s) => ("CmpLike", s.span()),
            BinOp::BitOr(s) => ("BitOr", s.span()),
            BinOp::BitAnd(s) => ("BitAnd", s.span()),
            BinOp::Shl(s) => ("Shl", s.span()),
            BinOp::Shr(s) => ("Shr", s.span()),
            BinOp::Sub(s) => ("Sub", s.span()),
            BinOp::Add(s) => ("Add", s.span()),
            BinOp::Mul(s) => ("Mul", s.span()),
            BinOp::Div(s) => ("Div", s.span()),
            BinOp::Mod(s) => ("Mod", s.span()),
            BinOp::BitXor(s) => ("BitXor", s.span()),
        };

        w.extend(TokenPath::new(span, &["typed_qb", "expr", name]));
    }
}

impl ToTokenStream for Binary {
    fn to_token_stream(&self, w: &mut TokenStream) {
        self.op.to_token_stream(w);

        let mut ts = TokenStream::new();
        self.lhs.to_token_stream(&mut ts);
        ts.extend([TokenTree::Punct(Punct::new(',', Spacing::Alone))]);
        self.rhs.to_token_stream(&mut ts);
        w.extend([TokenTree::Group(Group::new(Delimiter::Parenthesis, ts))]);
    }
}

impl ToTokenStream for IsKind {
    fn to_token_stream(&self, w: &mut TokenStream) {
        let (name, span) = match self {
            IsKind::Null(s) => ("IsNull", s.span()),
            IsKind::NotNull(s1, s2) => ("IsNotNull", s1.span().join(s2.span()).unwrap()),
            IsKind::True(s) => ("IsTrue", s.span()),
            IsKind::False(s) => ("IsFalse", s.span()),
            IsKind::Unknown(s) => ("IsUnknown", s.span()),
        };
        w.extend(TokenPath::new(span, &["typed_qb", "expr", name]));
    }
}

impl ToTokenStream for Value {
    fn to_token_stream(&self, w: &mut TokenStream) {
        match &self.value {
            syn::Lit::Str(s) => {
                w.extend(TokenPath::new(s.span(), &["typed_qb", "expr", "ConstStr"]));
                w.extend([
                    TokenTree::Punct(Punct::new(':', Spacing::Joint)),
                    TokenTree::Punct(Punct::new(':', Spacing::Joint)),
                    TokenTree::Punct(Punct::new('<', Spacing::Alone)),
                ]);
                w.extend(s.to_token_stream());
                w.extend([TokenTree::Punct(Punct::new('>', Spacing::Alone))])
            }
            syn::Lit::ByteStr(_) => todo!(),
            syn::Lit::Byte(_) => todo!(),
            syn::Lit::Char(_) => todo!(),
            syn::Lit::Int(lit) => {
                w.extend(TokenPath::new(
                    lit.span(),
                    &["typed_qb", "expr", "ConstI64"],
                ));
                w.extend([
                    TokenTree::Punct(Punct::new(':', Spacing::Joint)),
                    TokenTree::Punct(Punct::new(':', Spacing::Joint)),
                    TokenTree::Punct(Punct::new('<', Spacing::Alone)),
                ]);
                w.extend(lit.to_token_stream());
                w.extend([TokenTree::Punct(Punct::new('>', Spacing::Alone))])
            }
            syn::Lit::Float(_) => todo!(),
            syn::Lit::Bool(_) => todo!(),
            syn::Lit::Verbatim(_) => todo!(),
        }
    }
}

impl ToTokenStream for Unary {
    fn to_token_stream(&self, w: &mut TokenStream) {
        let (name, span) = match &self.op {
            UnaryOp::Not(s) => ("Not", s.span()),
            UnaryOp::Bang(s) => ("Not", s.span()),
            UnaryOp::Negate(s) => ("Neg", s.span()),
            UnaryOp::BitNegate(s) => ("BitNeg", s.span()),
        };

        w.extend(
            FunctionCallTokens::new(span, &["typed_qb", "expr", name])
                .arg(self.expr.into_token_stream()),
        );
    }
}

impl ToTokenStream for FunctionCall {
    fn to_token_stream(&self, w: &mut TokenStream) {
        let name = self.name.to_string();
        let path = ["typed_qb", "functions", &name];
        let mut fct = FunctionCallTokens::new(self.name.span(), &path);
        for arg in self.arguments.iter() {
            let mut t = TokenStream::new();
            arg.to_token_stream(&mut t);
            fct = fct.arg(t);
        }

        w.extend(fct);
    }
}

impl ToTokenStream for Variable {
    fn to_token_stream(&self, _w: &mut TokenStream) {
        todo!()
    }
}

impl ToTokenStream for Field {
    fn to_token_stream(&self, w: &mut TokenStream) {
        match &self.base {
            Some((base, dot)) => {
                w.extend([TokenTree::Ident(base.clone())]);
                dot.to_tokens(w);
            }
            _ => {}
        }

        w.extend([TokenTree::Ident(self.name.clone())]);
    }
}

impl ToTokenStream for Parameter {
    fn to_token_stream(&self, w: &mut TokenStream) {
        w.extend(
            FunctionCallTokens::new(self.name.span(), &["typed_qb", "expr", "Parameter"])
                .arg([TokenTree::Ident(self.name.clone())]),
        );
    }
}

impl ToTokenStream for ParameterExpr {
    fn to_token_stream(&self, w: &mut TokenStream) {
        let mut inner = TokenStream::new();
        inner.extend(quote::quote!(
            // Prevents "unnecessary braces" warning from showing up
            let _ = ();
        ));
        for stmt in self.block.stmts.iter() {
            stmt.to_tokens(&mut inner);
        }

        let mut arg = TokenStream::new();
        arg.extend([TokenTree::Group(Group::new(Delimiter::Brace, inner))]);

        w.extend(
            FunctionCallTokens::new(self.block.span(), &["typed_qb", "expr", "Parameter"]).arg(arg),
        );
    }
}

impl ToTokenStream for Is {
    fn to_token_stream(&self, w: &mut TokenStream) {
        self.kind.to_token_stream(w);
        let mut e = TokenStream::new();
        self.expr.to_token_stream(&mut e);

        w.extend([TokenTree::Group(Group::new(Delimiter::Parenthesis, e))]);
    }
}

impl ToTokenStream for Null {
    fn to_token_stream(&self, w: &mut TokenStream) {
        w.extend(TokenPath::new(
            self.keyword.span(),
            &["typed_qb", "expr", "Null"],
        ));
    }
}

impl ToTokenStream for Rust {
    fn to_token_stream(&self, w: &mut TokenStream) {
        let mut inner = TokenStream::new();
        inner.extend(quote::quote!(
            // Prevents "unnecessary braces" warning from showing up
            let _ = ();
        ));
        for stmt in self.block.stmts.iter() {
            stmt.to_tokens(&mut inner);
        }

        w.extend([TokenTree::Group(Group::new(Delimiter::Brace, inner))]);
    }
}

impl ToTokenStream for Star {
    fn to_token_stream(&self, w: &mut TokenStream) {
        w.extend(TokenPath::new(
            self.token.span(),
            &["typed_qb", "expr", "Star"],
        ));
    }
}

impl ToTokenStream for Distinct {
    fn to_token_stream(&self, w: &mut TokenStream) {
        let mut arg = TokenStream::new();
        self.expr.to_token_stream(&mut arg);
        w.extend(
            FunctionCallTokens::new(self.keyword.span(), &["typed_qb", "expr", "Distinct"])
                .arg(arg),
        );
    }
}

impl ToTokenStream for SqlStatements {
    fn to_token_stream(&self, w: &mut TokenStream) {
        for statement in self.inner.iter() {
            statement.to_token_stream(w);
        }
    }
}

impl ToTokenStream for SqlQuery {
    fn to_token_stream(&self, w: &mut TokenStream) {
        match self {
            SqlQuery::CreateTable(t) => t.to_token_stream(w),
        }
    }
}

impl ToTokenStream for CreateTable {
    fn to_token_stream(&self, w: &mut TokenStream) {
        w.extend(TokenPath::new(self.create.span(), &["typed_qb", "table"]));

        let mut table_args = TokenStream::new();
        let mut fields = TokenStream::new();

        for line in self.lines.iter() {
            match line {
                CreateTableLine::Column(column) => {
                    column.to_token_stream(&mut fields);
                    fields.extend([TokenTree::Punct(Punct::new(',', Spacing::Alone))]);
                }
                CreateTableLine::NotImplemented(_) => {}
            }
        }

        table_args.extend([
            TokenTree::Ident(self.name.clone().into()),
            TokenTree::Literal(Literal::string(&self.name.to_string())),
            TokenTree::Group(Group::new(Delimiter::Brace, fields)),
        ]);

        w.extend([
            TokenTree::Punct(Punct::new('!', Spacing::Joint)),
            TokenTree::Group(Group::new(Delimiter::Parenthesis, table_args)),
            TokenTree::Punct(Punct::new(';', Spacing::Alone)),
        ]);
    }
}

impl ToTokenStream for Column {
    fn to_token_stream(&self, w: &mut TokenStream) {
        w.extend([
            TokenTree::Ident(Ident::new(
                &SnakeCaseNamingStrategy.translate(&self.name.to_string()),
                self.name.span(),
            )),
            TokenTree::Literal(Literal::string(&self.name.to_string())),
            TokenTree::Punct(Punct::new(':', Spacing::Alone)),
        ]);
        w.extend(TokenPath::new(
            self.name.span(),
            &["typed_qb", "typing", "SimpleTy"],
        ));
        w.extend([TokenTree::Punct(Punct::new('<', Spacing::Alone))]);

        self.ty.to_token_stream(w);

        w.extend([TokenTree::Punct(Punct::new(',', Spacing::Alone))]);
        w.extend(TokenPath::new(
            self.nullable
                .as_ref()
                .map(|n| n.span())
                .unwrap_or(Span::call_site()),
            &[
                "typed_qb",
                "typing",
                if let Some(SqlNullable::NotNull(_, _)) = self.nullable {
                    "NonNullable"
                } else {
                    "Nullable"
                },
            ],
        ));
        w.extend([TokenTree::Punct(Punct::new('>', Spacing::Alone))])
    }
}

impl ToTokenStream for SqlType {
    fn to_token_stream(&self, w: &mut TokenStream) {
        match self {
            SqlType::Int(t) => t.to_token_stream(w),
            SqlType::Float(t) => t.to_token_stream(w),
            SqlType::Double(t) => t.to_token_stream(w),
            SqlType::VarChar(t) => t.to_token_stream(w),
            SqlType::Bit(t) => t.to_token_stream(w),
            SqlType::Text(t) => t.to_token_stream(w),
            SqlType::Time(t) => t.to_token_stream(w),
            SqlType::DateTime(t) => t.to_token_stream(w),
        }
    }
}

impl ToTokenStream for Int {
    fn to_token_stream(&self, w: &mut TokenStream) {
        self.size.to_token_stream(w);

        w.extend([TokenTree::Punct(Punct::new('<', Spacing::Alone))]);
        if let Some(ident) = &self.unsigned {
            w.extend(TokenPath::new(
                ident.span(),
                &["typed_qb", "typing", "Unsigned"],
            ));
        } else {
            w.extend(TokenPath::new(
                self.size.span(),
                &["typed_qb", "typing", "Signed"],
            ));
        }
        w.extend([TokenTree::Punct(Punct::new('>', Spacing::Alone))])
    }
}

impl ToTokenStream for Float {
    fn to_token_stream(&self, w: &mut TokenStream) {
        w.extend(TokenPath::new(
            self.name.span(),
            &["typed_qb", "typing", "F32"],
        ));
    }
}

impl ToTokenStream for Double {
    fn to_token_stream(&self, w: &mut TokenStream) {
        w.extend(TokenPath::new(
            self.name.span(),
            &["typed_qb", "typing", "F64"],
        ));
    }
}

impl ToTokenStream for VarChar {
    fn to_token_stream(&self, w: &mut TokenStream) {
        w.extend(TokenPath::new(
            self.name.span(),
            &["typed_qb", "typing", "Text"],
        ));
    }
}

impl ToTokenStream for Time {
    fn to_token_stream(&self, w: &mut TokenStream) {
        w.extend(TokenPath::new(
            self.name.span(),
            &["typed_qb", "typing", "Time"],
        ));
    }
}

impl ToTokenStream for DateTime {
    fn to_token_stream(&self, w: &mut TokenStream) {
        w.extend(TokenPath::new(
            self.name.span(),
            &["typed_qb", "typing", "DateTime"],
        ));
    }
}

impl ToTokenStream for Bit {
    fn to_token_stream(&self, w: &mut TokenStream) {
        if self.display.value().unwrap_or(1) == 1 {
            w.extend(TokenPath::new(
                self.name.span(),
                &["typed_qb", "typing", "Bool"],
            ));
        } else {
            w.extend(TokenPath::new(
                self.name.span(),
                &["typed_qb", "typing", "BigInt"],
            ));
            w.extend([TokenTree::Punct(Punct::new('<', Spacing::Alone))]);
            w.extend(TokenPath::new(
                self.name.span(),
                &["typed_qb", "typing", "Unsigned"],
            ));
            w.extend([TokenTree::Punct(Punct::new('>', Spacing::Alone))])
        }
    }
}

impl ToTokenStream for Text {
    fn to_token_stream(&self, w: &mut TokenStream) {
        w.extend(TokenPath::new(
            self.name.span(),
            &["typed_qb", "typing", "Text"],
        ));
    }
}

impl ToTokenStream for IntSize {
    fn to_token_stream(&self, w: &mut TokenStream) {
        let (name, span) = match self {
            IntSize::Tiny(s) => ("TinyInt", s.span()),
            IntSize::Small(s) => ("SmallInt", s.span()),
            IntSize::Medium(s) => ("MediumInt", s.span()),
            IntSize::Normal(s) => ("Int", s.span()),
            IntSize::Big(s) => ("BigInt", s.span()),
        };
        w.extend(TokenPath::new(span, &["typed_qb", "typing", name]));
    }
}
