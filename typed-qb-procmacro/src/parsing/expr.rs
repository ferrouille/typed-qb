use crate::parsing::parse_keyword;
use proc_macro2::{Ident, Span, TokenStream};
use std::fmt;
use syn::{
    ext::IdentExt,
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
    Block, Lit, Token,
};

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Either<A, B> {
    Left(A),
    Right(B),
}

pub trait GetSpan {
    fn span(&self) -> Span;
}

impl<T: quote::ToTokens> GetSpan for T {
    fn span(&self) -> Span {
        let mut w = TokenStream::new();
        self.to_tokens(&mut w);
        w.into_iter()
            .map(|t| t.span())
            .reduce(|acc, item| acc.join(item).unwrap())
            .unwrap()
    }
}

impl<A: GetSpan, B: GetSpan> Either<A, B> {
    pub fn span(&self) -> Span {
        match self {
            Either::Left(left) => left.span(),
            Either::Right(right) => right.span(),
        }
    }
}

#[derive(Clone)]
pub enum CmpOp {
    Eq(Token!(=)),
    EqNullSafe(Token!(<=), Token!(>)),
    Ge(Token!(>=)),
    Gt(Token!(>)),
    Le(Token!(<=)),
    Lt(Token!(<)),
    Ne(Token!(!=)),
    Like(Ident),
}

#[derive(Clone)]
pub enum BinOp {
    Or(Either<Token!(||), Ident>),
    Xor(Ident),
    And(Either<Token!(&&), Ident>),
    BitOr(Token!(|)),
    BitAnd(Token!(&)),
    Shl(Token!(<<)),
    Shr(Token!(>>)),
    Sub(Token!(-)),
    Add(Token!(+)),
    Mul(Token!(*)),
    Div(Token!(/)),
    Mod(Token!(%)),
    BitXor(Token!(^)),
    Cmp(CmpOp),
    CmpAny(CmpOp, Ident),
    CmpAll(CmpOp, Ident),
}

impl fmt::Debug for CmpOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            CmpOp::Eq(_) => "=",
            CmpOp::EqNullSafe(_, _) => "<=>",
            CmpOp::Ge(_) => ">=",
            CmpOp::Gt(_) => ">",
            CmpOp::Le(_) => "<=",
            CmpOp::Lt(_) => "<",
            CmpOp::Ne(_) => "!=",
            CmpOp::Like(_) => "LIKE",
        })
    }
}

impl fmt::Debug for BinOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            BinOp::Or(_) => "OR",
            BinOp::Xor(_) => "XOR",
            BinOp::And(_) => "AND",
            BinOp::BitOr(_) => "|",
            BinOp::BitAnd(_) => "&",
            BinOp::Shl(_) => "<<",
            BinOp::Shr(_) => ">>",
            BinOp::Sub(_) => "-",
            BinOp::Add(_) => "+",
            BinOp::Mul(_) => "*",
            BinOp::Div(_) => "/",
            BinOp::Mod(_) => "%",
            BinOp::BitXor(_) => "^",
            BinOp::Cmp(c) => return write!(f, "{:?}", c),
            BinOp::CmpAny(c, _) => return write!(f, "{:?} ANY", c),
            BinOp::CmpAll(c, _) => return write!(f, "{:?} ALL", c),
        })
    }
}

#[derive(Clone)]
pub enum UnaryOp {
    Not(Ident),
    Bang(Token!(!)),
    Negate(Token!(-)),
    BitNegate(Token!(~)),
}

impl fmt::Debug for UnaryOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Not(..) => write!(f, "Not"),
            Self::Bang(..) => write!(f, "Bang"),
            Self::Negate(..) => write!(f, "Negate"),
            Self::BitNegate(..) => write!(f, "BitNegate"),
        }
    }
}

#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub enum Precedence {
    Any,
    Or,
    Xor,
    And,
    Not,
    // TODO: BetweenCase,
    Comparison,
    BitOr,
    BitAnd,
    BitShift,
    AddSub,
    MulDiv,
    BitXor,
    Negate,
    Bang,
    // TODO: Interval,
}

impl BinOp {
    pub fn precedence(&self) -> Precedence {
        match self {
            BinOp::Or(..) => Precedence::Or,
            BinOp::Xor(..) => Precedence::Xor,
            BinOp::And(..) => Precedence::And,
            BinOp::Cmp(..) | BinOp::CmpAny(..) | BinOp::CmpAll(..) => Precedence::Comparison,
            BinOp::BitOr(..) => Precedence::BitOr,
            BinOp::BitAnd(..) => Precedence::BitAnd,
            BinOp::Shl(..) | BinOp::Shr(..) => Precedence::BitShift,
            BinOp::Sub(..) | BinOp::Add(..) => Precedence::AddSub,
            BinOp::Mul(..) | BinOp::Div(..) => Precedence::MulDiv,
            BinOp::Mod(..) => todo!(),
            BinOp::BitXor(..) => Precedence::BitXor,
        }
    }
}

#[derive(Debug, Clone)]
pub enum IsKind {
    Null(Ident),
    NotNull(Ident, Ident),
    True(Ident),
    False(Ident),
    Unknown(Ident),
}

impl fmt::Display for IsKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                IsKind::Null(_) => "NULL",
                IsKind::NotNull(_, _) => "NOT NULL",
                IsKind::True(_) => "TRUE",
                IsKind::False(_) => "FALSE",
                IsKind::Unknown(_) => "UNKNOWN",
            }
        )
    }
}

crate::parsing::gen_wrapper! {
    #[allow(unused)]
    #[derive(Clone, Debug)]
    pub Expr {
        Binary {
            pub(crate) lhs: Box<Expr>,
            pub(crate) op: BinOp,
            pub(crate) rhs: Box<Expr>,
        },
        Value {
            pub(crate) value: Lit,
        },
        Unary {
            pub(crate) op: UnaryOp,
            pub(crate) expr: Box<Expr>,
        },
        FunctionCall {
            pub(crate) name: Ident,
            pub(crate) arguments: Punctuated<Box<Expr>, Token![,]>,
        },
        Variable {
            pub(crate) name: Ident,
            pub(crate) assignment: Option<Box<Expr>>,
        },
        Field {
            pub(crate) base: Option<(Ident, Token![.])>,
            pub(crate) name: Ident,
        },
        Parameter {
            pub(crate) token: Token![:],
            pub(crate) name: Ident,
        },
        ParameterExpr {
            pub(crate) token: Token![:],
            pub(crate) block: Block,
        },
        Null {
            pub(crate) keyword: Ident,
        },
        Rust {
            pub(crate) block: Block,
        },
        Star {
            pub(crate) token: Token![*],
        },
        Distinct {
            pub(crate) keyword: Ident,
            pub(crate) expr: Box<Expr>,
        },
        Is {
            pub(crate) is: Ident,
            pub(crate) kind: IsKind,
            pub(crate) expr: Box<Expr>,
        },
        // TODO: ANY (subquery); ALL (subquery)
        // TODO: IN (list); IN (subquery)
        // TODO: EXISTS
        // TODO: CASE .. WHEN ..
    }
}

impl Parse for CmpOp {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        if input.peek(Token!(=)) {
            input.parse().map(CmpOp::Eq)
        } else if input.peek(Token!(<=)) && input.peek2(Token!(>)) {
            let a = input.parse()?;
            let b = input.parse()?;
            Ok(CmpOp::EqNullSafe(a, b))
        } else if input.peek(Token!(>=)) {
            input.parse().map(CmpOp::Ge)
        } else if input.peek(Token!(>)) {
            input.parse().map(CmpOp::Gt)
        } else if input.peek(Token!(<=)) {
            input.parse().map(CmpOp::Le)
        } else if input.peek(Token!(<)) {
            input.parse().map(CmpOp::Lt)
        } else if input.peek(Token!(!=)) {
            input.parse().map(CmpOp::Ne)
        } else if let Ok(ident) = parse_keyword("LIKE", &input) {
            Ok(CmpOp::Like(ident))
        } else {
            Err(input.error("expected a comparison operator"))
        }
    }
}

impl Parse for BinOp {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        if input.peek(Token!(||)) {
            input.parse().map(|t| BinOp::Or(Either::Left(t)))
        } else if let Ok(ident) = parse_keyword("OR", &input) {
            Ok(BinOp::Or(Either::Right(ident)))
        } else if let Ok(ident) = parse_keyword("XOR", &input) {
            Ok(BinOp::Xor(ident))
        } else if input.peek(Token!(&&)) {
            input.parse().map(|t| BinOp::And(Either::Left(t)))
        } else if let Ok(ident) = parse_keyword("AND", &input) {
            Ok(BinOp::And(Either::Right(ident)))
        } else if input.peek(Token!(|)) {
            input.parse().map(BinOp::BitOr)
        } else if input.peek(Token!(&)) {
            input.parse().map(BinOp::BitAnd)
        } else if input.peek(Token!(<<)) {
            input.parse().map(BinOp::Shl)
        } else if input.peek(Token!(>>)) {
            input.parse().map(BinOp::Shr)
        } else if input.peek(Token!(-)) {
            input.parse().map(BinOp::Sub)
        } else if input.peek(Token!(+)) {
            input.parse().map(BinOp::Add)
        } else if input.peek(Token!(*)) {
            input.parse().map(BinOp::Mul)
        } else if input.peek(Token!(/)) {
            input.parse().map(BinOp::Div)
        } else if input.peek(Token!(%)) {
            input.parse().map(BinOp::Mod)
        } else if input.peek(Token!(^)) {
            input.parse().map(BinOp::BitXor)
        } else if let Ok(op) = input.parse() {
            Ok(if let Ok(ident) = parse_keyword("ANY", &input) {
                BinOp::CmpAny(op, ident)
            } else if let Ok(ident) = parse_keyword("ALL", &input) {
                BinOp::CmpAll(op, ident)
            } else {
                BinOp::Cmp(op)
            })
        } else {
            Err(input.error("expected an operator"))
        }
    }
}

impl Parse for Expr {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        parse_at_precedence(input, Precedence::Any)
    }
}

fn parse_at_precedence(input: ParseStream, base: Precedence) -> syn::Result<Expr> {
    let lhs = parse_unary_expr(input)?;
    parse_expr(input, lhs, base)
}

fn parse_expr(input: ParseStream, mut lhs: Expr, base: Precedence) -> syn::Result<Expr> {
    loop {
        if input
            .fork()
            .parse::<BinOp>()
            .ok()
            .map_or(false, |op| op.precedence() >= base)
        {
            let op = input.parse::<BinOp>()?;
            let precedence = op.precedence();
            let mut rhs = parse_unary_expr(input)?;
            loop {
                let next = peek_precedence(input);
                if next > precedence {
                    rhs = parse_expr(input, rhs, next)?;
                } else {
                    break;
                }
            }

            lhs = Expr::Binary(Binary {
                lhs: Box::new(lhs),
                op,
                rhs: Box::new(rhs),
            })
        } else {
            break;
        }
    }

    Ok(lhs)
}

fn peek_precedence(input: ParseStream) -> Precedence {
    if let Ok(op) = input.fork().parse::<BinOp>() {
        op.precedence()
    } else {
        Precedence::Any
    }
}

fn parse_unary_expr(input: ParseStream) -> syn::Result<Expr> {
    let expr = if input.peek(Lit) {
        input.parse().map(Expr::Value)
    } else if input.peek(Token![!]) {
        let expr = parse_at_precedence(input, Precedence::Bang)?;
        Ok(Expr::Unary(Unary {
            op: UnaryOp::Bang(input.parse()?),
            expr: Box::new(expr),
        }))
    } else if input.peek(Token![ ~ ]) {
        let op = UnaryOp::BitNegate(input.parse()?);
        let expr = parse_at_precedence(input, Precedence::Negate)?;
        Ok(Expr::Unary(Unary {
            op,
            expr: Box::new(expr),
        }))
    } else if input.peek(Token![ : ]) {
        let token = input.parse()?;
        if let Ok(block) = input.parse() {
            Ok(Expr::ParameterExpr(ParameterExpr { token, block }))
        } else {
            let name = input.parse()?;
            Ok(Expr::Parameter(Parameter { token, name }))
        }
    } else if input.peek(Token![ - ]) {
        let expr = parse_at_precedence(input, Precedence::Negate)?;
        Ok(Expr::Unary(Unary {
            op: UnaryOp::Negate(input.parse()?),
            expr: Box::new(expr),
        }))
    } else if let Ok(ident) = parse_keyword("NOT", &input) {
        let expr = parse_at_precedence(input, Precedence::Not)?;
        Ok(Expr::Unary(Unary {
            op: UnaryOp::Not(ident),
            expr: Box::new(expr),
        }))
    } else if let Ok(keyword) = parse_keyword("NULL", &input) {
        Ok(Expr::Null(Null { keyword }))
    } else if input.peek(Token![ * ]) {
        let token = input.parse()?;
        Ok(Expr::Star(Star { token }))
    } else if let Ok(ident) = parse_keyword("DISTINCT", &input) {
        let expr = parse_at_precedence(input, Precedence::Any)?;
        Ok(Expr::Distinct(Distinct {
            keyword: ident,
            expr: Box::new(expr),
        }))
    } else if let Ok(expr) = super::parse_parens(input) {
        let buf = expr;
        let expr = Expr::parse(&buf)?;
        if !buf.is_empty() {
            return Err(buf.error("Found more tokens than expected"));
        }

        Ok(expr)
    } else if input.peek(Ident::peek_any) {
        let name = input.parse()?;

        if input.peek(Token![.]) {
            let dot = input.parse()?;
            let base = name;
            let name = input.parse()?;

            Ok(Expr::Field(Field {
                base: Some((base, dot)),
                name,
            }))
        } else if let Ok(args) = super::parse_parens(input) {
            let args = Punctuated::parse_terminated(&args)?;

            Ok(Expr::FunctionCall(FunctionCall {
                name,
                arguments: args,
            }))
        } else {
            Ok(Expr::Field(Field { base: None, name }))
        }
    } else if let Ok(block) = input.parse() {
        Ok(Expr::Rust(Rust { block }))
    } else {
        Err(input.error("expected a unary expression"))
    }?;

    if let Ok(ident) = parse_keyword("IS", &input) {
        Ok(Expr::Is(Is {
            is: ident,
            kind: if let Ok(ident) = parse_keyword("NULL", &input) {
                IsKind::Null(ident)
            } else if let Ok(ident) = parse_keyword("NOT", &input) {
                let ident2 = parse_keyword("NULL", &input)?;
                IsKind::NotNull(ident, ident2)
            } else if let Ok(ident) = parse_keyword("TRUE", &input) {
                IsKind::True(ident)
            } else if let Ok(ident) = parse_keyword("FALSE", &input) {
                IsKind::False(ident)
            } else if let Ok(ident) = parse_keyword("UNKNOWN", &input) {
                IsKind::Unknown(ident)
            } else {
                return Err(input.error("expected NULL, NOT NULL, TRUE, FALSE, UNKNOWN"));
            },
            expr: Box::new(expr),
        }))
    } else {
        Ok(expr)
    }
}

impl Parse for Value {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        Ok(Value {
            value: input.parse()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_parsing() {
        let parsed: Expr = syn::parse_str("5 + 5 * 2").unwrap();
        println!("{:?}", parsed);
        if let Expr::Binary(Binary {
            lhs,
            op: BinOp::Add(_),
            rhs,
        }) = parsed
        {
            match (lhs.as_ref(), rhs.as_ref()) {
                (
                    Expr::Value(Value {
                        value: Lit::Int(v1),
                    }),
                    Expr::Binary(Binary {
                        lhs,
                        op: BinOp::Mul(_),
                        rhs,
                    }),
                ) => match (lhs.as_ref(), rhs.as_ref()) {
                    (
                        Expr::Value(Value {
                            value: Lit::Int(v2),
                        }),
                        Expr::Value(Value {
                            value: Lit::Int(v3),
                        }),
                    ) => {
                        assert_eq!(v1.base10_parse::<u64>().unwrap(), 5);
                        assert_eq!(v2.base10_parse::<u64>().unwrap(), 5);
                        assert_eq!(v3.base10_parse::<u64>().unwrap(), 2);
                    }
                    _ => panic!(),
                },
                _ => panic!(),
            }
        } else {
            panic!()
        }
    }
}
