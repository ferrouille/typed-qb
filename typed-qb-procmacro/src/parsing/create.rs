use super::{parse_keyword, Backtick};
use crate::parsing::{expr::GetSpan, parse_any};
use syn::{parse::Parse, punctuated::Punctuated, Ident, LitInt, Token};

#[derive(Clone, Debug)]
pub struct Name {
    name: Ident,
}

impl From<Name> for Ident {
    fn from(n: Name) -> Self {
        n.name
    }
}

impl From<&Name> for Ident {
    fn from(n: &Name) -> Self {
        n.name.clone()
    }
}

impl ToString for Name {
    fn to_string(&self) -> String {
        self.name.to_string()
    }
}

#[derive(Clone, Debug)]
pub struct SqlStatements {
    pub inner: Punctuated<SqlQuery, Token![;]>,
}

impl Parse for SqlStatements {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let lines = Punctuated::parse_terminated(&input)?;
        Ok(SqlStatements { inner: lines })
    }
}

#[derive(Clone, Debug)]
pub enum SqlNullable {
    Null(Ident),
    NotNull(Ident, Ident),
}

impl GetSpan for SqlNullable {
    fn span(&self) -> proc_macro2::Span {
        match self {
            SqlNullable::Null(s) => s.span(),
            SqlNullable::NotNull(s1, s2) => s1.span().join(s2.span()).unwrap(),
        }
    }
}

crate::parsing::gen_wrapper! {
    #[derive(Clone, Debug)]
    pub SqlQuery {
        CreateTable {
            pub create: Ident,
            pub table: Ident,
            pub name: Name,
            pub if_not_exists: Option<(Ident, Ident, Ident)>,
            pub lines: Punctuated<CreateTableLine, Token![,]>,
        },
    }
}

crate::parsing::gen_wrapper! {
    #[derive(Clone, Debug)]
    pub CreateTableLine {
        Column {
            pub name: Ident,
            pub ty: SqlType,
            pub nullable: Option<SqlNullable>,
            pub default: Option<(Ident, crate::parsing::expr::Expr)>,
        },
        NotImplemented {},
    }
}

#[derive(Clone, Debug)]
pub enum IntSize {
    Tiny(Ident),
    Small(Ident),
    Medium(Ident),
    Normal(Ident),
    Big(Ident),
}

impl GetSpan for IntSize {
    fn span(&self) -> proc_macro2::Span {
        match self {
            IntSize::Tiny(s) => s.span(),
            IntSize::Small(s) => s.span(),
            IntSize::Medium(s) => s.span(),
            IntSize::Normal(s) => s.span(),
            IntSize::Big(s) => s.span(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum DisplayWidth {
    Value { val: LitInt },
    None,
}

impl DisplayWidth {
    pub fn value(&self) -> Option<usize> {
        match self {
            DisplayWidth::Value { val } => Some(val.base10_parse().unwrap()),
            DisplayWidth::None => None,
        }
    }
}

#[derive(Clone, Debug)]
pub enum Fsp {
    Value { val: LitInt },
    None,
}

crate::parsing::gen_wrapper! {
    #[derive(Clone, Debug)]
    pub SqlType {
        Int {
            pub size: IntSize,
            pub display: DisplayWidth,
            pub unsigned: Option<Ident>,
            pub zerofill: Option<Ident>,
        },
        Float {
            pub name: Ident,
            pub display: DisplayWidth,
        },
        Double {
            pub name: Ident,
            pub display: DisplayWidth,
        },
        VarChar {
            pub name: Ident,
            pub display: DisplayWidth,
        },
        Bit {
            pub name: Ident,
            pub display: DisplayWidth,
        },
        Text {
            pub name: Ident,
        },
        DateTime {
            pub name: Ident,
            pub fsp: Fsp,
        },
        Time {
            pub name: Ident,
            pub fsp: Fsp,
        },
    }
}

impl Parse for Name {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let name = if input.peek(Backtick) {
            input.parse::<Backtick>()?;
            let name = input.parse()?;
            input.parse::<Backtick>()?;
            name
        } else {
            input.parse()?
        };

        Ok(Name { name })
    }
}

impl Parse for SqlQuery {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let create = parse_keyword("CREATE", &input)?;
        let table = parse_keyword("TABLE", &input)?;
        let name = input.parse()?;
        let if_not_exists = if let Ok(i) = parse_keyword("IF", &input) {
            let n = parse_keyword("NOT", &input)?;
            let e = parse_keyword("EXISTS", &input)?;

            Some((i, n, e))
        } else {
            None
        };
        let lines;
        syn::parenthesized!(lines in input);
        let lines = Punctuated::parse_terminated(&lines)?;

        while !input.peek(Token![;]) && !input.is_empty() {
            parse_any(&input)?;
        }

        Ok(SqlQuery::CreateTable(CreateTable {
            create,
            table,
            name,
            if_not_exists,
            lines,
        }))
    }
}

impl Parse for CreateTableLine {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        Ok(
            if parse_keyword("PRIMARY", &input).is_ok()
                || parse_keyword("CONSTRAINT", &input).is_ok()
                || parse_keyword("INDEX", &input).is_ok()
                || parse_keyword("KEY", &input).is_ok()
                || parse_keyword("UNIQUE", &input).is_ok()
            {
                while !input.peek(Token![,]) && !input.is_empty() {
                    parse_any(&input)?;
                }

                CreateTableLine::NotImplemented(NotImplemented {})
            } else {
                CreateTableLine::Column(input.parse()?)
            },
        )
    }
}

impl Parse for Column {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let name = input.parse()?;

        let ty = input.parse()?;

        let nullable = if let Ok(not) = parse_keyword("NOT", &input) {
            let null = parse_keyword("NULL", &input)?;
            Some(SqlNullable::NotNull(not, null))
        } else if let Ok(null) = parse_keyword("NULL", &input) {
            Some(SqlNullable::Null(null))
        } else {
            None
        };

        let default = if let Ok(default) = parse_keyword("DEFAULT", &input) {
            Some((default, input.parse()?))
        } else {
            None
        };

        let _auto_increment = parse_keyword("AUTO_INCREMENT", &input).ok();

        // TODO: auto_increment
        // TODO: UNIQUE [KEY] | [PRIMARY] KEY | COMMENT | COLLATE | COLUMN_FORMAT | STORAGE | reference_definition

        Ok(Column {
            name,
            ty,
            nullable,
            default,
        })
    }
}

impl Parse for DisplayWidth {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        Ok(if let Ok(inner) = super::parse_parens(input) {
            let val = inner.parse()?;
            DisplayWidth::Value { val }
        } else {
            DisplayWidth::None
        })
    }
}

impl Parse for Fsp {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        Ok(if let Ok(inner) = super::parse_parens(input) {
            let val = inner.parse()?;
            Fsp::Value { val }
        } else {
            Fsp::None
        })
    }
}

impl Parse for SqlType {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let name = input.parse::<Ident>()?;
        let normalized = name.to_string().to_uppercase();
        Ok(match normalized.as_str() {
            "TINYINT" | "SMALLINT" | "MEDIUMINT" | "INT" | "INTEGER" | "BIGINT" => {
                let display = input.parse()?;
                let unsigned = parse_keyword("UNSIGNED", &input).ok();
                let zerofill = parse_keyword("ZEROFILL", &input).ok();

                SqlType::Int(Int {
                    size: match normalized.as_str() {
                        "TINYINT" => IntSize::Tiny,
                        "SMALLINT" => IntSize::Small,
                        "MEDIUMINT" => IntSize::Medium,
                        "INT" | "INTEGER" => IntSize::Normal,
                        "BIGINT" => IntSize::Big,
                        _ => unreachable!(),
                    }(name),
                    display,
                    unsigned,
                    zerofill,
                })
            }
            "FLOAT" => {
                let display = input.parse()?;
                SqlType::Float(Float { name, display })
            }
            "DOUBLE" => {
                let display = input.parse()?;
                SqlType::Double(Double { name, display })
            }
            "VARCHAR" => {
                let display = input.parse()?;
                SqlType::VarChar(VarChar { name, display })
            }
            "BIT" => {
                let display = input.parse()?;
                SqlType::Bit(Bit { name, display })
            }
            "TEXT" | "LONGTEXT" => SqlType::Text(Text { name }),
            "DATETIME" => {
                let fsp = input.parse()?;
                SqlType::DateTime(DateTime { name, fsp })
            }
            "TIME" => {
                let fsp = input.parse()?;
                SqlType::Time(Time { name, fsp })
            }
            _ => return Err(input.error(format!("Unknown data type {}", name))),
        })
    }
}
