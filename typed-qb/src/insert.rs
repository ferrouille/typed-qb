//! `INSERT` queries
//! 
//! **Note:** There are currently no checks against duplicate fields or missing fields without default value.
//! 
//! **Note:** There are currently no checks against using a field from the table in an expression.
//! That is, `INSERT INTO table (field) VALUES (table.field)` will not cause a compile error.
//! 
//! ```rust
//! # #![feature(generic_associated_types)] 
//! # use typed_qb::__doctest::*; 
//! # let mut conn = FakeConn;
//! let name = "root";
//! let results = conn.typed_exec(Users::insert(|user| values! {
//!     user.id => [1],
//!     user.name => [:name],
//! }))?;
//! # Ok::<(), mysql::Error>(())
//! ```

use crate::expr::Value;
use crate::typing::Ty;
use crate::{
    sql_concat, ConstSqlStr, Field, FieldName, QueryRoot, QueryTree, Table, TableAlias, ToSql, Up,
    UpEnd,
};
use std::marker::PhantomData;

#[macro_export]
macro_rules! values {
    ($key:expr => [$($value:tt)*] $(,)*) => {
        $crate::insert::InsertValue {
            field: $key,
            value: $crate::expr!($($value)*),
        }
    };
    ($key:expr => $value:expr $(,)*) => {
        $crate::insert::InsertValue {
            field: $key,
            value: $value,
        }
    };
    ($key:expr => [$($value:tt)*], $($rest:tt)+) => {
        $crate::insert::ValueListCons {
            head: $crate::values!($key => [$($value)*]),
            tail: $crate::values!($($rest)+),
        }
    };
    ($key:expr => $value:expr, $($rest:tt)+) => {
        $crate::insert::ValueListCons {
            head: $crate::values!($key => $value),
            tail: $crate::values!($($rest)+),
        }
    };
    ($(,)*) => {};
}

// TODO: S cannot contain references to fields of this table.
pub struct Insert<T: Table, S: ValueList> {
    pub(crate) values: S,
    pub(crate) _phantom: PhantomData<T>,
}

impl<T: Table + QueryTree<UpEnd>, S: ValueList + QueryTree<T::MaxUp>> QueryTree<UpEnd>
    for Insert<T, S>
{
    type MaxUp = S::MaxUp;
}

// TODO: Ensure that all columns without default value are specified
impl<T: Table + ToSql, S: ValueList + ToSql> ToSql for Insert<T, S> {
    const SQL: ConstSqlStr = sql_concat!(
        "INSERT INTO ",
        T,
        " (",
        (S::NAMES.as_str()),
        ") VALUES (",
        S,
        ")"
    );

    fn collect_parameters(&self, f: &mut Vec<crate::QueryValue>) {
        self.values.collect_parameters(f);
    }
}

pub trait ValueList {
    const NAMES: ConstSqlStr;
}

pub struct ValueListCons<H: IsValue, T: ValueList> {
    pub head: H,
    pub tail: T,
}

// TODO: Typecheck values
pub struct InsertValue<T: Ty, A: TableAlias, N: FieldName, V: Value> {
    pub field: Field<T, A, N>,
    pub value: V,
}

pub trait IsValue {
    const NAMES: ConstSqlStr;
}

impl<T: Ty, A: TableAlias, N: FieldName, V: Value> ValueList for InsertValue<T, A, N, V> {
    const NAMES: ConstSqlStr = sql_concat!([Field::<T, A, N>]);
}

impl<T: Ty, A: TableAlias, N: FieldName, V: Value> IsValue for InsertValue<T, A, N, V> {
    const NAMES: ConstSqlStr = sql_concat!([Field::<T, A, N>]);
}
impl<U: Up, T: Ty, A: TableAlias, N: FieldName, V: Value + QueryTree<U>> QueryTree<U>
    for InsertValue<T, A, N, V>
{
    type MaxUp = V::MaxUp;
}

impl<H: IsValue + ToSql, T: ValueList + ToSql> ValueList for ValueListCons<H, T> {
    const NAMES: ConstSqlStr = sql_concat!((H::NAMES.as_str()), ", ", (T::NAMES.as_str()));
}

impl<U: Up, H: IsValue + QueryTree<U>, T: ValueList + QueryTree<H::MaxUp>> QueryTree<U>
    for ValueListCons<H, T>
{
    type MaxUp = T::MaxUp;
}

impl<T: Ty, A: TableAlias, N: FieldName, V: Value + ToSql> ToSql for InsertValue<T, A, N, V>
where
    Field<T, A, N>: ToSql,
{
    const SQL: ConstSqlStr = sql_concat!(V);

    fn collect_parameters(&self, f: &mut Vec<crate::QueryValue>) {
        self.field.collect_parameters(f);
        self.value.collect_parameters(f);
    }
}

impl<H: IsValue + ToSql, T: ValueList + ToSql> ToSql for ValueListCons<H, T> {
    const SQL: ConstSqlStr = sql_concat!(H, ", ", T);

    fn collect_parameters(&self, f: &mut Vec<crate::QueryValue>) {
        self.head.collect_parameters(f);
        self.tail.collect_parameters(f);
    }
}

impl<T: Table, S: ValueList> QueryRoot for Insert<T, S> where Self: QueryTree<UpEnd> + ToSql {}

#[cfg(test)]
mod tests {
    use crate::{
        expr::{ConstI64, ConstStr},
        prelude::*,
        typing::*,
        QueryRoot, ToSql,
    };

    crate::table!(
        Foo "Foo" {
            id "Id": SimpleTy<BigInt<Signed>, NonNullable>,
            name "Name": SimpleTy<Text, NonNullable>,
            value "Value": SimpleTy<Int<Unsigned>, NonNullable>,
        }
    );

    fn ground<T: QueryRoot + ToSql>(t: T) -> T {
        t
    }

    #[test]
    pub fn test_insert() {
        assert_eq!(
            ground(Foo::insert(|t| values! {
                t.name => ConstStr::<"Test">,
                t.value => ConstI64::<5>,
            }))
            .sql_str(),
            "INSERT INTO `Foo` (`Name`, `Value`) VALUES ('Test', 5)"
        );
    }
}
