//! `UPDATE` queries
//!
//! ```rust
//! # #![feature(generic_associated_types)]
//! # use typed_qb::__doctest::*;
//! # let mut conn = FakeConn;
//! let name = "root";
//! let results = conn.typed_exec(Users::update(|user| set! {
//!     user.name => [:name],
//! }, |user| expr!(user.id = 42).limit::<1>()))?;
//! # Ok::<(), mysql::Error>(())
//! ```

use crate::expr::Value;
use crate::qualifiers::{
    AnyHaving, AnyOrderedBy, AnyWhere, Limit, LimitValue, OrderBy, OrderBySeq,
};
use crate::typing::Ty;
use crate::{
    sql_concat, ConstSqlStr, Field, FieldName, QueryRoot, QueryTree, Table, TableAlias, ToSql, Up,
    UpEnd,
};
use std::marker::PhantomData;

#[macro_export]
macro_rules! set {
    ($key:expr => [$($value:tt)*] $(,)*) => {
        $crate::update::Set {
            field: $key,
            value: $crate::expr!($($value)*),
        }
    };
    ($key:expr => $value:expr $(,)*) => {
        $crate::update::Set {
            field: $key,
            value: $value,
        }
    };
    ($key:expr => $value:expr, $($rest:tt)+) => {
        $crate::update::SetListCons {
            head: $crate::set!($key => $value),
            tail: $crate::set!($($rest)*),
        }
    }
}

pub trait UpdateQualifiers {}

impl<W: AnyWhere> UpdateQualifiers for W {}
impl<I: AnyHaving + UpdateQualifiers, S: OrderBySeq> UpdateQualifiers for OrderBy<I, S> {}
impl<I: AnyOrderedBy + UpdateQualifiers, L: LimitValue> UpdateQualifiers for Limit<I, L> {}

pub struct Update<T: Table, S: SetList, L: UpdateQualifiers> {
    pub(crate) sets: S,

    pub(crate) qualifiers: L,
    pub(crate) _phantom: PhantomData<T>,
}

impl<
        T: Table + QueryTree<UpEnd>,
        S: SetList + QueryTree<T::MaxUp>,
        L: UpdateQualifiers + QueryTree<S::MaxUp>,
    > QueryTree<UpEnd> for Update<T, S, L>
{
    type MaxUp = L::MaxUp;
}

impl<T: Table + ToSql, S: SetList + ToSql, L: UpdateQualifiers + ToSql> ToSql for Update<T, S, L> {
    const SQL: ConstSqlStr = sql_concat!("UPDATE ", T, " SET ", S, " ", L);

    fn collect_parameters(&self, f: &mut Vec<crate::QueryValue>) {
        self.sets.collect_parameters(f);
        self.qualifiers.collect_parameters(f);
    }
}

pub trait SetList {}

pub struct SetListCons<H: IsSet, T: SetList> {
    pub head: H,
    pub tail: T,
}

// TODO: Typecheck values
pub struct Set<T: Ty, A: TableAlias, N: FieldName, V: Value> {
    pub field: Field<T, A, N>,
    pub value: V,
}

pub trait IsSet {}

impl<T: Ty, A: TableAlias, N: FieldName, V: Value> SetList for Set<T, A, N, V> {}
impl<T: Ty, A: TableAlias, N: FieldName, V: Value> IsSet for Set<T, A, N, V> {}
impl<U: Up, T: Ty, A: TableAlias, N: FieldName, V: Value + QueryTree<U>> QueryTree<U>
    for Set<T, A, N, V>
{
    type MaxUp = V::MaxUp;
}

impl<H: IsSet, T: SetList> SetList for SetListCons<H, T> {}
impl<U: Up, H: IsSet + QueryTree<U>, T: SetList + QueryTree<H::MaxUp>> QueryTree<U>
    for SetListCons<H, T>
{
    type MaxUp = T::MaxUp;
}

impl<T: Ty, A: TableAlias, N: FieldName, V: Value + ToSql> ToSql for Set<T, A, N, V>
where
    Field<T, A, N>: ToSql,
{
    const SQL: ConstSqlStr = sql_concat!([Field::<T, A, N>], " = ", V);

    fn collect_parameters(&self, f: &mut Vec<crate::QueryValue>) {
        self.field.collect_parameters(f);
        self.value.collect_parameters(f);
    }
}

impl<H: IsSet + ToSql, T: SetList + ToSql> ToSql for SetListCons<H, T> {
    const SQL: ConstSqlStr = sql_concat!(H, ", ", T);

    fn collect_parameters(&self, f: &mut Vec<crate::QueryValue>) {
        self.head.collect_parameters(f);
        self.tail.collect_parameters(f);
    }
}

impl<T: Table, S: SetList, L: UpdateQualifiers> QueryRoot for Update<T, S, L> where
    Self: QueryTree<UpEnd> + ToSql
{
}

#[cfg(test)]
mod tests {
    use crate::{
        expr::{CmpEq, ConstI64, ConstStr},
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
    pub fn test_update() {
        assert_eq!(
            ground(Foo::update(
                |t| set! {
                    t.name => ConstStr::<"Test">,
                    t.value => ConstI64::<5>,
                },
                |t| CmpEq(ConstI64::<6>, t.id).as_where()
            ))
            .sql_str(),
            "UPDATE `Foo` SET `Name` = 'Test', `Value` = 5 WHERE (6 = `Id`)"
        );
    }
}
