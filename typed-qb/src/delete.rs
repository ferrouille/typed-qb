//! `DELETE FROM` queries
//! 
//! ```rust
//! # #![feature(generic_associated_types)] 
//! # use typed_qb::__doctest::*; 
//! # let mut conn = FakeConn;
//! let results = conn.typed_exec(Users::delete(|user| expr!(user.id = 5).limit::<1>()))?;
//! # Ok::<(), mysql::Error>(())
//! ```

use crate::qualifiers::{
    AnyHaving, AnyOrderedBy, AnyWhere, Limit, LimitValue, OrderBy, OrderBySeq,
};
use crate::{sql_concat, ConstSqlStr, QueryRoot, QueryTree, Table, ToSql, UpEnd};
use std::marker::PhantomData;

pub trait DeleteQualifiers {}

impl<W: AnyWhere> DeleteQualifiers for W {}
impl<I: AnyHaving + DeleteQualifiers, S: OrderBySeq> DeleteQualifiers for OrderBy<I, S> {}
impl<I: AnyOrderedBy + DeleteQualifiers, L: LimitValue> DeleteQualifiers for Limit<I, L> {}

pub struct Delete<T: Table, L: DeleteQualifiers> {
    pub(crate) qualifiers: L,
    pub(crate) _phantom: PhantomData<T>,
}

impl<T: Table + QueryTree<UpEnd>, L: DeleteQualifiers + QueryTree<T::MaxUp>> QueryTree<UpEnd>
    for Delete<T, L>
{
    type MaxUp = L::MaxUp;
}

impl<T: Table + ToSql, L: DeleteQualifiers + ToSql> ToSql for Delete<T, L> {
    const SQL: ConstSqlStr = sql_concat!("DELETE FROM ", T, " ", L);

    fn collect_parameters(&self, f: &mut Vec<crate::QueryValue>) {
        self.qualifiers.collect_parameters(f);
    }
}

impl<T: Table, L: DeleteQualifiers> QueryRoot for Delete<T, L> where Self: QueryTree<UpEnd> + ToSql {}

#[cfg(test)]
mod tests {
    use crate::{
        expr::{CmpEq, ConstI64},
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
    pub fn test_delete() {
        assert_eq!(
            ground(Foo::delete(|t| CmpEq(ConstI64::<6>, t.id).limit::<5>())).sql_str(),
            "DELETE FROM `Foo` WHERE (6 = `Id`) LIMIT 5"
        );
    }
}
