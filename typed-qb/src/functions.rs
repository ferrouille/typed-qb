use crate::{
    expr::{Distinct, Value, ValueOrStar},
    typing::{BigInt, Grouped, NonNullable, Nullable, Signed, SimpleTy, Ty, Unsigned},
    ConstSqlStr, QueryTree, ToSql, Up,
};

// TODO: Macro to deduplicate function definitions
#[derive(Debug, Clone)]
pub struct Sum<V: Value> {
    of: V,
}

impl<U: Up, V: Value + QueryTree<U>> QueryTree<U> for Sum<V> {
    type MaxUp = V::MaxUp;
}

#[allow(non_snake_case)]
pub fn SUM<V: Value>(of: V) -> Sum<V> {
    Sum { of }
}

impl<V: Value> Value for Sum<V> {
    type Ty = SimpleTy<BigInt<Signed>, Nullable>;
    type Grouped = Grouped;
}

impl<V: Value + ToSql> ToSql for Sum<V> {
    const SQL: ConstSqlStr = crate::sql_concat!("SUM(", V, ")");

    fn collect_parameters(&self, f: &mut Vec<crate::QueryValue>) {
        self.of.collect_parameters(f)
    }
}

#[derive(Debug, Clone)]
pub struct Max<V: Value> {
    of: V,
}

impl<U: Up, V: Value + QueryTree<U>> QueryTree<U> for Max<V> {
    type MaxUp = V::MaxUp;
}

#[allow(non_snake_case)]
pub fn MAX<V: Value>(of: V) -> Max<V> {
    Max { of }
}

impl<V: Value> Value for Max<V> {
    type Ty = SimpleTy<BigInt<Signed>, Nullable>;
    type Grouped = Grouped;
}

impl<V: Value + ToSql> ToSql for Max<V> {
    const SQL: ConstSqlStr = crate::sql_concat!("MAX(", V, ")");

    fn collect_parameters(&self, f: &mut Vec<crate::QueryValue>) {
        self.of.collect_parameters(f)
    }
}

#[derive(Debug, Clone)]
pub struct IfNull<X: Value, Y: Value> {
    val: X,
    ifnull: Y,
}

impl<U: Up, X: Value + QueryTree<U>, Y: Value + QueryTree<X::MaxUp>> QueryTree<U> for IfNull<X, Y> {
    type MaxUp = Y::MaxUp;
}

#[allow(non_snake_case)]
pub fn IFNULL<X: Value, Y: Value>(x: X, y: Y) -> IfNull<X, Y> {
    IfNull { val: x, ifnull: y }
}

impl<X: Value, Y: Value> Value for IfNull<X, Y> {
    // TODO: Merge first and second type
    type Ty = SimpleTy<<X::Ty as Ty>::Base, <Y::Ty as Ty>::Nullable>;

    // TODO: Merge grouping
    type Grouped = X::Grouped;
}

impl<X: Value + ToSql, Y: Value + ToSql> ToSql for IfNull<X, Y> {
    const SQL: ConstSqlStr = crate::sql_concat!("IFNULL(", X, ", ", Y, ")");

    fn collect_parameters(&self, f: &mut Vec<crate::QueryValue>) {
        self.val.collect_parameters(f);
        self.ifnull.collect_parameters(f);
    }
}

#[derive(Debug, Clone)]
pub struct Count<V: ValueStarOrDistinct> {
    of: V,
}

pub trait ValueStarOrDistinct {}

impl<V: ValueOrStar> ValueStarOrDistinct for V {}
impl<V: ValueOrStar> ValueStarOrDistinct for Distinct<V> {}

impl<U: Up, V: ValueStarOrDistinct + QueryTree<U>> QueryTree<U> for Count<V> {
    type MaxUp = V::MaxUp;
}

#[allow(non_snake_case)]
pub fn COUNT<V: ValueStarOrDistinct>(of: V) -> Count<V> {
    Count { of }
}

impl<V: ValueStarOrDistinct> Value for Count<V> {
    type Ty = SimpleTy<BigInt<Unsigned>, NonNullable>;
    type Grouped = Grouped;
}

impl<V: ValueStarOrDistinct + ToSql> ToSql for Count<V> {
    const SQL: ConstSqlStr = crate::sql_concat!("COUNT(", V, ")");

    fn collect_parameters(&self, f: &mut Vec<crate::QueryValue>) {
        self.of.collect_parameters(f)
    }
}
