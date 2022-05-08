use crate::{
    expr::{Distinct, Value, ValueOrStar},
    typing::{
        BigInt, Grouped, NonNullable, Nullable, Signed, SimpleTy, Ty, Undetermined, Unsigned, F64, Bool,
    },
    ConstSqlStr, QueryTree, QueryValue, ToSql, Up,
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
    const NUM_PARAMS: usize = V::NUM_PARAMS;

    fn collect_parameters<'a>(&self, params: &'a mut [QueryValue]) -> &'a mut [QueryValue] {
        self.of.collect_parameters(params)
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
    type Ty = SimpleTy<<V::Ty as Ty>::Base, Nullable>;
    type Grouped = Grouped;
}

impl<V: Value + ToSql> ToSql for Max<V> {
    const SQL: ConstSqlStr = crate::sql_concat!("MAX(", V, ")");
    const NUM_PARAMS: usize = V::NUM_PARAMS;

    fn collect_parameters<'a>(&self, params: &'a mut [QueryValue]) -> &'a mut [QueryValue] {
        self.of.collect_parameters(params)
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
    const NUM_PARAMS: usize = X::NUM_PARAMS + Y::NUM_PARAMS;

    fn collect_parameters<'a>(&self, params: &'a mut [QueryValue]) -> &'a mut [QueryValue] {
        let params = self.val.collect_parameters(params);
        self.ifnull.collect_parameters(params)
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
    const NUM_PARAMS: usize = V::NUM_PARAMS;

    fn collect_parameters<'a>(&self, params: &'a mut [QueryValue]) -> &'a mut [QueryValue] {
        self.of.collect_parameters(params)
    }
}

#[derive(Debug, Clone)]
pub struct Rand;

impl<U: Up> QueryTree<U> for Rand {
    type MaxUp = U;
}

#[allow(non_snake_case)]
pub fn RAND() -> Rand {
    Rand
}

impl Value for Rand {
    type Ty = SimpleTy<F64, NonNullable>;
    type Grouped = Undetermined;
}

impl ToSql for Rand {
    const SQL: ConstSqlStr = crate::sql_concat!("RAND()");
    const NUM_PARAMS: usize = 0;

    fn collect_parameters<'a>(&self, params: &'a mut [QueryValue]) -> &'a mut [QueryValue] {
        params
    }
}

#[derive(Debug, Clone)]
pub struct If<X: Value, Y: Value, Z: Value> {
    cond: X,
    then_val: Y,
    else_val: Z,
}

impl<U: Up, X: Value + QueryTree<U>, Y: Value + QueryTree<X::MaxUp>, Z: Value + QueryTree<Y::MaxUp>> QueryTree<U> for If<X, Y, Z> {
    type MaxUp = Z::MaxUp;
}

#[allow(non_snake_case)]
pub fn IF<X: Value, Y: Value, Z: Value>(x: X, y: Y, z: Z) -> If<X, Y, Z> {
    If { cond: x, then_val: y, else_val: z }
}

impl<X: Value, Y: Value, Z: Value> Value for If<X, Y, Z> 
    where X::Ty: Ty<Base = Bool> {
    // TODO: Merge first and second type
    type Ty = SimpleTy<<Y::Ty as Ty>::Base, <Y::Ty as Ty>::Nullable>;

    // TODO: Merge grouping
    type Grouped = X::Grouped;
}

impl<X: Value + ToSql, Y: Value + ToSql, Z: Value + ToSql> ToSql for If<X, Y, Z> {
    const SQL: ConstSqlStr = crate::sql_concat!("IF(", X, ", ", Y, ",", Z, ")");
    const NUM_PARAMS: usize = X::NUM_PARAMS + Y::NUM_PARAMS;

    fn collect_parameters<'a>(&self, params: &'a mut [QueryValue]) -> &'a mut [QueryValue] {
        let params = self.cond.collect_parameters(params);
        let params = self.then_val.collect_parameters(params);
        self.else_val.collect_parameters(params)
    }
}