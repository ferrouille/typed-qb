use crate::select::SelectQuery;
use crate::typing::{BigInt, IsGrouped, IsNullable, Signed, SimpleTy, Ty, Unsigned};
use crate::{
    sql_concat,
    typing::{
        AnyInt, BaseTy, Bool, CombineGrouping, CombineNullability, Int, MergeNumbers, NonNullable,
        Nullable, Text, Undetermined, F64,
    },
    ConstSqlStr, QueryTree, QueryValue, ToSql, Up,
};

pub trait Value {
    type Ty: Ty;
    type Grouped: IsGrouped;
}

impl<V: Value> Value for Box<V> {
    type Ty = V::Ty;
    type Grouped = V::Grouped;
}

impl<T: Ty, G: IsGrouped> Value for Box<dyn Value<Ty = T, Grouped = G>> {
    type Ty = T;
    type Grouped = G;
}

impl<T: Ty, G: IsGrouped> Value for &dyn Value<Ty = T, Grouped = G> {
    type Ty = T;
    type Grouped = G;
}

impl<T: Ty, V: Value<Ty = T>> Value for &V {
    type Ty = T;
    type Grouped = V::Grouped;
}

/// This is just an identity function with a trait bound.
pub fn is_value<T: Ty, V: Value<Ty = T>>(v: V) -> V {
    v
}

// The default integer size; MySQL will scale everything up to 64-bit unsigned or signed integers.
#[derive(Debug)]
pub struct ConstI64<const N: i64>;
impl<const N: i64> Value for ConstI64<N> {
    type Ty = SimpleTy<BigInt<Signed>, NonNullable>;
    type Grouped = Undetermined;
}

impl<const N: i64, U: Up> QueryTree<U> for ConstI64<N> {
    type MaxUp = U;
}

impl<const N: u64, U: Up> QueryTree<U> for ConstU64<N> {
    type MaxUp = U;
}

impl<const S: &'static str, U: Up> QueryTree<U> for ConstStr<S> {
    type MaxUp = U;
}

#[derive(Debug)]
pub struct ConstU64<const N: u64>;
impl<const N: u64> Value for ConstU64<N> {
    type Ty = SimpleTy<BigInt<Signed>, NonNullable>;
    type Grouped = Undetermined;
}

#[derive(Debug)]
pub struct ConstStr<const S: &'static str>;
impl<const S: &'static str> Value for ConstStr<S> {
    type Ty = SimpleTy<Text, NonNullable>;
    type Grouped = Undetermined;
}

#[derive(Debug)]
pub struct Null;
impl Value for Null {
    type Ty = SimpleTy<Int<Signed>, Nullable>;
    type Grouped = Undetermined;
}

impl<U: Up> QueryTree<U> for Null {
    type MaxUp = U;
}

impl ToSql for Null {
    const SQL: ConstSqlStr = ConstSqlStr::new("NULL");
    const NUM_PARAMS: usize = 0;

    fn collect_parameters<'a>(&self, params: &'a mut [QueryValue]) -> &'a mut [QueryValue] {
        params
    }
}

pub trait ParameterValue {
    type Ty: BaseTy;
    type Nullable: IsNullable;

    fn to_param(&self) -> QueryValue;
}

impl ParameterValue for usize {
    type Ty = BigInt<Unsigned>;
    type Nullable = NonNullable;

    fn to_param(&self) -> QueryValue {
        // TODO: What if usize != u64
        QueryValue::U64(*self as u64)
    }
}

impl ParameterValue for i64 {
    type Ty = BigInt<Signed>;
    type Nullable = NonNullable;

    fn to_param(&self) -> QueryValue {
        QueryValue::I64(*self)
    }
}

impl ParameterValue for u64 {
    type Ty = BigInt<Unsigned>;
    type Nullable = NonNullable;

    fn to_param(&self) -> QueryValue {
        QueryValue::U64(*self)
    }
}

impl ParameterValue for i32 {
    type Ty = Int<Signed>;
    type Nullable = NonNullable;

    fn to_param(&self) -> QueryValue {
        QueryValue::I64(*self as i64)
    }
}

impl ParameterValue for bool {
    type Ty = Bool;
    type Nullable = NonNullable;

    fn to_param(&self) -> QueryValue {
        QueryValue::I64(if *self { 1 } else { 0 })
    }
}

impl ParameterValue for String {
    type Ty = Text;
    type Nullable = NonNullable;

    fn to_param(&self) -> QueryValue {
        QueryValue::String(self.clone())
    }
}

impl ParameterValue for &str {
    type Ty = Text;
    type Nullable = NonNullable;

    fn to_param(&self) -> QueryValue {
        QueryValue::String(self.to_string())
    }
}

impl ParameterValue for chrono::NaiveDateTime {
    type Ty = Text;
    type Nullable = NonNullable;

    fn to_param(&self) -> QueryValue {
        QueryValue::DateTime(self.into())
    }
}

impl<P: ParameterValue> ParameterValue for Option<P> {
    type Ty = P::Ty;
    type Nullable = Nullable;

    fn to_param(&self) -> QueryValue {
        match self {
            Some(t) => t.to_param(),
            None => QueryValue::Null,
        }
    }
}

#[derive(Debug)]
pub struct Parameter<P: ParameterValue>(pub P);

impl<P: ParameterValue> Value for Parameter<P> {
    type Ty = SimpleTy<P::Ty, P::Nullable>;
    type Grouped = Undetermined;
}

impl<U: Up, P: ParameterValue> QueryTree<U> for Parameter<P> {
    type MaxUp = U;
}

#[derive(Debug)]
pub struct Add<X: Value, Y: Value>(pub X, pub Y);
impl<X: Value, Y: Value, I1: AnyInt, I2: AnyInt> Value for Add<X, Y>
where
    X::Ty: Ty<Base = I1>,
    Y::Ty: Ty<Base = I2>,
    (I1, I2): MergeNumbers,
    (<X::Ty as Ty>::Nullable, <Y::Ty as Ty>::Nullable): CombineNullability,
    (X::Grouped, Y::Grouped): CombineGrouping,
{
    type Ty = SimpleTy<
        <(I1, I2) as MergeNumbers>::Result,
        <(<X::Ty as Ty>::Nullable, <Y::Ty as Ty>::Nullable) as CombineNullability>::Result,
    >;
    type Grouped = <(X::Grouped, Y::Grouped) as CombineGrouping>::Result;
}

#[derive(Debug)]
pub struct Sub<X: Value, Y: Value>(pub X, pub Y);
impl<X: Value, Y: Value, I1: AnyInt, I2: AnyInt> Value for Sub<X, Y>
where
    X::Ty: Ty<Base = I1>,
    Y::Ty: Ty<Base = I2>,
    (I1, I2::WithSign<Signed>): MergeNumbers,
    (<X::Ty as Ty>::Nullable, <Y::Ty as Ty>::Nullable): CombineNullability,
    (X::Grouped, Y::Grouped): CombineGrouping,
{
    type Ty = SimpleTy<
        <(I1, I2::WithSign<Signed>) as MergeNumbers>::Result,
        <(<X::Ty as Ty>::Nullable, <Y::Ty as Ty>::Nullable) as CombineNullability>::Result,
    >;
    type Grouped = <(X::Grouped, Y::Grouped) as CombineGrouping>::Result;
}

#[derive(Debug)]
pub struct Mul<X: Value, Y: Value>(pub X, pub Y);
impl<X: Value, Y: Value, I1: AnyInt, I2: AnyInt> Value for Mul<X, Y>
where
    X::Ty: Ty<Base = I1>,
    Y::Ty: Ty<Base = I2>,
    (I1, I2): MergeNumbers,
    (<X::Ty as Ty>::Nullable, <Y::Ty as Ty>::Nullable): CombineNullability,
    (X::Grouped, Y::Grouped): CombineGrouping,
{
    type Ty = SimpleTy<
        <(I1, I2) as MergeNumbers>::Result,
        <(<X::Ty as Ty>::Nullable, <Y::Ty as Ty>::Nullable) as CombineNullability>::Result,
    >;
    type Grouped = <(X::Grouped, Y::Grouped) as CombineGrouping>::Result;
}

#[derive(Debug)]
pub struct Div<X: Value, Y: Value>(pub X, pub Y);
impl<X: Value, Y: Value, I1: AnyInt, I2: AnyInt> Value for Div<X, Y>
where
    X::Ty: Ty<Base = I1>,
    Y::Ty: Ty<Base = I2>,
    (X::Grouped, Y::Grouped): CombineGrouping,
{
    type Ty = SimpleTy<F64, Nullable>;
    type Grouped = <(X::Grouped, Y::Grouped) as CombineGrouping>::Result;
}

#[derive(Debug)]
pub struct Shl<X: Value, Y: Value>(pub X, pub Y);
impl<X: Value, Y: Value, I1: AnyInt, I2: AnyInt> Value for Shl<X, Y>
where
    X::Ty: Ty<Base = I1>,
    Y::Ty: Ty<Base = I2>,
    (<X::Ty as Ty>::Nullable, <Y::Ty as Ty>::Nullable): CombineNullability,
    (X::Grouped, Y::Grouped): CombineGrouping,
{
    type Ty = SimpleTy<
        BigInt<Unsigned>,
        <(<X::Ty as Ty>::Nullable, <Y::Ty as Ty>::Nullable) as CombineNullability>::Result,
    >;
    type Grouped = <(X::Grouped, Y::Grouped) as CombineGrouping>::Result;
}

#[derive(Debug)]
pub struct Shr<X: Value, Y: Value>(pub X, pub Y);
impl<X: Value, Y: Value, I1: AnyInt, I2: AnyInt> Value for Shr<X, Y>
where
    X::Ty: Ty<Base = I1>,
    Y::Ty: Ty<Base = I2>,
    (<X::Ty as Ty>::Nullable, <Y::Ty as Ty>::Nullable): CombineNullability,
    (X::Grouped, Y::Grouped): CombineGrouping,
{
    type Ty = SimpleTy<
        BigInt<Unsigned>,
        <(<X::Ty as Ty>::Nullable, <Y::Ty as Ty>::Nullable) as CombineNullability>::Result,
    >;
    type Grouped = <(X::Grouped, Y::Grouped) as CombineGrouping>::Result;
}

#[derive(Debug)]
pub struct BitOr<X: Value, Y: Value>(pub X, pub Y);
impl<X: Value, Y: Value, I1: AnyInt, I2: AnyInt> Value for BitOr<X, Y>
where
    X::Ty: Ty<Base = I1>,
    Y::Ty: Ty<Base = I2>,
    (<X::Ty as Ty>::Nullable, <Y::Ty as Ty>::Nullable): CombineNullability,
    (X::Grouped, Y::Grouped): CombineGrouping,
{
    type Ty = SimpleTy<
        BigInt<Unsigned>,
        <(<X::Ty as Ty>::Nullable, <Y::Ty as Ty>::Nullable) as CombineNullability>::Result,
    >;
    type Grouped = <(X::Grouped, Y::Grouped) as CombineGrouping>::Result;
}

#[derive(Debug)]
pub struct BitAnd<X: Value, Y: Value>(pub X, pub Y);
impl<X: Value, Y: Value, I1: AnyInt, I2: AnyInt> Value for BitAnd<X, Y>
where
    X::Ty: Ty<Base = I1>,
    Y::Ty: Ty<Base = I2>,
    (<X::Ty as Ty>::Nullable, <Y::Ty as Ty>::Nullable): CombineNullability,
    (X::Grouped, Y::Grouped): CombineGrouping,
{
    type Ty = SimpleTy<
        BigInt<Unsigned>,
        <(<X::Ty as Ty>::Nullable, <Y::Ty as Ty>::Nullable) as CombineNullability>::Result,
    >;
    type Grouped = <(X::Grouped, Y::Grouped) as CombineGrouping>::Result;
}

#[derive(Debug)]
pub struct BitXor<X: Value, Y: Value>(pub X, pub Y);
impl<X: Value, Y: Value, I1: AnyInt, I2: AnyInt> Value for BitXor<X, Y>
where
    X::Ty: Ty<Base = I1>,
    Y::Ty: Ty<Base = I2>,
    (<X::Ty as Ty>::Nullable, <Y::Ty as Ty>::Nullable): CombineNullability,
    (X::Grouped, Y::Grouped): CombineGrouping,
{
    type Ty = SimpleTy<
        BigInt<Unsigned>,
        <(<X::Ty as Ty>::Nullable, <Y::Ty as Ty>::Nullable) as CombineNullability>::Result,
    >;
    type Grouped = <(X::Grouped, Y::Grouped) as CombineGrouping>::Result;
}

// TODO: Make sure we're not comparing strings with numbers anywhere, because that means we're probably comparing the wrong values.
#[derive(Debug)]
pub struct CmpGt<X: Value, Y: Value>(pub X, pub Y);
impl<X: Value, Y: Value> Value for CmpGt<X, Y>
where
    (<X::Ty as Ty>::Nullable, <Y::Ty as Ty>::Nullable): CombineNullability,
    (X::Grouped, Y::Grouped): CombineGrouping,
{
    type Ty = SimpleTy<
        Bool,
        <(<X::Ty as Ty>::Nullable, <Y::Ty as Ty>::Nullable) as CombineNullability>::Result,
    >;
    type Grouped = <(X::Grouped, Y::Grouped) as CombineGrouping>::Result;
}

#[derive(Debug)]
pub struct CmpGe<X: Value, Y: Value>(pub X, pub Y);
impl<X: Value, Y: Value> Value for CmpGe<X, Y>
where
    (<X::Ty as Ty>::Nullable, <Y::Ty as Ty>::Nullable): CombineNullability,
    (X::Grouped, Y::Grouped): CombineGrouping,
{
    type Ty = SimpleTy<
        Bool,
        <(<X::Ty as Ty>::Nullable, <Y::Ty as Ty>::Nullable) as CombineNullability>::Result,
    >;
    type Grouped = <(X::Grouped, Y::Grouped) as CombineGrouping>::Result;
}

#[derive(Debug)]
pub struct CmpLt<X: Value, Y: Value>(pub X, pub Y);
impl<X: Value, Y: Value> Value for CmpLt<X, Y>
where
    (<X::Ty as Ty>::Nullable, <Y::Ty as Ty>::Nullable): CombineNullability,
    (X::Grouped, Y::Grouped): CombineGrouping,
{
    type Ty = SimpleTy<
        Bool,
        <(<X::Ty as Ty>::Nullable, <Y::Ty as Ty>::Nullable) as CombineNullability>::Result,
    >;
    type Grouped = <(X::Grouped, Y::Grouped) as CombineGrouping>::Result;
}

#[derive(Debug)]
pub struct CmpLe<X: Value, Y: Value>(pub X, pub Y);
impl<X: Value, Y: Value> Value for CmpLe<X, Y>
where
    (<X::Ty as Ty>::Nullable, <Y::Ty as Ty>::Nullable): CombineNullability,
    (X::Grouped, Y::Grouped): CombineGrouping,
{
    type Ty = SimpleTy<
        Bool,
        <(<X::Ty as Ty>::Nullable, <Y::Ty as Ty>::Nullable) as CombineNullability>::Result,
    >;
    type Grouped = <(X::Grouped, Y::Grouped) as CombineGrouping>::Result;
}

// TODO: Can we derive that a WHERE UniqueKey = X always produces zero or one rows?
#[derive(Debug)]
pub struct CmpEq<X: Value, Y: Value>(pub X, pub Y);
impl<X: Value, Y: Value> Value for CmpEq<X, Y>
where
    (<X::Ty as Ty>::Nullable, <Y::Ty as Ty>::Nullable): CombineNullability,
    (X::Grouped, Y::Grouped): CombineGrouping,
{
    type Ty = SimpleTy<
        Bool,
        <(<X::Ty as Ty>::Nullable, <Y::Ty as Ty>::Nullable) as CombineNullability>::Result,
    >;
    type Grouped = <(X::Grouped, Y::Grouped) as CombineGrouping>::Result;
}

#[derive(Debug)]
pub struct CmpNe<X: Value, Y: Value>(pub X, pub Y);
impl<X: Value, Y: Value> Value for CmpNe<X, Y>
where
    (<X::Ty as Ty>::Nullable, <Y::Ty as Ty>::Nullable): CombineNullability,
    (X::Grouped, Y::Grouped): CombineGrouping,
{
    type Ty = SimpleTy<
        Bool,
        <(<X::Ty as Ty>::Nullable, <Y::Ty as Ty>::Nullable) as CombineNullability>::Result,
    >;
    type Grouped = <(X::Grouped, Y::Grouped) as CombineGrouping>::Result;
}

#[derive(Debug)]
pub struct CmpEqNullSafe<X: Value, Y: Value>(pub X, pub Y);
impl<X: Value, Y: Value> Value for CmpEqNullSafe<X, Y>
where
    (X::Grouped, Y::Grouped): CombineGrouping,
{
    type Ty = SimpleTy<Bool, NonNullable>;
    type Grouped = <(X::Grouped, Y::Grouped) as CombineGrouping>::Result;
}

#[derive(Debug)]
pub struct CmpLike<X: Value, Y: Value>(pub X, pub Y);
impl<X: Value, Y: Value> Value for CmpLike<X, Y>
where
    (<X::Ty as Ty>::Nullable, <Y::Ty as Ty>::Nullable): CombineNullability,
    (X::Grouped, Y::Grouped): CombineGrouping,
{
    type Ty = SimpleTy<
        Bool,
        <(<X::Ty as Ty>::Nullable, <Y::Ty as Ty>::Nullable) as CombineNullability>::Result,
    >;
    type Grouped = <(X::Grouped, Y::Grouped) as CombineGrouping>::Result;
}

#[derive(Debug)]
pub struct IsNull<X: Value>(pub X);
impl<X: Value> Value for IsNull<X> {
    type Ty = SimpleTy<Bool, NonNullable>;
    type Grouped = X::Grouped;
}

#[derive(Debug)]
pub struct IsNotNull<X: Value>(pub X);
impl<X: Value> Value for IsNotNull<X> {
    type Ty = SimpleTy<Bool, NonNullable>;
    type Grouped = X::Grouped;
}

#[derive(Debug)]
pub struct IsTrue<X: Value>(pub X);
impl<X: Value> Value for IsTrue<X> {
    type Ty = SimpleTy<Bool, NonNullable>;
    type Grouped = X::Grouped;
}

#[derive(Debug)]
pub struct IsFalse<X: Value>(pub X);
impl<X: Value> Value for IsFalse<X> {
    type Ty = SimpleTy<Bool, NonNullable>;
    type Grouped = X::Grouped;
}

#[derive(Debug)]
pub struct IsUnknown<X: Value>(pub X);
impl<X: Value> Value for IsUnknown<X> {
    type Ty = SimpleTy<Bool, NonNullable>;
    type Grouped = X::Grouped;
}

#[derive(Debug)]
pub struct Neg<X: Value>(pub X);
impl<X: Value> Value for Neg<X> {
    // TODO:Should this become signed?
    type Ty = SimpleTy<<X::Ty as Ty>::Base, <X::Ty as Ty>::Nullable>;
    type Grouped = X::Grouped;
}

#[derive(Debug)]
pub struct Not<X: Value>(pub X);
impl<X: Value> Value for Not<X>
where
    X::Ty: Ty<Base = Bool>,
{
    type Ty = SimpleTy<Bool, <X::Ty as Ty>::Nullable>;
    type Grouped = X::Grouped;
}

#[derive(Debug)]
pub struct BitNeg<X: Value>(pub X);
impl<X: Value> Value for BitNeg<X> {
    type Ty = SimpleTy<BigInt<Unsigned>, <X::Ty as Ty>::Nullable>;
    type Grouped = X::Grouped;
}

#[derive(Debug)]
pub struct Or<X: Value, Y: Value>(pub X, pub Y);
impl<X: Value, Y: Value> Value for Or<X, Y>
where
    X::Ty: Ty<Base = Bool>,
    Y::Ty: Ty<Base = Bool>,
    (<X::Ty as Ty>::Nullable, <Y::Ty as Ty>::Nullable): CombineNullability,
    (X::Grouped, Y::Grouped): CombineGrouping,
{
    type Ty = SimpleTy<
        Bool,
        <(<X::Ty as Ty>::Nullable, <Y::Ty as Ty>::Nullable) as CombineNullability>::Result,
    >;
    type Grouped = X::Grouped;
}

#[derive(Debug)]
pub struct And<X: Value, Y: Value>(pub X, pub Y);
impl<X: Value, Y: Value> Value for And<X, Y>
where
    X::Ty: Ty<Base = Bool>,
    Y::Ty: Ty<Base = Bool>,
    (<X::Ty as Ty>::Nullable, <Y::Ty as Ty>::Nullable): CombineNullability,
    (X::Grouped, Y::Grouped): CombineGrouping,
{
    type Ty = SimpleTy<
        Bool,
        <(<X::Ty as Ty>::Nullable, <Y::Ty as Ty>::Nullable) as CombineNullability>::Result,
    >;
    type Grouped = X::Grouped;
}

#[derive(Debug)]
pub struct Xor<X: Value, Y: Value>(pub X, pub Y);
impl<X: Value, Y: Value> Value for Xor<X, Y>
where
    X::Ty: Ty<Base = Bool>,
    Y::Ty: Ty<Base = Bool>,
    (<X::Ty as Ty>::Nullable, <Y::Ty as Ty>::Nullable): CombineNullability,
    (X::Grouped, Y::Grouped): CombineGrouping,
{
    type Ty = SimpleTy<
        Bool,
        <(<X::Ty as Ty>::Nullable, <Y::Ty as Ty>::Nullable) as CombineNullability>::Result,
    >;
    type Grouped = X::Grouped;
}

macro_rules! gen_ops {
    ($({$($ops:tt)*} where {$($conds:tt)*} => $t:ty),*) => {
        $(
            impl<V: Value, $($ops)*> std::ops::Add<V> for $t where $($conds)* {
                type Output = Add<Self, V>;
                fn add(self, other: V) -> Self::Output {
                    Add(self, other)
                }
            }

            impl<V: Value, $($ops)*> std::ops::Sub<V> for $t where $($conds)* {
                type Output = Sub<Self, V>;
                fn sub(self, other: V) -> Self::Output {
                    Sub(self, other)
                }
            }

            impl<V: Value, $($ops)*> std::ops::Mul<V> for $t where $($conds)* {
                type Output = Mul<Self, V>;
                fn mul(self, other: V) -> Self::Output {
                    Mul(self, other)
                }
            }
        )*
    }
}

gen_ops!(
    {X: Value, Y: Value, I1: AnyInt, I2: AnyInt} where {
        X::Ty: Ty<Base = I1>, Y::Ty: Ty<Base = I2>,
        (I1, I2): MergeNumbers,
        (<X::Ty as Ty>::Nullable, <Y::Ty as Ty>::Nullable): CombineNullability,
        (X::Grouped, Y::Grouped): CombineGrouping,
    } => Add<X, Y>,
    {X: Value, Y: Value, I1: AnyInt, I2: AnyInt} where {
        X::Ty: Ty<Base = I1>, Y::Ty: Ty<Base = I2>,
        (I1, I2::WithSign<Signed>): MergeNumbers,
        (<X::Ty as Ty>::Nullable, <Y::Ty as Ty>::Nullable): CombineNullability,
        (X::Grouped, Y::Grouped): CombineGrouping,
    } => Sub<X, Y>,
    {X: Value, Y: Value, I1: AnyInt, I2: AnyInt} where {
        X::Ty: Ty<Base = I1>, Y::Ty: Ty<Base = I2>,
        (I1, I2): MergeNumbers,
        (<X::Ty as Ty>::Nullable, <Y::Ty as Ty>::Nullable): CombineNullability,
        (X::Grouped, Y::Grouped): CombineGrouping,
    } => Mul<X, Y>,
    {X: Value, Y: Value, I1: AnyInt, I2: AnyInt} where {
        X::Ty: Ty<Base = I1>,
        Y::Ty: Ty<Base = I2>,
        (X::Grouped, Y::Grouped): CombineGrouping,
    } => Div<X, Y>,
    {const N: i64} where {} => ConstI64<N>
);

macro_rules! gen_bin_ops {
    ($($t:ty => $op:literal),* $(,)*) => {
        $(
            impl<X: Value + ToSql, Y: Value + ToSql> ToSql for $t {
                const SQL: ConstSqlStr = $crate::sql_concat!("(", X, " ", $op, " ", Y, ")");
                const NUM_PARAMS: usize = X::NUM_PARAMS + Y::NUM_PARAMS;

                fn collect_parameters<'a>(&self, params: &'a mut [QueryValue]) -> &'a mut [QueryValue]   {
                    let params = self.0.collect_parameters(params);
                    self.1.collect_parameters(params)
                }
            }

            impl<U: Up, X: Value + QueryTree<U>, Y: Value + QueryTree<X::MaxUp>> QueryTree<U> for $t {
                type MaxUp = Y::MaxUp;
            }
        )*
    }
}

gen_bin_ops! {
    And<X, Y> => "AND",
    Or<X, Y> => "OR",
    Xor<X, Y> => "XOR",

    Add<X, Y> => "+",
    Sub<X, Y> => "-",
    Mul<X, Y> => "*",
    Div<X, Y> => "/",

    Shl<X, Y> => "<<",
    Shr<X, Y> => ">>",
    BitOr<X, Y> => "|",
    BitAnd<X, Y> => "&",
    BitXor<X, Y> => "^",

    CmpGt<X, Y> => ">",
    CmpGe<X, Y> => ">=",
    CmpEq<X, Y> => "=",
    CmpEqNullSafe<X, Y> => "<=>",
    CmpNe<X, Y> => "!=",
    CmpLe<X, Y> => "<=",
    CmpLt<X, Y> => "<",
    CmpLike<X, Y> => "LIKE",
}

macro_rules! gen_unary_ops {
    ($($t:ty => $op:literal),* $(,)*) => {
        $(
            impl<X: Value + ToSql> ToSql for $t {
                const SQL: ConstSqlStr = $crate::sql_concat!($op, " (", X, ")");
                const NUM_PARAMS: usize = X::NUM_PARAMS;

                fn collect_parameters<'a>(&self, params: &'a mut [QueryValue]) -> &'a mut [QueryValue]  {
                    self.0.collect_parameters(params)
                }
            }
        )*
    }
}

gen_unary_ops! {
    Not<X> => "NOT",
    Neg<X> => "-",
    BitNeg<X> => "~",
}

macro_rules! gen_postfix_unary_ops {
    ($($t:ty => $op:literal),* $(,)*) => {
        $(
            impl<X: Value + ToSql> ToSql for $t {
                const SQL: ConstSqlStr = $crate::sql_concat!("(", X, ") ", $op);
                const NUM_PARAMS: usize = X::NUM_PARAMS;

                fn collect_parameters<'a>(&self, params: &'a mut [QueryValue]) -> &'a mut [QueryValue]  {
                    self.0.collect_parameters(params)
                }
            }

            impl<U: Up, X: Value + QueryTree<U>> QueryTree<U> for $t {
                type MaxUp = X::MaxUp;
            }
        )*
    }
}

gen_postfix_unary_ops! {
    IsNull<X> => "IS NULL",
    IsNotNull<X> => "IS NOT NULL",
    IsTrue<X> => "IS TRUE",
    IsFalse<X> => "IS FALSE",
    IsUnknown<X> => "IS UNKNOWN",
}

#[derive(Debug)]
pub struct Exists<Q: SelectQuery>(pub Q);
impl<Q: SelectQuery> Value for Exists<Q> {
    type Ty = SimpleTy<Bool, NonNullable>;
    type Grouped = Undetermined;
}

impl<U: Up, Q: SelectQuery + QueryTree<U>> QueryTree<U> for Exists<Q> {
    type MaxUp = Q::MaxUp;
}

impl<Q: SelectQuery + ToSql> ToSql for Exists<Q> {
    const SQL: ConstSqlStr = sql_concat!("EXISTS ", Q);
    const NUM_PARAMS: usize = Q::NUM_PARAMS;

    fn collect_parameters<'a>(&self, params: &'a mut [QueryValue]) -> &'a mut [QueryValue] {
        self.0.collect_parameters(params)
    }
}

#[derive(Debug)]
pub struct Any<Q: SelectQuery>(pub Q);
impl<Q: SelectQuery> Value for Any<Q> {
    type Ty = SimpleTy<Bool, NonNullable>;
    type Grouped = Undetermined;
}

impl<U: Up, Q: SelectQuery + QueryTree<U>> QueryTree<U> for Any<Q> {
    type MaxUp = Q::MaxUp;
}

impl<Q: SelectQuery + ToSql> ToSql for Any<Q> {
    const SQL: ConstSqlStr = sql_concat!("ANY ", Q);
    const NUM_PARAMS: usize = Q::NUM_PARAMS;

    fn collect_parameters<'a>(&self, params: &'a mut [QueryValue]) -> &'a mut [QueryValue] {
        self.0.collect_parameters(params)
    }
}

#[derive(Debug)]
pub struct All<Q: SelectQuery>(pub Q);
impl<Q: SelectQuery> Value for All<Q> {
    type Ty = SimpleTy<Bool, NonNullable>;
    type Grouped = Undetermined;
}

impl<U: Up, Q: SelectQuery + QueryTree<U>> QueryTree<U> for All<Q> {
    type MaxUp = Q::MaxUp;
}

impl<Q: SelectQuery + ToSql> ToSql for All<Q> {
    const SQL: ConstSqlStr = sql_concat!("ALL ", Q);
    const NUM_PARAMS: usize = Q::NUM_PARAMS;

    fn collect_parameters<'a>(&self, params: &'a mut [QueryValue]) -> &'a mut [QueryValue] {
        self.0.collect_parameters(params)
    }
}

impl<const N: i64> ToSql for ConstI64<N> {
    const SQL: ConstSqlStr = ConstSqlStr::empty().append_i64(N);
    const NUM_PARAMS: usize = 0;

    fn collect_parameters<'a>(&self, params: &'a mut [QueryValue]) -> &'a mut [QueryValue] {
        params
    }
}

impl<const N: u64> ToSql for ConstU64<N> {
    const SQL: ConstSqlStr = ConstSqlStr::empty().append_u64(N);
    const NUM_PARAMS: usize = 0;

    fn collect_parameters<'a>(&self, params: &'a mut [QueryValue]) -> &'a mut [QueryValue] {
        params
    }
}

impl<const S: &'static str> ToSql for ConstStr<S> {
    const SQL: ConstSqlStr = ConstSqlStr::empty().append_quoted_str(S);
    const NUM_PARAMS: usize = 0;

    fn collect_parameters<'a>(&self, params: &'a mut [QueryValue]) -> &'a mut [QueryValue] {
        params
    }
}

impl<P: ParameterValue> ToSql for Parameter<P> {
    const SQL: ConstSqlStr = ConstSqlStr::new("?");
    const NUM_PARAMS: usize = 1;

    fn collect_parameters<'a>(&self, params: &'a mut [QueryValue]) -> &'a mut [QueryValue] {
        params[0] = self.0.to_param();
        &mut params[1..]
    }
}

#[derive(Debug)]
pub struct Star;

#[derive(Debug)]
pub struct Distinct<X: ValueOrStar>(pub X);

pub trait ValueOrStar {}
impl<V: Value> ValueOrStar for V {}
impl ValueOrStar for Star {}

impl ToSql for Star {
    const SQL: ConstSqlStr = ConstSqlStr::new("*");
    const NUM_PARAMS: usize = 0;

    fn collect_parameters<'a>(&self, params: &'a mut [QueryValue]) -> &'a mut [QueryValue] {
        params
    }
}

impl<U: Up> QueryTree<U> for Star {
    type MaxUp = U;
}

impl<X: ValueOrStar + ToSql> ToSql for Distinct<X> {
    const SQL: ConstSqlStr = sql_concat!("DISTINCT ", X);
    const NUM_PARAMS: usize = 0;

    fn collect_parameters<'a>(&self, params: &'a mut [QueryValue]) -> &'a mut [QueryValue] {
        self.0.collect_parameters(params)
    }
}

impl<U: Up, X: ValueOrStar + QueryTree<U>> QueryTree<U> for Distinct<X> {
    type MaxUp = X::MaxUp;
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        functions::COUNT,
        typing::{Int, Ungrouped},
        ConstSqlStr,
    };
    use std::{fmt, marker::PhantomData};

    #[derive(Copy, Clone)]
    struct V<T: Ty>(PhantomData<T>);

    impl<T: Ty> std::fmt::Debug for V<T> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "v")
        }
    }

    impl<T: Ty> V<T> {
        const V: V<T> = Self(PhantomData);
    }

    impl<T: Ty> Value for V<T> {
        type Ty = T;
        type Grouped = Ungrouped;
    }

    impl<T: Ty> ToSql for V<T> {
        const SQL: ConstSqlStr = ConstSqlStr::new("v");
        const NUM_PARAMS: usize = 0;

        fn collect_parameters<'a>(&self, params: &'a mut [QueryValue]) -> &'a mut [QueryValue] {
            params
        }
    }

    fn is_value_noargs<V: Value>() {}
    fn is_value<V: Value>(_v: &V) {}
    fn is_nullable<V: Value>(_v: &V)
    where
        V::Ty: Ty<Nullable = Nullable>,
    {
    }
    fn is_non_nullable<V: Value>(_v: &V)
    where
        V::Ty: Ty<Nullable = NonNullable>,
    {
    }
    fn is_ungrouped<V: Value<Grouped = Ungrouped>>(_v: &V) {}
    fn is_grouping_undetermined<V: Value<Grouped = Undetermined>>(_v: &V) {}

    #[test]
    pub fn test_typing() {
        let x = Sub(
            V::<SimpleTy<BigInt<Signed>, NonNullable>>::V,
            V::<SimpleTy<BigInt<Signed>, NonNullable>>::V,
        ) + V::<SimpleTy<BigInt<Signed>, NonNullable>>::V;
        is_value(&x);
        is_non_nullable(&x);
        is_ungrouped(&x);

        let x = Sub(
            V::<SimpleTy<BigInt<Signed>, NonNullable>>::V,
            V::<SimpleTy<Int<Signed>, NonNullable>>::V,
        ) + V::<SimpleTy<BigInt<Signed>, NonNullable>>::V;
        is_value(&x);
        is_non_nullable(&x);
        is_ungrouped(&x);

        is_value_noargs::<Sub<ConstI64<5>, ConstI64<8>>>();

        let x = (ConstI64::<5> - ConstI64::<7>) + ConstI64::<2>;
        is_value(&x);
        is_non_nullable(&x);
        is_grouping_undetermined(&x);
        println!("{:?}", x);

        let x = Add(
            V::<SimpleTy<BigInt<Signed>, NonNullable>>::V,
            V::<SimpleTy<BigInt<Signed>, NonNullable>>::V,
        );
        is_value(&x);
        is_non_nullable(&x);
        is_ungrouped(&x);

        let y = Add(
            V::<SimpleTy<BigInt<Signed>, NonNullable>>::V,
            V::<SimpleTy<BigInt<Signed>, Nullable>>::V,
        );
        is_value(&y);
        is_nullable(&y);
        is_ungrouped(&y);

        let z = Add(
            V::<SimpleTy<BigInt<Signed>, NonNullable>>::V,
            V::<SimpleTy<BigInt<Signed>, Nullable>>::V,
        );
        is_value(&z);
        is_nullable(&z);
        // is_grouped(&z);
    }

    #[test]
    pub fn type_tests() {
        // Make sure COUNT(*) and COUNT(DISTINCT *) implement Value, ToSql and QueryTree<_>.
        // * and DISTINCT are not Values on their own
        fn _test1<U: Up>() -> impl Value + ToSql + QueryTree<U> {
            COUNT(Distinct(Star))
        }
        fn _test2<U: Up>() -> impl Value + ToSql + QueryTree<U> {
            COUNT(Star)
        }
    }
}
