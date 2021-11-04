use crate::select::{RowKind, ZeroOrMore, ZeroOrOne};
use crate::{
    expr::{Parameter, Value},
    typing::{Bool, Ty},
    ConstSqlStr, QueryTree, QueryValue, ToSql, Up,
};
use std::fmt;

pub trait ModifyRows {
    type Rows<R: RowKind>: RowKind;
}

// Shorthands for where conditions
pub trait AsWhere: Sized + Value
where
    Self::Ty: Ty<Base = Bool>,
{
    fn as_where(self) -> Where<Self> {
        Where::new(self)
    }

    fn group_by<S: GroupBySeq>(self, values: S) -> GroupBy<Where<Self>, S> {
        self.as_where().group_by(values)
    }

    fn having<V: Value>(self, value: V) -> Having<Where<Self>, V> {
        self.as_where().having(value)
    }

    fn order_by<S: OrderBySeq>(self, values: S) -> OrderBy<Where<Self>, S> {
        self.as_where().order_by(values)
    }

    fn limit<const N: usize>(self) -> Limit<Where<Self>, ConstLimit<N>>
    where
        ConstLimit<N>: ModifyRows,
    {
        self.as_where().limit::<N>()
    }

    fn limit_offset<const N: usize, const OFFSET: usize>(
        self,
    ) -> Limit<Where<Self>, ConstLimitAndOffset<N, OFFSET>>
    where
        ConstLimitAndOffset<N, OFFSET>: ModifyRows,
    {
        self.as_where().limit_offset::<N, OFFSET>()
    }

    fn limit_param(self, row_count: usize) -> Limit<Where<Self>, ParameterLimit> {
        self.as_where().limit_param(row_count)
    }

    fn limit_offset_params(
        self,
        row_count: usize,
        offset: usize,
    ) -> Limit<Where<Self>, ParameterLimitAndOffset> {
        self.as_where().limit_offset_params(row_count, offset)
    }
}

impl<V: Value> AsWhere for V where V::Ty: Ty<Base = Bool> {}

// WHERE
impl<T: AnyWhere> AnyGroupedBy for T {}
pub trait AnyWhere: AnyGroupedBy {
    fn group_by<S: GroupBySeq>(self, values: S) -> GroupBy<Self, S> {
        GroupBy {
            inner: self,
            values,
        }
    }
}

#[derive(Debug)]
pub struct AllRows;

impl<U: Up> QueryTree<U> for AllRows {
    type MaxUp = U;
}

#[derive(Debug)]
pub struct Where<V: Value> {
    condition: V,
}

impl<U: Up, V: Value + QueryTree<U>> QueryTree<U> for Where<V> {
    type MaxUp = V::MaxUp;
}

impl AnyWhere for AllRows {}
impl<V: Value> AnyWhere for Where<V> {}
impl ModifyRows for AllRows {
    type Rows<R: RowKind> = R;
}

impl<V: Value> ModifyRows for Where<V> {
    type Rows<R: RowKind> = R;
}

impl<V: Value> Where<V> {
    pub const fn new(condition: V) -> Self
    where
        V::Ty: Ty<Base = Bool>,
    {
        Where { condition }
    }
}

// GROUP BY
impl<T: AnyGroupedBy> AnyHaving for T {}
pub trait AnyGroupedBy: ModifyRows + Sized {
    fn having<V: Value>(self, value: V) -> Having<Self, V> {
        Having { inner: self, value }
    }
}

pub trait GroupBySeq: Sized {
    fn then_by<H: Value>(self, value: H) -> GroupByCons<H, Self> {
        GroupByCons {
            head: value,
            tail: self,
        }
    }
}

pub struct GroupByCons<H: Value, T: GroupBySeq> {
    head: H,
    tail: T,
}

impl<H: Value, T: GroupBySeq> GroupBySeq for GroupByCons<H, T> {}

impl<V: Value> GroupBySeq for V {}

#[derive(Debug)]
pub struct GroupBy<I: AnyWhere, S: GroupBySeq> {
    inner: I,
    values: S,
}

impl<U: Up, I: AnyWhere + QueryTree<U>, S: GroupBySeq + QueryTree<I::MaxUp>> QueryTree<U>
    for GroupBy<I, S>
{
    type MaxUp = S::MaxUp;
}

impl<I: AnyWhere, V: Value> AnyGroupedBy for GroupBy<I, V> {}
impl<I: AnyWhere, V: Value> ModifyRows for GroupBy<I, V> {
    type Rows<R: RowKind> = ZeroOrMore;
}

// HAVING
impl<T: AnyHaving> AnyOrderedBy for T {}
pub trait AnyHaving: ModifyRows + Sized {
    fn order_by<S: OrderBySeq>(self, values: S) -> OrderBy<Self, S> {
        OrderBy {
            inner: self,
            values,
        }
    }
}

#[derive(Debug)]
pub struct Having<I: AnyGroupedBy, V: Value> {
    inner: I,
    value: V,
}

impl<U: Up, I: AnyGroupedBy + QueryTree<U>, V: Value + QueryTree<I::MaxUp>> QueryTree<U>
    for Having<I, V>
{
    type MaxUp = V::MaxUp;
}

impl<I: AnyGroupedBy, V: Value> AnyHaving for Having<I, V> {}
impl<I: AnyGroupedBy, V: Value> ModifyRows for Having<I, V> {
    type Rows<R: RowKind> = <I as ModifyRows>::Rows<R>;
}

// ORDER BY
impl<T: AnyOrderedBy> AnyLimit for T {}
pub trait AnyOrderedBy: ModifyRows + Sized {
    fn limit<const N: usize>(self) -> Limit<Self, ConstLimit<N>>
    where
        ConstLimit<N>: ModifyRows,
    {
        Limit {
            inner: self,
            value: ConstLimit,
        }
    }

    fn limit_offset<const N: usize, const OFFSET: usize>(
        self,
    ) -> Limit<Self, ConstLimitAndOffset<N, OFFSET>>
    where
        ConstLimitAndOffset<N, OFFSET>: ModifyRows,
    {
        Limit {
            inner: self,
            value: ConstLimitAndOffset,
        }
    }

    fn limit_param(self, row_count: usize) -> Limit<Self, ParameterLimit> {
        Limit {
            inner: self,
            value: ParameterLimit(Parameter(row_count)),
        }
    }

    fn limit_offset_params(
        self,
        row_count: usize,
        offset: usize,
    ) -> Limit<Self, ParameterLimitAndOffset> {
        Limit {
            inner: self,
            value: ParameterLimitAndOffset(Parameter(row_count), Parameter(offset)),
        }
    }
}

#[derive(Debug)]
pub struct OrderBy<I: AnyHaving, S: OrderBySeq> {
    inner: I,
    values: S,
}

impl<U: Up, I: AnyHaving + QueryTree<U>, S: OrderBySeq + QueryTree<I::MaxUp>> QueryTree<U>
    for OrderBy<I, S>
{
    type MaxUp = S::MaxUp;
}

pub trait Direction {}
pub struct Asc;
pub struct Desc;

impl Direction for Asc {}
impl Direction for Desc {}

impl ToSql for Asc {
    const SQL: ConstSqlStr = ConstSqlStr::new("ASC");

    fn collect_parameters(&self, _f: &mut Vec<QueryValue>) {}
}

impl ToSql for Desc {
    const SQL: ConstSqlStr = ConstSqlStr::new("DESC");

    fn collect_parameters(&self, _f: &mut Vec<QueryValue>) {}
}

impl fmt::Display for Asc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ASC")
    }
}

impl fmt::Display for Desc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DESC")
    }
}

pub struct OrderByEntry<V: Value, D: Direction> {
    value: V,
    _direction: D,
}

pub trait CreateOrderByEntry: Value + Sized {
    fn asc(self) -> OrderByEntry<Self, Asc> {
        OrderByEntry {
            value: self,
            _direction: Asc,
        }
    }

    fn desc(self) -> OrderByEntry<Self, Desc> {
        OrderByEntry {
            value: self,
            _direction: Desc,
        }
    }
}

impl<V: Value> CreateOrderByEntry for V {}

pub trait OrderBySeq: Sized {
    fn then_by<H: Value, D: Direction>(self, value: OrderByEntry<H, D>) -> OrderByCons<H, D, Self> {
        OrderByCons {
            head: value,
            tail: self,
        }
    }
}

pub struct OrderByCons<H: Value, D: Direction, T: OrderBySeq> {
    head: OrderByEntry<H, D>,
    tail: T,
}

impl<U: Up, H: Value + QueryTree<U>, D: Direction, T: OrderBySeq + QueryTree<H::MaxUp>> QueryTree<U>
    for OrderByCons<H, D, T>
{
    type MaxUp = T::MaxUp;
}

impl<H: Value, D: Direction, T: OrderBySeq> OrderBySeq for OrderByCons<H, D, T> {}

impl<V: Value, D: Direction> OrderBySeq for OrderByEntry<V, D> {}

impl<U: Up, V: Value + QueryTree<U>, D: Direction> QueryTree<U> for OrderByEntry<V, D> {
    type MaxUp = V::MaxUp;
}

impl<I: AnyHaving, S: OrderBySeq> AnyOrderedBy for OrderBy<I, S> {}
impl<I: AnyHaving, S: OrderBySeq> ModifyRows for OrderBy<I, S> {
    type Rows<R: RowKind> = <I as ModifyRows>::Rows<R>;
}

// LIMIT
pub trait AnyLimit: ModifyRows {}

pub trait LimitValue: ToSql + ModifyRows {}

pub struct ConstLimit<const N: usize>;
pub struct ConstLimitAndOffset<const N: usize, const OFFSET: usize>;
pub struct ParameterLimitAndOffset(Parameter<usize>, Parameter<usize>);
pub struct ParameterLimit(Parameter<usize>);

impl<const N: usize> LimitValue for ConstLimit<N> where Self: ModifyRows {}
impl<const N: usize, const OFFSET: usize> LimitValue for ConstLimitAndOffset<N, OFFSET> where
    Self: ModifyRows
{
}
impl LimitValue for ParameterLimitAndOffset {}
impl LimitValue for ParameterLimit {}

impl ModifyRows for ConstLimit<1> {
    type Rows<R: RowKind> = ZeroOrOne;
}

impl<const OFFSET: usize> ModifyRows for ConstLimitAndOffset<1, OFFSET> {
    type Rows<R: RowKind> = ZeroOrOne;
}

impl<const N: usize> ModifyRows for ConstLimit<N>
where
    ConstCheck<{ N >= 2 }>: True,
{
    type Rows<R: RowKind> = R;
}

impl<const N: usize, const OFFSET: usize> ModifyRows for ConstLimitAndOffset<N, OFFSET>
where
    ConstCheck<{ N >= 2 }>: True,
{
    type Rows<R: RowKind> = R;
}

pub struct ConstCheck<const CHECK: bool>;

pub trait True {}
impl True for ConstCheck<true> {}

impl ModifyRows for ParameterLimitAndOffset {
    type Rows<R: RowKind> = R;
}

impl ModifyRows for ParameterLimit {
    type Rows<R: RowKind> = R;
}

impl<const N: usize> ToSql for ConstLimit<N> {
    const SQL: ConstSqlStr = ConstSqlStr::empty().append_usize(N);

    fn collect_parameters(&self, _f: &mut Vec<QueryValue>) {}
}

impl<U: Up, const N: usize> QueryTree<U> for ConstLimit<N> {
    type MaxUp = U;
}

impl<const N: usize, const OFFSET: usize> ToSql for ConstLimitAndOffset<N, OFFSET> {
    const SQL: ConstSqlStr = ConstSqlStr::empty()
        .append_usize(N)
        .append_str(", ")
        .append_usize(OFFSET);

    fn collect_parameters(&self, _f: &mut Vec<QueryValue>) {}
}

impl<U: Up, const N: usize, const OFFSET: usize> QueryTree<U> for ConstLimitAndOffset<N, OFFSET> {
    type MaxUp = U;
}

impl ToSql for ParameterLimitAndOffset {
    const SQL: ConstSqlStr = crate::sql_concat!([Parameter::<usize>], ", ", [Parameter::<usize>]);

    fn collect_parameters(&self, f: &mut Vec<QueryValue>) {
        self.0.collect_parameters(f);
        self.1.collect_parameters(f);
    }
}

impl<U: Up> QueryTree<U> for ParameterLimitAndOffset
where
    Parameter<usize>: QueryTree<U>,
    Parameter<usize>: QueryTree<<Parameter<usize> as QueryTree<U>>::MaxUp>,
{
    type MaxUp = <Parameter<usize> as QueryTree<<Parameter<usize> as QueryTree<U>>::MaxUp>>::MaxUp;
}

impl ToSql for ParameterLimit {
    const SQL: ConstSqlStr = crate::sql_concat!([Parameter::<usize>]);

    fn collect_parameters(&self, f: &mut Vec<QueryValue>) {
        self.0.collect_parameters(f);
    }
}

#[derive(Debug)]
pub struct Limit<I: AnyOrderedBy, L: LimitValue> {
    inner: I,
    value: L,
}

impl<U: Up, I: AnyOrderedBy + QueryTree<U>, L: LimitValue + QueryTree<I::MaxUp>> QueryTree<U>
    for Limit<I, L>
{
    type MaxUp = L::MaxUp;
}

impl<I: AnyOrderedBy, L: LimitValue> AnyLimit for Limit<I, L> {}
impl<I: AnyOrderedBy, L: LimitValue> ModifyRows for Limit<I, L> {
    type Rows<R: RowKind> = <L as ModifyRows>::Rows<<I as ModifyRows>::Rows<R>>;
}

impl ToSql for AllRows {
    const SQL: ConstSqlStr = ConstSqlStr::empty();

    fn collect_parameters(&self, _: &mut Vec<QueryValue>) {}
}

impl<V: Value + ToSql> ToSql for Where<V> {
    const SQL: ConstSqlStr = crate::sql_concat!("WHERE ", V);

    fn collect_parameters(&self, f: &mut Vec<QueryValue>) {
        self.condition.collect_parameters(f);
    }
}

impl<I: AnyWhere + ToSql, V: Value + ToSql> ToSql for GroupBy<I, V> {
    const SQL: ConstSqlStr = crate::sql_concat!(I, " GROUP BY ", V);

    fn collect_parameters(&self, f: &mut Vec<QueryValue>) {
        self.inner.collect_parameters(f);
        self.values.collect_parameters(f);
    }
}

impl<H: Value + ToSql, T: GroupBySeq + ToSql> ToSql for GroupByCons<H, T> {
    const SQL: ConstSqlStr = crate::sql_concat!(T, ", ", H);

    fn collect_parameters(&self, params: &mut Vec<QueryValue>) {
        self.tail.collect_parameters(params);
        self.head.collect_parameters(params);
    }
}

impl<I: AnyGroupedBy + ToSql, V: Value + ToSql> ToSql for Having<I, V> {
    const SQL: ConstSqlStr = crate::sql_concat!(I, " HAVING ", V);

    fn collect_parameters(&self, f: &mut Vec<QueryValue>) {
        self.inner.collect_parameters(f);
        self.value.collect_parameters(f);
    }
}

impl<I: AnyHaving + ToSql, S: OrderBySeq + ToSql> ToSql for OrderBy<I, S> {
    const SQL: ConstSqlStr = crate::sql_concat!(I, " ORDER BY ", S);

    fn collect_parameters(&self, f: &mut Vec<QueryValue>) {
        self.inner.collect_parameters(f);
        self.values.collect_parameters(f);
    }
}

impl<H: Value + ToSql, D: Direction + ToSql, S: OrderBySeq + ToSql> ToSql for OrderByCons<H, D, S> {
    const SQL: ConstSqlStr = crate::sql_concat!(S, ", ", [OrderByEntry::<H, D>]);

    fn collect_parameters(&self, params: &mut Vec<QueryValue>) {
        self.tail.collect_parameters(params);
        self.head.collect_parameters(params);
    }
}

impl<H: Value + ToSql, D: Direction + ToSql> ToSql for OrderByEntry<H, D> {
    const SQL: ConstSqlStr = crate::sql_concat!(H, " ", D);

    fn collect_parameters(&self, params: &mut Vec<QueryValue>) {
        self.value.collect_parameters(params)
    }
}

impl<I: AnyOrderedBy + ToSql, L: LimitValue> ToSql for Limit<I, L> {
    const SQL: ConstSqlStr = crate::sql_concat!(I, " LIMIT ", L);

    fn collect_parameters(&self, f: &mut Vec<QueryValue>) {
        self.inner.collect_parameters(f);
        self.value.collect_parameters(f);
    }
}
