//! `SELECT` queries
//!
//! ```rust
//! # #![feature(generic_associated_types)]
//! # use typed_qb::__doctest::*;
//! # let mut conn = FakeConn;
//! let name = "root";
//! let results = conn.typed_exec(Users::query(|user| user.name))?;
//! # Ok::<(), mysql::Error>(())
//! ```

use crate::prelude::AllRows;
use crate::qualifiers::AnyLimit;
use crate::typing::{
    AllNullable, Grouped, IsGrouped, KeepOriginalNullability, NullabilityModifier, Ty,
    Undetermined, Ungrouped,
};
use crate::{
    expr::Value, Alias, ConstSqlStr, QueryRoot, QueryTree, QueryValue, TableAlias, TableReference,
    ToSql, Up, UpEnd, UpOne,
};
use crate::{sql_concat, Field, FieldName, Fieldable};
use log::debug;
use std::{fmt, marker::PhantomData};

/// Specify `WHERE`, `ORDER BY`, `GROUP BY`, `LIMIT`, etc. clauses on [`SelectedData`].
///
/// See also [`data!`].
///
/// ```rust
/// # #![feature(generic_associated_types)]
/// # use typed_qb::__doctest::*;
/// # let mut conn = FakeConn;
/// let results = conn.typed_query(Users::query(|user|
///     select(data! {
///         id: user.id,
///         username: user.name,
///     }, |selected|
///         expr!(selected.id < 100)
///             .order_by(selected.username.asc()
///                 .then_by(user.name.desc())
///             )
///             .limit::<5>()
///     )
/// ))?;
/// # Ok::<(), mysql::Error>(())
/// ```
pub fn select<D: SelectedData, L: AnyLimit>(
    data: D,
    qualifiers: impl FnOnce(&D::Instantiated<()>) -> L,
) -> SelectWithoutFrom<D, L> {
    let new = D::instantiate();
    SelectWithoutFrom {
        query: qualifiers(&new),
        data,
    }
}

pub trait RowKind {
    type Repr<T>;
}

pub trait NoMoreThanOneRow {}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ZeroOrOne;

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ExactlyOne;

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ZeroOrMore;

impl NoMoreThanOneRow for ZeroOrOne {}
impl NoMoreThanOneRow for ExactlyOne {}

impl RowKind for ExactlyOne {
    type Repr<T> = T;
}

impl RowKind for ZeroOrOne {
    type Repr<T> = Option<T>;
}

impl RowKind for ZeroOrMore {
    type Repr<T> = Vec<T>;
}

pub trait SelectQuery {
    type Columns: SelectedData;
    type Rows: RowKind;
    type Repr = <Self::Rows as RowKind>::Repr<<Self::Columns as SelectedData>::Repr>;
    type Inverted: SelectQuery;
}

pub trait FromTables {}

#[doc(hidden)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct NilTable;
impl FromTables for NilTable {}

impl<U: Up> QueryTree<U> for NilTable {
    type MaxUp = U;
}

impl ToSql for NilTable {
    const SQL: ConstSqlStr = ConstSqlStr::empty();

    fn collect_parameters(&self, _f: &mut Vec<QueryValue>) {}
}

pub trait PartialSelect<D: SelectedData, L: AnyLimit> {
    type From: FromTables;

    fn map_from<T: FromTables, F: FnOnce(Self::From) -> T>(self, f: F) -> Select<D, L, T>;
}

/// Describes any data selected from a table.
/// You should **not** implement this trait.
/// Instead, use the [`data!`] macro.
pub trait SelectedData: Sized {
    type Instantiated<A: TableAlias>;
    type Queried;
    type Repr;
    type Rows: RowKind;
    type AllNullable: SelectedData;
    const NUM_COLS: usize;

    fn instantiate<A: TableAlias>() -> Self::Instantiated<A>;

    fn make_nullable(self) -> Self::AllNullable;

    fn from_row(columns: &[QueryValue]) -> Self::Queried;
}

#[derive(Debug, Clone)]
pub struct Select<D: SelectedData, L: AnyLimit, F: FromTables> {
    select: SelectWithoutFrom<D, L>,
    from: F,
}

impl<D: SelectedData, L: AnyLimit, F: FromTables> SelectQuery for Select<D, L, F> {
    type Columns = D;
    type Rows = L::Rows<D::Rows>;
    type Inverted = Self;
}

impl<D: SelectedData, L: AnyLimit, F: FromTables + ToSql> fmt::Display for Select<D, L, F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SelectQuery[{}]", F::SQL_STR)
    }
}

impl<D: SelectedData, L: AnyLimit, F: FromTables> TableReference for Select<D, L, F> {
    type AllNullable = Select<D::AllNullable, L, F>;

    fn make_nullable(self) -> Self::AllNullable {
        Select {
            select: SelectWithoutFrom {
                data: self.select.data.make_nullable(),
                query: self.select.query,
            },
            from: self.from,
        }
    }
}

impl<D: SelectedData, L: AnyLimit, F: FromTables> FromTables for Select<D, L, F> {}

impl<DX: SelectedData + ToSql, LX: AnyLimit + ToSql, F: FromTables + ToSql> Select<DX, LX, F>
where
    Self: SelectWithCompleteFrom,
{
    // TODO: These function definitions are unreadable.
    /// Queries the SELECT query on which this function is called.
    /// That is, it generates something of the form `SELECT .. FROM (self)`.
    pub fn query<
        U: Up,
        D: SelectedData,
        L: AnyLimit,
        I: IntoPartialSelect<D, L>,
        G: FnOnce(&DX::Instantiated<Alias<U>>) -> I,
    >(
        self,
        data: G,
    ) -> Select<
        D,
        L,
        BaseTable<
            U,
            Alias<U>,
            Select<DX, LX, F>,
            <<I as IntoPartialSelect<D, L>>::Output as PartialSelect<D, L>>::From,
        >,
    >
    where
        Self: QueryTree<UpOne<U>>,
    {
        let table = DX::instantiate();
        let query = data(&table);
        query.into_partial_select().map_from(|next| BaseTable {
            table: self,
            next,
            _phantom: PhantomData,
        })
    }

    /// Left joins the SELECT query on which this function is called.
    /// That is, it generates something of the form `SELECT .. LEFT JOIN (self) ON ..`.
    pub fn left_join<
        U: Up,
        V: Value,
        D: SelectedData,
        L: AnyLimit,
        C: FnOnce(&<DX::AllNullable as SelectedData>::Instantiated<Alias<U>>) -> V,
        I: IntoPartialSelect<D, L>,
        G: FnOnce(&<DX::AllNullable as SelectedData>::Instantiated<Alias<U>>) -> I,
    >(
        self,
        condition: C,
        data: G,
    ) -> Select<
        D,
        L,
        LeftJoin<
            U,
            Alias<U>,
            <Select<DX, LX, F> as TableReference>::AllNullable,
            V,
            <<I as IntoPartialSelect<D, L>>::Output as PartialSelect<D, L>>::From,
        >,
    >
    where
        <DX as SelectedData>::AllNullable: ToSql,
        Self: QueryTree<UpOne<U>>,
    {
        let table = DX::AllNullable::instantiate();
        let query = data(&table);
        query.into_partial_select().map_from(|next| LeftJoin {
            condition: condition(&table),
            table: self.make_nullable(),
            next,
            _phantom: PhantomData,
        })
    }

    /// Inner joins the SELECT query on which this function is called.
    /// That is, it generates something of the form `SELECT .. LEFT JOIN (self) ON ..`.
    pub fn inner_join<
        U: Up,
        V: Value,
        D: SelectedData,
        L: AnyLimit,
        C: FnOnce(&DX::Instantiated<Alias<U>>) -> V,
        I: IntoPartialSelect<D, L>,
        G: FnOnce(&DX::Instantiated<Alias<U>>) -> I,
    >(
        self,
        condition: C,
        data: G,
    ) -> Select<
        D,
        L,
        InnerJoin<
            U,
            Alias<U>,
            Select<DX, LX, F>,
            V,
            <<I as IntoPartialSelect<D, L>>::Output as PartialSelect<D, L>>::From,
        >,
    >
    where
        <DX as SelectedData>::AllNullable: ToSql,
        Self: QueryTree<UpOne<U>>,
    {
        let table = DX::instantiate();
        let query = data(&table);
        query.into_partial_select().map_from(|next| InnerJoin {
            condition: condition(&table),
            table: self,
            next,
            _phantom: PhantomData,
        })
    }
}

pub trait SelectWithCompleteFrom {}

impl<D: SelectedData, L: AnyLimit, U: Up, A: TableAlias, T: TableReference, F: FromTables>
    SelectWithCompleteFrom for Select<D, L, BaseTable<U, A, T, F>>
{
}

impl<D: SelectedData, L: AnyLimit, F: FromTables> QueryRoot for Select<D, L, F> where
    Self: QueryTree<UpEnd> + SelectWithCompleteFrom + ToSql
{
}

impl<
        U: Up,
        D: SelectedData + QueryTree<U>,
        L: AnyLimit + QueryTree<D::MaxUp>,
        F: FromTables + QueryTree<L::MaxUp>,
    > QueryTree<U> for Select<D, L, F>
{
    type MaxUp = F::MaxUp;
}

impl<D: SelectedData + ToSql, L: AnyLimit + ToSql, F: FromTables + ToSql> PartialSelect<D, L>
    for Select<D, L, F>
{
    type From = F;

    fn map_from<T: FromTables, G: FnOnce(Self::From) -> T>(self, f: G) -> Select<D, L, T> {
        Select {
            select: self.select,
            from: f(self.from),
        }
    }
}

impl<D: SelectedData + ToSql, L: AnyLimit + ToSql, F: FromTables + ToSql> ToSql
    for Select<D, L, F>
{
    const SQL: ConstSqlStr = crate::sql_concat!("(SELECT ", D, F, " ", L, ")");

    fn collect_parameters(&self, f: &mut Vec<QueryValue>) {
        self.select.data.collect_parameters(f);
        self.from.collect_parameters(f);
        self.select.query.collect_parameters(f);
    }
}

pub trait SingleColumnSelectedData {
    type ColumnTy: Ty;
    type ColumnGrouping: IsGrouped;
}

impl<
        D: SingleColumnSelectedData + SelectedData + ToSql,
        L: AnyLimit + ToSql,
        F: FromTables + ToSql,
    > Value for Select<D, L, F>
{
    type Ty = <D as SingleColumnSelectedData>::ColumnTy;
    type Grouped = Undetermined;
}

/// See [super::Table::query].
#[derive(Debug, Clone)]
pub struct SelectWithoutFrom<D: SelectedData, L: AnyLimit> {
    data: D,
    query: L,
}

impl<D: SelectedData, L: AnyLimit> SelectQuery for SelectWithoutFrom<D, L> {
    type Columns = D;
    type Rows = L::Rows<D::Rows>;
    type Inverted = Self;
}

impl<D: SelectedData, L: AnyLimit> PartialSelect<D, L> for SelectWithoutFrom<D, L> {
    type From = NilTable;

    fn map_from<T: FromTables, F: FnOnce(Self::From) -> T>(self, f: F) -> Select<D, L, T> {
        Select {
            select: self,
            from: f(NilTable),
        }
    }
}

pub trait IntoPartialSelect<D: SelectedData, L: AnyLimit> {
    type Output: PartialSelect<D, L>;

    fn into_partial_select(self) -> Self::Output;
}

impl<F: FromTables + ToSql, D: SelectedData + ToSql, L: AnyLimit + ToSql> IntoPartialSelect<D, L>
    for Select<D, L, F>
{
    type Output = Self;

    fn into_partial_select(self) -> Self::Output {
        self
    }
}

impl<D: SelectedData, L: AnyLimit> IntoPartialSelect<D, L> for SelectWithoutFrom<D, L> {
    type Output = Self;

    fn into_partial_select(self) -> Self::Output {
        self
    }
}

pub struct SingleColumn<T, M> {
    value: T,
    _phantom: PhantomData<M>,
}

#[doc(hidden)]
pub struct SingleColumnFieldName;
impl FieldName for SingleColumnFieldName {
    const NAME: &'static str = "value";
}

impl<T: Fieldable, M: NullabilityModifier> SelectedData for SingleColumn<T, M>
where
    <T as Fieldable>::Grouped: GroupedToRows,
{
    type Instantiated<A: TableAlias> =
        Field<<<T as Value>::Ty as Ty>::ModifyNullability<M>, A, SingleColumnFieldName>;
    type Queried = T::Repr;
    type Repr = ();
    type Rows = <<T as Fieldable>::Grouped as GroupedToRows>::Output;
    type AllNullable = SingleColumn<T, AllNullable>;

    const NUM_COLS: usize = 1;

    fn instantiate<A: TableAlias>() -> Self::Instantiated<A> {
        Field::new()
    }

    fn make_nullable(self) -> Self::AllNullable {
        SingleColumn {
            value: self.value,
            _phantom: PhantomData,
        }
    }

    fn from_row(columns: &[QueryValue]) -> Self::Queried {
        T::from_query_value(&columns[0])
    }
}

impl<V: Value> IntoPartialSelect<SingleColumn<V, KeepOriginalNullability>, AllRows> for V
where
    V::Grouped: GroupedToRows,
{
    type Output = SelectWithoutFrom<SingleColumn<V, KeepOriginalNullability>, AllRows>;

    fn into_partial_select(self) -> Self::Output {
        select(
            SingleColumn {
                value: self,
                _phantom: PhantomData,
            },
            |_| AllRows,
        )
    }
}

impl<U: Up, T: Fieldable + QueryTree<U>, M: NullabilityModifier> QueryTree<U>
    for SingleColumn<T, M>
{
    type MaxUp = T::MaxUp;
}

impl<T: Fieldable + ToSql, M: NullabilityModifier> ToSql for SingleColumn<T, M> {
    const SQL: ConstSqlStr = sql_concat!(T, " AS `", (SingleColumnFieldName::NAME), "`");

    fn collect_parameters(&self, f: &mut Vec<QueryValue>) {
        self.value.collect_parameters(f)
    }
}

impl<V: Value + Fieldable> SelectedData for V
where
    <V as Fieldable>::Grouped: GroupedToRows,
{
    type Instantiated<A: TableAlias> = Field<V::Ty, A, SingleColumnFieldName>;
    type Queried = V::Repr;
    type Repr = ();
    type Rows = <<V as Fieldable>::Grouped as GroupedToRows>::Output;
    type AllNullable = SingleColumn<V, AllNullable>;

    const NUM_COLS: usize = 1;

    fn instantiate<A: TableAlias>() -> Self::Instantiated<A> {
        Field::new()
    }

    fn make_nullable(self) -> Self::AllNullable {
        SingleColumn {
            value: self,
            _phantom: PhantomData,
        }
    }

    fn from_row(columns: &[QueryValue]) -> Self::Queried {
        V::from_query_value(&columns[0])
    }
}

#[derive(Debug)]
pub struct BaseTable<U: Up, A: TableAlias, T: TableReference, F: FromTables> {
    table: T,
    next: F,
    _phantom: PhantomData<(U, A)>,
}

impl<U: Up, A: TableAlias, T: TableReference, F: FromTables> BaseTable<U, A, T, F> {
    pub fn new(table: T, next: F) -> Self {
        Self {
            table,
            next,
            _phantom: PhantomData,
        }
    }
}

impl<
        U: Up,
        A: TableAlias,
        T: TableReference + QueryTree<UpOne<U>>,
        F: FromTables + QueryTree<T::MaxUp>,
    > QueryTree<U> for BaseTable<U, A, T, F>
{
    type MaxUp = F::MaxUp;
}

impl<U: Up, A: TableAlias, T: TableReference, F: FromTables> FromTables for BaseTable<U, A, T, F> {}

impl<U: Up, A: TableAlias, T: TableReference + ToSql, F: FromTables + ToSql> ToSql
    for BaseTable<U, A, T, F>
{
    const SQL: ConstSqlStr = crate::sql_concat!(" FROM ", T, " AS ", (A::NAME), F);

    fn collect_parameters(&self, f: &mut Vec<QueryValue>) {
        debug!(
            "BaseTable {} with alias {} has QueryTree Id = {}",
            T::SQL_STR,
            A::NAME,
            U::NUM
        );
        self.table.collect_parameters(f);
        self.next.collect_parameters(f);
    }
}

// TODO: UNIONs
/// See [super::Table::left_join].
#[derive(Debug)]
pub struct LeftJoin<U: Up, A: TableAlias, N: TableReference, V: Value, F: FromTables> {
    condition: V,
    table: N,
    next: F,
    _phantom: PhantomData<(U, A)>,
}

impl<U: Up, A: TableAlias, N: TableReference, V: Value, F: FromTables> LeftJoin<U, A, N, V, F> {
    pub fn new(condition: V, table: N, next: F) -> Self {
        Self {
            condition,
            table,
            next,
            _phantom: PhantomData,
        }
    }
}

impl<
        U: Up,
        A: TableAlias,
        N: TableReference,
        V: Value + QueryTree<UpOne<U>>,
        F: FromTables + QueryTree<V::MaxUp>,
    > QueryTree<U> for LeftJoin<U, A, N, V, F>
{
    type MaxUp = F::MaxUp;
}

impl<U: Up, A: TableAlias, N: TableReference, V: Value, F: FromTables> FromTables
    for LeftJoin<U, A, N, V, F>
{
}

impl<U: Up, A: TableAlias, N: TableReference + ToSql, V: Value + ToSql, F: FromTables + ToSql> ToSql
    for LeftJoin<U, A, N, V, F>
{
    const SQL: ConstSqlStr = crate::sql_concat!(" LEFT JOIN ", N, " AS ", (A::NAME), " ON ", V, F);

    fn collect_parameters(&self, f: &mut Vec<QueryValue>) {
        debug!(
            "LEFT JOIN with alias {} has QueryTree Id = {}",
            A::NAME,
            U::NUM
        );
        self.table.collect_parameters(f);
        self.condition.collect_parameters(f);
        self.next.collect_parameters(f);
    }
}

/// See [super::Table::inner_join].
#[derive(Debug)]
pub struct InnerJoin<U: Up, A: TableAlias, N: TableReference, V: Value, F: FromTables> {
    condition: V,
    table: N,
    next: F,
    _phantom: PhantomData<(U, A, N)>,
}

impl<U: Up, A: TableAlias, N: TableReference, V: Value, F: FromTables> InnerJoin<U, A, N, V, F> {
    pub fn new(condition: V, table: N, next: F) -> Self {
        Self {
            condition,
            table,
            next,
            _phantom: PhantomData,
        }
    }
}

impl<
        U: Up,
        A: TableAlias,
        N: TableReference,
        V: Value + QueryTree<UpOne<U>>,
        F: FromTables + QueryTree<V::MaxUp>,
    > QueryTree<U> for InnerJoin<U, A, N, V, F>
{
    type MaxUp = F::MaxUp;
}

impl<U: Up, A: TableAlias, N: TableReference, V: Value, F: FromTables> FromTables
    for InnerJoin<U, A, N, V, F>
{
}

impl<U: Up, A: TableAlias, N: TableReference + ToSql, V: Value + ToSql, F: FromTables + ToSql> ToSql
    for InnerJoin<U, A, N, V, F>
{
    const SQL: ConstSqlStr = crate::sql_concat!(" INNER JOIN ", N, " AS ", (A::NAME), " ON ", V, F);

    fn collect_parameters(&self, f: &mut Vec<QueryValue>) {
        debug!(
            "INNER JOIN with alias {} has QueryTree Id = {}",
            A::NAME,
            U::NUM
        );
        self.table.collect_parameters(f);
        self.condition.collect_parameters(f);
        self.next.collect_parameters(f);
    }
}

pub trait GroupedToRows {
    type Output: RowKind;
}

impl GroupedToRows for Ungrouped {
    type Output = ZeroOrMore;
}

impl GroupedToRows for Grouped {
    type Output = ExactlyOne;
}

impl GroupedToRows for Undetermined {
    type Output = ZeroOrMore;
}

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
    pub fn test_select() {
        assert_eq!(ground(Foo::query(|t|
            select(data! {
                _s: t.name,
                _n: t.value,
                _c: ConstI64::<5>,
            }, |_| CmpEq(ConstI64::<6>, t.id)
            .group_by(t.name)
            .limit_offset::<5, 7>()
        ))).sql_str(), "(SELECT `t0`.`Name` AS `_s`, `t0`.`Value` AS `_n`, 5 AS `_c` FROM `Foo` AS t0 WHERE (6 = `t0`.`Id`) GROUP BY `t0`.`Name` LIMIT 5, 7)");
    }
}
