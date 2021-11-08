#![allow(incomplete_features)]
#![recursion_limit = "256"]
// Incomplete features
#![feature(adt_const_params, generic_const_exprs)]
// Other features
#![feature(
    generic_associated_types,
    associated_type_defaults,
    const_ptr_offset,
    const_slice_from_raw_parts,
    const_mut_refs,
    const_raw_ptr_deref,
    const_fn_trait_bound
)]

//! `typed-qb` is a compile-time, typed, query builder.
//! The query is transformed into an SQL query string at compile time.
//! If code compiles and the schema in the code matches the database, it should be (*almost*) impossible to write queries that produce errors.
//!
//! Make sure to enable the `generic_associated_types` feature and include the prelude:
//! ```rust
//! #![feature(generic_associated_types)]
//! use typed_qb::prelude::*;
//! ```
//!
//! Use the [tables](tables) macro to generate table definitions:
//! ```rust
//! # #![feature(generic_associated_types)]
//! typed_qb::tables! {
//!     CREATE TABLE Users (
//!         Id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT,
//!         Name VARCHAR(64) NOT NULL,
//!         PRIMARY KEY(Id)
//!     );
//! }
//! ```
//! This will generate a table definition `Users` implementing [Table](Table).
//! To construct queries, call the methods on this trait:
//! ```rust
//! # #![feature(generic_associated_types)]
//! # use typed_qb::__doctest::*;
//! let query = Users::query(|user| data! {
//!     id: user.id,
//!     name: user.name,
//! });
//! # ground(query);
//! ```
//! Pass the query to [Database::typed_query](mysql::Database::typed_query) to execute the query:
//! ```rust,no_run
//! # #![feature(generic_associated_types)] use typed_qb::__doctest::*;
//! # let query = Users::query(|user| expr!(COUNT(*)));
//! let opts = mysql::OptsBuilder::new()
//!     .user(Some("..."))
//!     .pass(Some("..."))
//!     .db_name(Some("..."));
//! let pool = mysql::Pool::new(opts)?;
//!
//! let mut conn = pool.get_conn()?;
//! let results = conn.typed_query(query)?;
//! # Ok::<(), mysql::Error>(())
//! ```

pub mod expr;
pub mod functions;
pub mod mysql;
pub mod qualifiers;
pub mod typing;

pub mod delete;
pub mod insert;
pub mod select;
pub mod update;

pub mod prelude {
    pub use crate::mysql::Database;
    pub use crate::qualifiers::{
        AllRows, AnyGroupedBy, AnyHaving, AnyLimit, AnyOrderedBy, AnyWhere, AsWhere,
        CreateOrderByEntry, GroupBySeq, OrderBySeq, Where,
    };
    pub use crate::select::select;
    pub use crate::{data, expr, set, values, QueryInto, Table};
}

use delete::{Delete, DeleteQualifiers};
use insert::{Insert, ValueList};
// re-export the macros
pub use typed_qb_procmacro::*;

use expr::Value;
use qualifiers::AnyLimit;
use select::{
    BaseTable, InnerJoin, IntoPartialSelect, LeftJoin, PartialSelect, Select, SelectedData,
};
use std::{fmt, marker::PhantomData};
use typing::{BaseTy, IsGrouped, IsNullable, Ty, Ungrouped};
use update::{SetList, Update, UpdateQualifiers};

pub use __private::ConstSqlStr;

#[doc(hidden)]
pub mod __private {
    pub use concat_idents::concat_idents;
    pub use std::fmt::Debug;
    pub use std::marker::PhantomData;

    const BUFFER_SIZE: usize = 8192;

    pub struct ConstSqlStr {
        data: [u8; BUFFER_SIZE],
        len: usize,
    }

    impl ConstSqlStr {
        pub const fn empty() -> ConstSqlStr {
            ConstSqlStr {
                data: [0u8; BUFFER_SIZE],
                len: 0,
            }
        }

        pub const fn new(s: &'static str) -> ConstSqlStr {
            Self::empty().append_str(s)
        }

        pub const fn len(&self) -> usize {
            self.len
        }

        pub const fn append_str(mut self, s: &'static str) -> Self {
            let b = s.as_bytes();
            let mut index = 0;
            loop {
                if index >= b.len() {
                    break;
                } else {
                    self.data[self.len] = b[index];
                    self.len += 1;
                    index += 1;
                }
            }

            self
        }

        /// Appends a string where 's are replaced with \' and \ are replaced with \\
        pub const fn append_quoted_str(mut self, s: &'static str) -> Self {
            // TODO: How important is security for hardcoded strings in the source code? An end-user will never be able to control these values, so in theory it's not a big deal. Are there any funny edge-cases that we should take into account anyways?

            self = self.append_str("'");

            let b = s.as_bytes();
            let mut index = 0;
            loop {
                if index >= b.len() {
                    break;
                } else {
                    const QUOTE: u8 = '\'' as u8;
                    const BACKSLASH: u8 = '\\' as u8;
                    match b[index] {
                        QUOTE => self = self.append_str("\\'"),
                        BACKSLASH => self = self.append_str("\\\\"),
                        b => {
                            self.data[self.len] = b;
                            self.len += 1;
                            index += 1;
                        }
                    }
                }
            }

            self.append_str("'")
        }

        pub const fn append_const_str(mut self, s: ConstSqlStr) -> Self {
            let b = &s.data;
            let mut index = 0;
            loop {
                if index >= s.len {
                    break;
                } else {
                    self.data[self.len] = b[index];
                    self.len += 1;
                    index += 1;
                }
            }

            self
        }

        pub const fn append_i64(self, v: i64) -> Self {
            let mut target = [0u8; 32];
            let mut index = target.len() - 1;

            if v == 0 {
                return self.append_str("0");
            }

            let sign = v < 0;
            let mut v = v;
            loop {
                if v == 0 {
                    break;
                }

                target[index] = match (v % 10).abs() {
                    0 => '0',
                    1 => '1',
                    2 => '2',
                    3 => '3',
                    4 => '4',
                    5 => '5',
                    6 => '6',
                    7 => '7',
                    8 => '8',
                    9 => '9',
                    _ => unreachable!(),
                } as u8;

                index -= 1;
                v /= 10;
            }

            if sign {
                target[index] = '-' as u8;
                index -= 1;
            }

            index += 1;
            self.append_str(resize_and_strify(&target, index, target.len() - index))
        }

        pub const fn append_u64(self, v: u64) -> Self {
            let mut target = [0u8; 32];
            let mut index = target.len() - 1;

            if v == 0 {
                return self.append_str("0");
            }

            let mut v = v;
            loop {
                if v == 0 {
                    break;
                }

                target[index] = match v % 10 {
                    0 => '0',
                    1 => '1',
                    2 => '2',
                    3 => '3',
                    4 => '4',
                    5 => '5',
                    6 => '6',
                    7 => '7',
                    8 => '8',
                    9 => '9',
                    _ => unreachable!(),
                } as u8;

                index -= 1;
                v /= 10;
            }

            index += 1;
            self.append_str(resize_and_strify(&target, index, target.len() - index))
        }

        pub const fn append_usize(self, v: usize) -> Self {
            let mut target = [0u8; 32];
            let mut index = target.len() - 1;

            if v == 0 {
                return self.append_str("0");
            }

            let mut v = v;
            loop {
                if v == 0 {
                    break;
                }

                target[index] = match v % 10 {
                    0 => '0',
                    1 => '1',
                    2 => '2',
                    3 => '3',
                    4 => '4',
                    5 => '5',
                    6 => '6',
                    7 => '7',
                    8 => '8',
                    9 => '9',
                    _ => unreachable!(),
                } as u8;

                index -= 1;
                v /= 10;
            }

            index += 1;
            self.append_str(resize_and_strify(&target, index, target.len() - index))
        }

        pub const fn as_str(&self) -> &'static str {
            resize_and_strify(&self.data, 0, self.len)
        }
    }

    pub const fn copy_into(target: &mut [u8], left: &'static str, offset: usize) -> usize {
        let mut index = offset;
        let left = left.as_bytes();

        let mut p = 0;
        loop {
            if p >= left.len() {
                break;
            } else {
                target[index] = left[p];
                index += 1;
                p += 1;
            }
        }

        index
    }

    pub const fn resize_and_strify(data: &[u8], offset: usize, len: usize) -> &'static str {
        if offset + len > data.len() {
            panic!("Tried to slice out of bounds")
        }

        unsafe {
            let ptr = data.as_ptr().add(offset);
            let slice = std::ptr::slice_from_raw_parts(ptr, len);
            let slice: &[u8] = &*slice as &[u8];

            std::mem::transmute(slice)
        }
    }

    #[macro_export]
    macro_rules! sql_concat {
        (@len $a:literal $(, $($rest:tt),*)?) => {
            $a.as_bytes().len() $(+ $crate::sql_concat!(@len $($rest),*))?
        };
        (@len $a:ident $(, $($rest:tt),*)?) => {
            $a::SQL_LEN $(+ $crate::sql_concat!(@len $($rest),*))?
        };
        (@len) => { 0 };

        ($target:expr; $a:literal) => {
            $target.append_str($a)
        };
        ($target:expr; [$a:ty]) => {
            $target.append_const_str(<$a>::SQL)
        };
        ($target:expr; $a:ident) => {
            $target.append_const_str($a::SQL)
        };
        ($target:expr; ($a:expr)) => {
            $target.append_str($a)
        };

        ($target:expr; $a:literal, $($rest:tt),*) => {{
            $crate::sql_concat!($target.append_str($a); $($rest),*)
        }};
        ($target:expr; [$a:ty], $($rest:tt),*) => {{
            $crate::sql_concat!($target.append_const_str(<$a>::SQL); $($rest),*)
        }};
        ($target:expr; $a:ident, $($rest:tt),*) => {{
            $crate::sql_concat!($target.append_const_str($a::SQL); $($rest),*)
        }};
        ($target:expr; ($a:expr), $($rest:tt),*) => {{
            $crate::sql_concat!($target.append_str($a); $($rest),*)
        }};

        ($($rest:tt),*) => {{
            let target = $crate::__private::ConstSqlStr::empty();
            $crate::sql_concat!(target; $($rest),*)
        }};
    }

    // #[cfg(test)]
    mod tests {
        use super::ConstSqlStr;

        pub trait MiniSql {
            const SQL: ConstSqlStr;
            const SQL_LEN: usize = Self::SQL.len();
        }

        struct Test;

        impl MiniSql for Test {
            const SQL: ConstSqlStr = ConstSqlStr::new("Test");
        }

        struct Chain<A: MiniSql, B: MiniSql>(A, B);
        impl<A: MiniSql, B: MiniSql> MiniSql for Chain<A, B> {
            const SQL: ConstSqlStr = sql_concat!("(", A, ", ", B, ")");
        }

        #[test]
        pub fn test_minisql() {
            assert_eq!(
                Chain::<Chain::<Test, Test>, Test>::SQL.as_str(),
                "((Test, Test), Test)"
            );
        }

        #[test]
        pub fn test_concat() {
            assert_eq!(sql_concat!("abc", "def").as_str(), "abcdef");
            assert_eq!(sql_concat!("abc", "def", "ghi").as_str(), "abcdefghi");
            assert_eq!(
                sql_concat!("abc", "def", "ghi", "jkl").as_str(),
                "abcdefghijkl"
            );
            assert_eq!(
                sql_concat!("abc", Test, "ghi", "jkl").as_str(),
                "abcTestghijkl"
            );
        }

        #[test]
        pub fn test_strify_i64() {
            for n in -100000..100000 {
                assert_eq!(
                    ConstSqlStr::empty().append_i64(n).as_str(),
                    format!("{}", n).as_str()
                );
            }

            assert_eq!(
                ConstSqlStr::empty().append_i64(i64::MAX).as_str(),
                format!("{}", i64::MAX).as_str()
            );
            assert_eq!(
                ConstSqlStr::empty().append_i64(i64::MIN).as_str(),
                format!("{}", i64::MIN).as_str()
            );
        }
    }
}
pub trait QueryRoot: QueryTree<UpEnd> + ToSql {}

pub trait Up {
    const NUM: usize;
}

#[derive(Copy, Clone, Debug, Default)]
pub struct UpOne<U: Up> {
    _u: U,
}

#[derive(Copy, Clone, Debug, Default)]
pub struct UpEnd;

impl<U: Up> Up for UpOne<U> {
    const NUM: usize = U::NUM + 1;
}

impl Up for UpEnd {
    const NUM: usize = 0;
}

pub trait QueryTree<U: Up> {
    type MaxUp: Up;
}

pub trait TableAlias {
    const PREFIX: &'static str;
    const NAME: &'static str;
}

#[doc(hidden)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Alias<U: Up>(U);

impl<U: Up> Alias<U> {
    const SQL_NAME: ConstSqlStr = ConstSqlStr::new("t").append_usize(U::NUM);
    const SQL_PREFIX: ConstSqlStr = ConstSqlStr::new("`t").append_usize(U::NUM).append_str("`.");
}

impl<U: Up> TableAlias for Alias<U> {
    const PREFIX: &'static str = Self::SQL_PREFIX.as_str();
    const NAME: &'static str = Self::SQL_NAME.as_str();
}

impl TableAlias for () {
    const PREFIX: &'static str = "";
    const NAME: &'static str = panic!("() cannot be used for froms or joins");
}

pub trait FieldName {
    const NAME: &'static str;
}

#[derive(Debug)]
pub struct Field<T: Ty, A, N: FieldName> {
    _phantom: PhantomData<(T, A, N)>,
}

impl<U: Up, T: Ty, A: TableAlias, N: FieldName> QueryTree<U> for Field<T, A, N> {
    type MaxUp = U;
}

impl<T: Ty, A, N: FieldName> Copy for Field<T, A, N> {}
impl<T: Ty, A, N: FieldName> Clone for Field<T, A, N> {
    fn clone(&self) -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<T: Ty, A: TableAlias, N: FieldName> Field<T, A, N> {
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<T: Ty, A: TableAlias, N: FieldName> ToSql for Field<T, A, N> {
    const SQL: ConstSqlStr = sql_concat!((A::PREFIX), "`", (N::NAME), "`");

    fn collect_parameters(&self, _params: &mut Vec<QueryValue>) {}
}

/// Represents a table. All fields of the table can be accessed via struct fields.
/// You should **not** implement this trait.
/// `TableReference`s are passed to the closures provided to the various methods on the [`Table`] trait.
pub trait TableReference {
    type AllNullable: TableReference;

    fn make_nullable(self) -> Self::AllNullable;
}

pub trait Table: Sized {
    type WithAlias<A: TableAlias>: TableReference;

    fn new<A: TableAlias>() -> Self::WithAlias<A>;

    // TODO: Can we implement some kind of trait to allow IntoPartialSelect to be passed directy into this function?
    /// `SELECT` data from the table.
    ///
    /// See also [`data`] and [`select`](select::select).
    ///
    /// ```rust
    /// # #![feature(generic_associated_types)]
    /// # use typed_qb::__doctest::*;
    /// # let mut conn = FakeConn;
    /// let results = conn.typed_query(Users::query(|user| {
    ///     data! {
    ///         id: user.id,
    ///         username: user.name,
    ///     }
    /// }))?;
    /// # Ok::<(), mysql::Error>(())
    /// ```
    fn query<
        U: Up,
        D: SelectedData,
        L: AnyLimit,
        I: IntoPartialSelect<D, L>,
        G: FnOnce(&Self::WithAlias<Alias<U>>) -> I,
    >(
        data: G,
    ) -> Select<
        D,
        L,
        BaseTable<
            U,
            Alias<U>,
            Self::WithAlias<Alias<U>>,
            <<I as IntoPartialSelect<D, L>>::Output as PartialSelect<D, L>>::From,
        >,
    > {
        let table = Self::new();
        let query = data(&table);
        query
            .into_partial_select()
            .map_from(|next| BaseTable::new(table, next))
    }

    /// `LEFT JOIN` the table.
    ///
    /// `condition` takes a [TableReference](TableReference) to the newly joined data, and expects a boolean expression as a return value.
    /// The condition is what you would normally write for the `ON ...` part of a `JOIN`.
    /// Usually this is something like `expr!(foo_table.id = bar_table.foo_id)`.
    ///
    /// `data` takes a [TableReference](TableReference) to the newly joined data, and expects the rest of the query as a return value.
    ///
    /// ```rust
    /// # #![feature(generic_associated_types)]
    /// # use typed_qb::__doctest::*;
    /// # let mut conn = FakeConn;
    /// let results = conn.typed_query(Users::query(|user|
    ///     Questions::left_join(
    ///         |question| expr!(question.asked_by_id = user.id),
    ///         |question| select(data! {
    ///             id: user.id,
    ///             username: user.name,
    ///             num_questions: [COUNT(*)],
    ///         }, |_| AllRows.group_by(user.id))
    ///     )
    /// ))?;
    /// # Ok::<(), mysql::Error>(())
    /// ```
    fn left_join<
        U: Up,
        V: Value,
        D: SelectedData,
        L: AnyLimit,
        I: IntoPartialSelect<D, L>,
        C: FnOnce(&<Self::WithAlias<Alias<U>> as TableReference>::AllNullable) -> V,
        G: FnOnce(&<Self::WithAlias<Alias<U>> as TableReference>::AllNullable) -> I,
    >(
        condition: C,
        data: G,
    ) -> Select<
        D,
        L,
        LeftJoin<
            U,
            Alias<U>,
            <Self::WithAlias<Alias<U>> as TableReference>::AllNullable,
            V,
            <<I as IntoPartialSelect<D, L>>::Output as PartialSelect<D, L>>::From,
        >,
    > {
        let table = Self::new().make_nullable();
        let query = data(&table);
        query
            .into_partial_select()
            .map_from(|next| LeftJoin::new(condition(&table), table, next))
    }

    /// `INNER JOIN` the table.
    ///
    /// See [Table::left_join]
    fn inner_join<
        U: Up,
        V: Value,
        D: SelectedData,
        L: AnyLimit,
        I: IntoPartialSelect<D, L>,
        C: FnOnce(&Self::WithAlias<Alias<U>>) -> V,
        G: FnOnce(&Self::WithAlias<Alias<U>>) -> I,
    >(
        condition: C,
        data: G,
    ) -> Select<
        D,
        L,
        InnerJoin<
            U,
            Alias<U>,
            Self::WithAlias<Alias<U>>,
            V,
            <<I as IntoPartialSelect<D, L>>::Output as PartialSelect<D, L>>::From,
        >,
    > {
        let table = Self::new();
        let query = data(&table);
        query
            .into_partial_select()
            .map_from(|next| InnerJoin::new(condition(&table), table, next))
    }

    /// `UPDATE` rows of the table.
    fn update<
        S: SetList,
        F: FnOnce(&Self::WithAlias<()>) -> S,
        L: UpdateQualifiers,
        C: FnOnce(&Self::WithAlias<()>) -> L,
    >(
        list: F,
        chain: C,
    ) -> Update<Self, S, L> {
        let table = Self::new();
        Update {
            sets: list(&table),
            qualifiers: chain(&table),
            _phantom: PhantomData,
        }
    }

    /// `INSERT` new rows into the table.
    fn insert<S: ValueList, F: FnOnce(&Self::WithAlias<()>) -> S>(list: F) -> Insert<Self, S> {
        let table = Self::new();
        Insert {
            values: list(&table),
            _phantom: PhantomData,
        }
    }

    /// `DELETE FROM` the table.
    fn delete<L: DeleteQualifiers, F: FnOnce(&Self::WithAlias<()>) -> L>(
        list: F,
    ) -> Delete<Self, L> {
        let table = Self::new();
        Delete {
            qualifiers: list(&table),
            _phantom: PhantomData,
        }
    }
}

impl<T: Ty, A: TableAlias, N: FieldName> Value for Field<T, A, N> {
    type Ty = T;
    type Grouped = Ungrouped;
}

#[derive(Clone, Debug, PartialEq)]
pub enum QueryValue {
    String(String),
    Bytes(Vec<u8>),
    I64(i64),
    U64(u64),
    F32(f32),
    F64(f64),
    DateTime(DateTime),
    Time(Time),
    Null,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Time {
    pub is_negative: bool,
    pub days: u32,
    pub hours: u8,
    pub minutes: u8,
    pub seconds: u8,
    pub micro_seconds: u32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DateTime {
    pub year: u16,
    pub month: u8,
    pub day: u8,

    pub hour: u8,
    pub minutes: u8,
    pub seconds: u8,

    pub micro_seconds: u32,
}

pub trait Fieldable: Value {
    type Grouped: IsGrouped = <Self as Value>::Grouped;
    type Repr;

    fn from_query_value(value: &QueryValue) -> Self::Repr;
}

impl<T: Ty, V: Value<Ty = T>> Fieldable for V {
    type Repr = <T::Nullable as IsNullable>::Repr<<T::Base as BaseTy>::Repr>;

    fn from_query_value(value: &QueryValue) -> Self::Repr {
        <T::Nullable as IsNullable>::parse::<T::Base>(value)
    }
}

impl fmt::Display for DateTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{}",
            self.year,
            self.month,
            self.day,
            self.hour,
            self.minutes,
            self.seconds,
            self.micro_seconds
        )
    }
}

#[macro_export]
#[doc(hidden)]
macro_rules! count {
    ($v:tt, $($tts:tt),*) => {
        1 + $crate::count!($($tts),*)
    };
    ($v:tt) => { 1 };
    () => { 0 }
}

/// Generates a [`SelectedData`] anonymous struct from a list of expressions.
/// Each expression must be separated by a comma (,).
/// Fields can be specified in a `struct`-like syntax:
/// ```rust
/// # #![feature(generic_associated_types)]
/// # use typed_qb::prelude::*;
/// # #[derive(Default)]
/// # struct Table { id: (), name: (), }
/// # let table = Table::default();
/// let data = data! {
///     a: table.id,
///     b: table.name,
/// };
/// ```
///
/// An expression of the form `a.b` is shorthand for `b: a.b`:
///
/// ```rust
/// # #![feature(generic_associated_types)]
/// # use typed_qb::prelude::*;
/// # #[derive(Default)]
/// # struct Table { id: (), name: (), }
/// # let table = Table::default();
/// let data = data! {
///     table.id,
///     table.name,
/// };
/// // data contains two fields: id and name
/// ```
///
/// `[...]` can be used to include a SQL expression via [expr!]:
///
/// ```rust
/// # #![feature(generic_associated_types)]
/// # use typed_qb::prelude::*;
/// # #[derive(Default)]
/// # struct Table { id: (), name: (), }
/// # let table = Table::default();
/// let data = data! {
///     a: table.id,
///     b: [COUNT(*)], // equivalent to: expr!(COUNT(*))
/// };
/// ```
///
/// When a single expression is provided, it is automatically named '_value':
///
/// ```rust
/// # #![feature(generic_associated_types)]
/// # use typed_qb::prelude::*;
/// # #[derive(Default)]
/// # struct Table { id: (), name: (), }
/// # let table = Table::default();
/// let data = data! {
///     [COUNT(*)]
/// };
/// // data now contains a single field `data._value`
/// ```
///
/// [`SelectedData`] is automatically derived for the anonymous struct.
/// You can also load the data into an existing struct:
///
/// ```rust
/// # #![feature(generic_associated_types)]
/// # use typed_qb::prelude::*;
/// # #[derive(Default)]
/// # struct Table { id: u32, name: String, }
/// # let table = Table::default();
///
/// #[derive(QueryInto)]
/// struct MyData {
///     id: u32,
///     name: String,
/// }
/// let data = data! { as MyData:
///     table.id,
///     table.name,
/// };
/// ```
///
/// If you are selecting multiple columns, you must derive [`QueryInto`] for the struct.
/// The struct must contain all the fields that are being selected.
/// An implementation `FieldType: From<SelectedType>` must exist.
///
/// If you are selecting a single column, you must implement `From<SelectedType>` for the struct.
///
/// You do not need to implement any other traits.
/// Depending on the query, [`Database::typed_query`] will either return a `T`, `Option<T>` or an `Iterator<Item = T>` where `T` is the custom struct.
///
#[macro_export]
macro_rules! data {
    (genquerytree @ $($key:ident),*) => {
        $crate::data!(genquerytree:zip @ $($key),*; U, $(<$key>::MaxUp),* => );
    };
    (genquerytree:zip @ $firsta:ident $(, $($a:ident),*)?; $firstb:ty $(, $($b:ty),*)? => $($out:tt,)*) => {
        $crate::data!(genquerytree:zip @ $($($a),*)?; $($($b),*)? => $($out,)* [$firsta: $firstb],);
    };
    (genquerytree:zip @ ; $final:ty => $($out:tt,)*) => {
        $crate::data!(genquerytree:full @ $($out,)* => $final);
    };
    (genquerytree:full @ $([ $key:ident : $constraint:ty ],)* => $final:ty) => {
        #[allow(non_camel_case_types)]
        impl<U: $crate::Up, $($key,)* M: $crate::typing::NullabilityModifier> $crate::QueryTree<U> for AnonymousData<$($key),*, M>
            where $($key : $crate::QueryTree<$constraint>,)* {
            type MaxUp = $final;
        }
    };

    (genfieldtypes @ { } { $($key2:ident)* }) => {};
    (genfieldtypes @ { $key:ident $($rest:ident)* } { $($key2:ident)* }) => {
        #[allow(non_camel_case_types)]
        impl<$($key2),*> $crate::FieldType<{ stringify!($key) }> for AnonymousDataQueried<$($key2),*> {
            type Ty = $key;
        }

        $crate::data!(genfieldtypes @ { $($rest)* } { $($key2)* });
    };

    (output @ { } $($key:ident : $value:expr),* $(,)*) => {
        {
            #[allow(non_camel_case_types)]
            #[derive(Clone)]
            struct AnonymousDataQueried<$($key),*> {
                $(pub $key: $key),*
            }

            $crate::data!(genfieldtypes @ { $($key)* } { $($key)* });

            #[allow(non_camel_case_types)]
            impl<$($key),*> AnonymousDataQueried<$($key),*> {
                fn __create_from_row<D: $($crate::WithField<{ stringify!($key) }, Output = $key> + )*>(data: D) -> Self {
                    Self {
                        $($key: <D as $crate::WithField<{ stringify!($key) }>>::value(&data),)*
                    }
                }
            }

            #[allow(non_camel_case_types)]
            impl<$($key),*> $crate::__private::Debug for AnonymousDataQueried<$($key),*>
                where $($key: $crate::__private::Debug),* {
                fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> std::fmt::Result {
                    f.debug_struct("$")
                        $(.field(stringify!($key), &self.$key))*
                        .finish()
                }
            }

            let result;
            $crate::data!(finaloutput in result @ { default AnonymousDataQueried::<$($key::Repr),*> } $($key: $value,)*);

            result
        }
    };

    (output @ { $ty:ty } $($key:ident : $value:expr),* $(,)*) => {
        {
            let result;
            $crate::data!(finaloutput in result @ { custom $ty } $($key: $value,)*);

            result
        }
    };

    (fromrow @ custom { $ty:ty } { $key:ident } { $intermediate:ty }) => {
        #[allow(non_camel_case_types)]
        impl<$key: $crate::Fieldable, M: $crate::typing::NullabilityModifier> $crate::select::FromRow for AnonymousData<$key, M>
            where Self: $crate::select::SelectedData,
                $ty: From<$key::Repr> {
            type Queried = $ty;
            fn from_row(columns: &[$crate::QueryValue]) -> Self::Queried {
                $key::from_query_value(&columns[0]).into()
            }
        }
    };

    (fromrow @ custom { $ty:ty } { $($key:ident),* } { $intermediate:ty }) => {
        #[allow(non_camel_case_types)]
        impl<$($key: $crate::Fieldable),*, M: $crate::typing::NullabilityModifier> $crate::select::FromRow for AnonymousData<$($key),*, M>
            where Self: $crate::select::SelectedData,
                $(<$ty as $crate::FieldType<{ stringify!($key) }>>::Ty: From<$key::Repr>,)*
                {
            type Queried = $ty;
            #[allow(unused_assignments)]
            fn from_row<'a>(columns: &'a [$crate::QueryValue]) -> Self::Queried {
                <$ty>::__create_from_row(Intermediate::<'a, $($key,)*>(columns, $crate::__private::PhantomData))
            }
        }

        #[allow(non_camel_case_types)]
        impl<$($key: $crate::Fieldable),*, M: $crate::typing::NullabilityModifier> $crate::select::IntoPartialSelect<AnonymousData<$($key),*, M>, AllRows> for AnonymousData<$($key),*, M>
            where Self: $crate::select::SelectedData, {
            type Output = $crate::select::SelectWithoutFrom<Self, AllRows>;

            fn into_partial_select(self) -> Self::Output {
                select(self, |_| AllRows)
            }
        }
    };

    (fromrow @ default { $ty:ty } { $key:ident } { $intermediate:ty }) => {
        #[allow(non_camel_case_types)]
        impl<$key: $crate::Fieldable, M: $crate::typing::NullabilityModifier> $crate::select::FromRow for AnonymousData<$key, M>
            where Self: $crate::select::SelectedData {
            type Queried = $key::Repr;
            fn from_row(columns: &[$crate::QueryValue]) -> Self::Queried {
                $key::from_query_value(&columns[0])
            }
        }
    };

    (fromrow @ default { $ty:ty } { $($key:ident),* } { $intermediate:ty }) => {
        #[allow(non_camel_case_types)]
        impl<$($key: $crate::Fieldable),*, M: $crate::typing::NullabilityModifier> $crate::select::FromRow for AnonymousData<$($key),*, M>
            where Self: $crate::select::SelectedData,
                $(for<'a> $intermediate: $crate::WithField<{ stringify!($key) }, Output = $key::Repr>,)*
                {
            type Queried = $ty;
            #[allow(unused_assignments)]
            fn from_row<'a>(columns: &'a [$crate::QueryValue]) -> Self::Queried {
                <$ty>::__create_from_row(Intermediate::<'a, $($key,)*>(columns, $crate::__private::PhantomData))
            }
        }

        #[allow(non_camel_case_types)]
        impl<$($key: $crate::Fieldable),*, M: $crate::typing::NullabilityModifier> $crate::select::IntoPartialSelect<AnonymousData<$($key),*, M>, AllRows> for AnonymousData<$($key),*, M>
            where Self: $crate::select::SelectedData, {
            type Output = $crate::select::SelectWithoutFrom<Self, AllRows>;

            fn into_partial_select(self) -> Self::Output {
                select(self, |_| AllRows)
            }
        }
    };

    (finaloutput in $result:ident @ { $tykind:ident $ty:ty } $($key:ident : $value:expr,)*) => {
        $(
            $crate::__private::concat_idents!( fieldname = $key, FieldName {
                #[allow(non_camel_case_types)]
                struct fieldname;

                impl $crate::FieldName for fieldname {
                    const NAME: &'static str = stringify!($key);
                }
            });
        )*

        #[allow(non_camel_case_types)]
        #[derive(Debug, Clone)]
        struct AnonymousData<$($key),*, M: $crate::typing::NullabilityModifier> {
            $($key: $key,)*
            _phantom: $crate::__private::PhantomData<M>,
        }

        #[allow(non_camel_case_types)]
        struct AnonymousDataInst<$($key: $crate::Fieldable),*, A: $crate::TableAlias, M: $crate::typing::NullabilityModifier> {
            $(pub $key: $crate::Field<<<$key as $crate::expr::Value>::Ty as $crate::typing::Ty>::ModifyNullability<M>, A, $crate::__private::concat_idents!( fieldname = $key, FieldName {
                fieldname
            })>),*
        }

        $crate::data!(genquerytree @ $($key),*);

        $crate::_internal_impl_selected_data!({ $ty } $($key,)*);
        $crate::_internal_select_derive_to_sql!($($key: $value),*);

        $crate::data!(fromrow @ $tykind { $ty } { $($key),* } { Intermediate::<'a, $($key),*> });

        #[allow(non_camel_case_types)]
        fn cast<$($key),*>(o: AnonymousData<$($key),*, $crate::typing::KeepOriginalNullability>) -> AnonymousData<$($key),*, $crate::typing::KeepOriginalNullability> {
            o
        }

        $result = cast(AnonymousData {
            $($key: $value,)*
            _phantom: $crate::__private::PhantomData,
        });
    };

    (preprocess @ { $($ty:tt)* } { $(,)* } => { $($key:ident: $value:expr,)* }) => {
        $crate::data! { output @ { $($ty)* } $($key : $value),* }
    };
    (preprocess @ { $($ty:tt)* } { $(,)* } => { $($rest:tt)* }) => {
        $crate::data!(preprocess @ { $($ty)* } { => $crate::qualifiers::AllRows } => { $($rest)* })
    };

    // expressions surrounded by [] are interpreted via expr!()
    (preprocess @ { $($ty:tt)* } { $(,)* $key:ident: [$($tt:tt)*]$(, $($rest:tt)*)? } => { $($unfolded:tt)* }) => {
        $crate::data!(preprocess @ { $($ty)* } { $($($rest)*)? } => { $($unfolded)* $key: $crate::expr!($($tt)*), })
    };

    // The following cases are "key: expr" followed by ", $rest" or "", since there is no or operator in macro_rules!
    (preprocess @ { $($ty:tt)* } { $(,)* $key:ident: $value:expr$(, $($rest:tt)*)? } => { $($unfolded:tt)* }) => {
        $crate::data!(preprocess @ { $($ty)* } { $($($rest)*)? } => { $($unfolded)* $key: $value, })
    };
    // shorthand 'a.b' expands to 'b: a.b'
    (preprocess @ { $($ty:tt)* } { $(,)* $a:ident.$b:ident $($rest:tt)* } => { $($unfolded:tt)* }) => {
        $crate::data!(preprocess @ { $($ty)* } { $($rest)* } => { $($unfolded)* $b: $a.$b, })
    };

    // a single [...] expands to _value: expr!(...)
    (preprocess @ { $($ty:tt)* } { $(,)* [$($tt:tt)*] } => { $($unfolded:tt)* }) => {
        $crate::data!(preprocess @ { $($ty)* } { } => { $($unfolded)* _value: $crate::expr!($($tt)*), })
    };
    // just a single expression $e expands to '_value: $e'
    (preprocess @ { $($ty:tt)* } { $(,)* $e:expr } => { $($unfolded:tt)* }) => {
        $crate::data!(preprocess @ { $($ty)* } { } => { $($unfolded)* _value: $e, })
    };

    (preprocess @ $($any:tt)*) => {
        compile_error!(concat!("unfolding of data! is missing a rule for ", stringify!($($any)*)))
    };

    // Entry point
    ( as $ty:ty: $($rest:tt)*) => {
        $crate::data!{ preprocess @ { $ty } { $($rest)* } => { } }
    };
    ($($rest:tt)*) => {
        $crate::data!{ preprocess @ { } { $($rest)* } => { } }
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! _internal_impl_selected_data {
    ({ $ty:ty } $key:ident,) => {
        #[allow(non_camel_case_types)]
        impl<$key: $crate::Fieldable, M: $crate::typing::NullabilityModifier> $crate::select::SelectedData for AnonymousData<$key, M>
            where <$key as $crate::Fieldable>::Grouped: $crate::select::GroupedToRows, {
            type Instantiated<A: $crate::TableAlias> = AnonymousDataInst<$key, A, M>;
            type Repr = ();
            type Rows = <<$key as $crate::Fieldable>::Grouped as $crate::select::GroupedToRows>::Output;
            type AllNullable = AnonymousData<$key, $crate::typing::AllNullable>;

            const NUM_COLS: usize = 1;

            fn instantiate<A: $crate::TableAlias>() -> Self::Instantiated<A> {
                AnonymousDataInst {
                    $key: $crate::Field::new(),
                }
            }

            fn make_nullable(self) -> Self::AllNullable {
                AnonymousData {
                    $key: self.$key,
                    _phantom: $crate::__private::PhantomData,
                }
            }
        }

        #[allow(non_camel_case_types)]
        impl<$key: $crate::Fieldable, M: $crate::typing::NullabilityModifier> $crate::select::SingleColumnSelectedData for AnonymousData<$key, M> {
            type ColumnTy = <$key as $crate::expr::Value>::Ty;
            type ColumnGrouping = <$key as $crate::Fieldable>::Grouped;
        }

        #[allow(non_camel_case_types)]
        impl<$key: $crate::Fieldable, M: $crate::typing::NullabilityModifier> $crate::select::IntoPartialSelect<AnonymousData<$key, M>, AllRows> for AnonymousData<$key, M>
            where <$key as $crate::Fieldable>::Grouped: $crate::select::GroupedToRows {
            type Output = $crate::select::SelectWithoutFrom<Self, AllRows>;

            fn into_partial_select(self) -> Self::Output {
                select(self, |_| AllRows)
            }
        }
    };
    (@wheregen { $ty:ty } { } { $last:ty } { $($backup:ident,)* } => { $($output:tt)* }) => {
        $crate::_internal_impl_selected_data!(@output { $ty } { $($backup,)* } { $($output)* } { $last });
    };
    (@wheregen { $ty:ty } { $firstkey:ident, $($key:ident,)* } { $last:ty } { $($backup:ident,)* } => { $($output:tt)* }) => {
        $crate::_internal_impl_selected_data!(@wheregen { $ty }
            { $($key,)* }
            { <($last, <$firstkey as $crate::Fieldable>::Grouped) as $crate::typing::CombineGrouping>::Result }
            { $($backup,)* }
            => { $($output)* ($last, <$firstkey as $crate::Fieldable>::Grouped): $crate::typing::CombineGrouping, }
        );
    };
    ( { $ty:ty } $firstkey:ident, $($key:ident,)*) => {
        $crate::_internal_impl_selected_data!(@wheregen { $ty } { $($key,)* } { <$firstkey as $crate::Fieldable>::Grouped } { $firstkey, $($key,)* } => {});
    };
    (@withfields { $index:expr } { } { $($key2:ident)* }) => {};
    (@withfields { $index:expr } { $key:ident $($rest:ident)* } { $($key2:ident)* }) => {
        #[allow(non_camel_case_types)]
        impl<$($key2: $crate::Fieldable,)*> $crate::WithField<{ stringify!($key) }> for Intermediate<'_, $($key2,)*> {
            type Output = $key::Repr;
            fn value(&self) -> Self::Output {
                $key::from_query_value(&self.0[$index])
            }
        }

        $crate::_internal_impl_selected_data!(@withfields { $index + 1 } { $($rest)* } { $($key2)* });
    };
    (@output { $ty:ty } { $($key:ident,)* } { $($constraint:tt)* } { $finalgrouping:ty }) => {
        #[allow(non_camel_case_types)]
        impl<$($key: $crate::Fieldable),*, M: $crate::typing::NullabilityModifier> $crate::select::SelectedData for AnonymousData<$($key),*, M>
            where $($constraint)*
                $finalgrouping: $crate::select::GroupedToRows {
            type Instantiated<A: $crate::TableAlias> = AnonymousDataInst<$($key),*, A, M>;
            type Repr = ();
            type AllNullable = AnonymousData<$($key),*, $crate::typing::AllNullable>;

            type Rows = <$finalgrouping as $crate::select::GroupedToRows>::Output;

            const NUM_COLS: usize = $crate::count!($($key),*);

            fn instantiate<A: $crate::TableAlias>() -> Self::Instantiated<A> {
                AnonymousDataInst {
                    $($key: $crate::Field::new()),*
                }
            }

            fn make_nullable(self) -> Self::AllNullable {
                AnonymousData {
                    $($key: self.$key),*,
                    _phantom: $crate::__private::PhantomData,
                }
            }
        }

        #[allow(non_camel_case_types)]
        struct Intermediate<'a, $($key, )*>(&'a [$crate::QueryValue], $crate::__private::PhantomData<($($key),*)>);

        $crate::_internal_impl_selected_data!(@withfields { 0 } { $($key)* }  { $($key)* });
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! _internal_select_derive_to_sql {
    ($firstkey:ident : $firstvalue:expr $(, $($key:ident : $value:expr),*)?) => {
        {
            #[allow(non_camel_case_types)]
            impl<
                $firstkey: $crate::ToSql,
                $($($key: $crate::ToSql,)*)?
                M: $crate::typing::NullabilityModifier
            > $crate::ToSql for AnonymousData<$firstkey, $($($key,)*)? M> {
                const SQL: $crate::ConstSqlStr = $crate::sql_concat!(
                    $firstkey, " AS `", (stringify!($firstkey)), "`"
                    $(, $(
                        ", ", $key, " AS `", (stringify!($key)), "`"
                    ),*)?
                );

                fn collect_parameters(&self, params: &mut Vec<$crate::QueryValue>) {
                    self.$firstkey.collect_parameters(params);

                    $($(
                        self.$key.collect_parameters(params);
                    )*)?
                }
            }
        }
    }
}

#[macro_export]
macro_rules! table {
    ($name:ident $real_table_name:literal { $($field:ident $real_name:literal: $ty:ty,)* }) => {
        $crate::__private::concat_idents!( modname = $name, Types {
            #[allow(non_snake_case)]
            mod modname {
                use super::*;
                $(
                    #[allow(non_camel_case_types)]
                    #[derive(Copy, Clone, Debug)]
                    pub struct $field;

                    impl $crate::FieldName for $field {
                        const NAME: &'static str = $real_name;
                    }
                )*

                #[derive(Clone)]
                pub struct $name<A, N: $crate::typing::NullabilityModifier> {
                    $(
                        pub $field: $crate::Field<<$ty as $crate::typing::Ty>::ModifyNullability<N>, A, $field>,
                    )*
                }
            }

            pub use modname::$name;

            impl<A: $crate::TableAlias, N: $crate::typing::NullabilityModifier> $crate::ToSql for $name<A, N> {
                const SQL: $crate::ConstSqlStr = $crate::ConstSqlStr::empty()
                    .append_str("`")
                    .append_str(stringify!($name))
                    .append_str("`")
                ;

                fn collect_parameters(&self, _: &mut Vec<$crate::QueryValue>) {}
            }

            impl<U: $crate::Up, A: $crate::TableAlias, N: $crate::typing::NullabilityModifier> $crate::QueryTree<U> for $name<A, N> {
                type MaxUp = U;
            }

            impl<A: $crate::TableAlias, N: $crate::typing::NullabilityModifier> $crate::TableReference for $name<A, N> {
                type AllNullable = $name<A, $crate::typing::AllNullable>;

                fn make_nullable(self) -> Self::AllNullable {
                    $name {
                        $(
                            $field: $crate::Field::new(),
                        )*
                    }
                }
            }

            impl $crate::Table for $name<(), $crate::typing::KeepOriginalNullability> {
                type WithAlias<X: $crate::TableAlias> = $name<X, $crate::typing::KeepOriginalNullability>;

                fn new<X: $crate::TableAlias>() -> Self::WithAlias<X> {
                    $name {
                        $(
                            $field: $crate::Field::new(),
                        )*
                    }
                }
            }
        });

        impl<A: $crate::TableAlias, N: $crate::typing::NullabilityModifier> std::fmt::Debug for $name<A, N> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, $real_table_name)
            }
        }

        impl<A: $crate::TableAlias, N: $crate::typing::NullabilityModifier> std::fmt::Display for $name<A, N> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, $real_table_name)
            }
        }
    }
}

pub trait FieldType<const NAME: &'static str> {
    type Ty;
}

pub trait WithField<const NAME: &'static str> {
    type Output;
    fn value(&self) -> Self::Output;
}

// TODO: @variable:=
// TODO: FROM tableA, tableB (without join)

pub trait ToSql {
    const SQL: ConstSqlStr;
    const SQL_STR: &'static str = Self::SQL.as_str();

    fn sql_str(&self) -> &'static str {
        Self::SQL_STR
    }

    fn collect_parameters(&self, f: &mut Vec<QueryValue>);
}

#[doc(hidden)]
pub mod __doctest {
    use crate::mysql::CollectResults;
    pub use crate::prelude::*;
    use crate::select::{FromRow, SelectQuery};
    use crate::typing::*;
    use crate::*;

    crate::table! {
        Users "Users" {
            id "Id": SimpleTy<BigInt<Signed>, NonNullable>,
            name "Name": SimpleTy<Text, NonNullable>,
        }
    }

    crate::table! {
        Questions "Questions" {
            id "Id": SimpleTy<BigInt<Signed>, NonNullable>,
            text "Text": SimpleTy<Text, NonNullable>,
            asked_by_id "AskedById": SimpleTy<BigInt<Signed>, NonNullable>,
        }
    }

    pub struct FakeConn;

    impl Database for FakeConn {
        type Iter<'a, Q: select::SelectQuery>
        where
            <Q as SelectQuery>::Columns: FromRow,
        = std::vec::IntoIter<
            Result<<<Q as SelectQuery>::Columns as FromRow>::Queried, ::mysql::Error>,
        >;

        fn typed_query<'a, Q: select::SelectQuery + QueryRoot>(
            &'a mut self,
            _query: Q,
        ) -> Result<
            <<Q as select::SelectQuery>::Rows as mysql::CollectResults<
                <Q::Columns as FromRow>::Queried,
                Self::Iter<'a, Q>,
            >>::PartialOutput,
            ::mysql::Error,
        >
        where
            Q::Columns: FromRow,
            <Q as select::SelectQuery>::Rows:
                mysql::CollectResults<<Q::Columns as FromRow>::Queried, Self::Iter<'a, Q>>,
        {
            // TODO: This will crash if we expect exactly one result
            <Q as SelectQuery>::Rows::collect_results(Vec::new().into_iter())
        }

        fn typed_exec<'a, Q: QueryRoot>(&'a mut self, _query: Q) -> Result<(), ::mysql::Error> {
            Ok(())
        }
    }

    pub fn ground<T: QueryTree<UpEnd>>(t: T) -> T {
        t
    }
}

#[cfg(test)]
mod tests {
    macro_rules! check_lift_if {
        ($($code:tt)*) => {
            assert_eq!($($code)*, crate::lift_if!($($code)*))
        }
    }

    #[test]
    pub fn lift_if_test() {
        #[allow(unused_braces)]
        for a in [false, true] {
            check_lift_if!(if a { 5 } else { 3 })
        }

        for b in [false, true] {
            for c in [false, true] {
                println!("b={:?}, c={:?}", b, c);
                check_lift_if!({
                    let x = if b { 7 } else { 1 };

                    if c {
                        x * 2
                    } else {
                        x
                    }
                })
            }
        }

        for a in [false, true] {
            for b in [false, true] {
                for c in [false, true] {
                    println!("a={:?}, b={:?}, c={:?}", a, b, c);
                    check_lift_if!(if a {
                        let x = if b { 7 } else { 1 };

                        if c {
                            x * 2
                        } else {
                            x
                        }
                    } else {
                        3
                    })
                }
            }
        }
    }

    #[test]
    pub fn lift_match() {
        for a in [false, true] {
            check_lift_if!(match a {
                true => 5,
                false => 3,
            })
        }

        for a in [false, true] {
            for b in [7, 9, 12] {
                for c in [false, true] {
                    println!("a={:?}, b={:?}, c={:?}", a, b, c);
                    check_lift_if!(if a {
                        let x = match b {
                            7 => 2,
                            9 => 4,
                            _ => 1,
                        };

                        if c {
                            x * 2
                        } else {
                            x
                        }
                    } else {
                        3
                    })
                }
            }
        }
    }
}
