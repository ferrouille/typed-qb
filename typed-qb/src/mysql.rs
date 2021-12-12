use crate::{
    select::{ExactlyOne, FromRow, RowKind, SelectQuery, SelectedData, ZeroOrMore, ZeroOrOne},
    QueryRoot, QueryValue,
};
use log::{debug, trace};
use mysql::{prelude::Queryable, Binary, QueryResult, Value};
use std::marker::PhantomData;

pub trait CollectResults<T, I: Iterator<Item = Result<T, mysql::Error>>>: RowKind {
    type Output = Self::Repr<T>;
    type PartialOutput = Self::Repr<T>;

    fn collect_results(iter: I) -> Result<Self::PartialOutput, mysql::Error>;
}

impl<T, I: Iterator<Item = Result<T, mysql::Error>>> CollectResults<T, I> for ZeroOrOne {
    fn collect_results(mut iter: I) -> Result<Self::PartialOutput, mysql::Error> {
        Ok(match iter.next() {
            Some(Ok(result)) => {
                assert!(iter.next().is_none(), "Compile-time analysis of the query was wrong; Expected exactly one result, but received more than one.");
                Some(result)
            }
            Some(Err(e)) => return Err(e),
            None => None,
        })
    }
}

impl<T, I: Iterator<Item = Result<T, mysql::Error>>> CollectResults<T, I> for ExactlyOne {
    fn collect_results(mut iter: I) -> Result<Self::PartialOutput, mysql::Error> {
        Ok(match iter.next() {
            Some(Ok(result)) => {
                assert!(iter.next().is_none(), "Compile-time analysis of the query was wrong; Expected exactly one result, but received more than one.");
                result
            },
            Some(Err(e)) => return Err(e),
            None => panic!("Compile-time analysis of the query was wrong; Expected exactly one result, but received none"),
        })
    }
}

impl<T, I: Iterator<Item = Result<T, mysql::Error>>> CollectResults<T, I> for ZeroOrMore {
    type PartialOutput = I;

    fn collect_results(iter: I) -> Result<Self::PartialOutput, mysql::Error> {
        Ok(iter)
    }
}

pub trait Database {
    type Iter<'a, Q: SelectQuery>: Iterator<
        Item = Result<<Q::Columns as FromRow>::Queried, mysql::Error>,
    >
    where
        Self: 'a,
        Q::Columns: FromRow;

    fn typed_query<'a, Q: SelectQuery + QueryRoot>(
        &'a mut self,
        query: Q,
    ) -> Result<
        <<Q as SelectQuery>::Rows as CollectResults<
            <Q::Columns as FromRow>::Queried,
            Self::Iter<'a, Q>,
        >>::PartialOutput,
        mysql::Error,
    >
    where
        Q::Columns: FromRow,
        <Q as SelectQuery>::Rows:
            CollectResults<<Q::Columns as FromRow>::Queried, Self::Iter<'a, Q>>;

    fn typed_exec<'a, Q: QueryRoot>(&'a mut self, query: Q) -> Result<usize, mysql::Error>;
}

const NULL: QueryValue = QueryValue::Null;

pub struct ResultIter<'c, 't, 'tc, Q> {
    iter: QueryResult<'c, 't, 'tc, Binary>,
    _phantom: PhantomData<Q>,
}

impl<'c, 't, 'tc, Q: SelectQuery> Iterator for ResultIter<'c, 't, 'tc, Q>
where
    Q::Columns: FromRow,
{
    type Item = Result<<Q::Columns as FromRow>::Queried, mysql::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|result| {
            result.map(|mut row| {
                // TODO: Make this an array once rust supports it (currently gives an ICE)
                let mut data = vec![NULL; <Q::Columns as SelectedData>::NUM_COLS];
                for (index, value) in data.iter_mut().enumerate() {
                    *value = match row.take(index).unwrap() {
                        Value::NULL => QueryValue::Null,
                        Value::Int(v) => QueryValue::I64(v),
                        Value::UInt(v) => QueryValue::U64(v),
                        Value::Bytes(b) => QueryValue::Bytes(b),
                        Value::Float(v) => QueryValue::F32(v),
                        Value::Double(v) => QueryValue::F64(v),
                        Value::Date(year, month, day, hour, minutes, seconds, micro_seconds) => {
                            QueryValue::DateTime(crate::DateTime {
                                year,
                                month,
                                day,
                                hour,
                                minutes,
                                seconds,
                                micro_seconds,
                            })
                        }
                        Value::Time(is_negative, days, hours, minutes, seconds, micro_seconds) => {
                            QueryValue::Time(crate::Time {
                                is_negative,
                                days,
                                hours,
                                minutes,
                                seconds,
                                micro_seconds,
                            })
                        }
                    };
                }

                trace!("Loaded row: {:?}", data);

                <Q::Columns as FromRow>::from_row(&data)
            })
        })
    }
}

impl<'c, 't, 'tc, Q: SelectQuery> ResultIter<'c, 't, 'tc, Q>
where
    Q::Columns: FromRow,
{
    pub fn to_vec(self) -> Result<Vec<<Q::Columns as FromRow>::Queried>, mysql::Error> {
        self.collect()
    }
}

impl From<crate::DateTime> for Value {
    fn from(dt: crate::DateTime) -> Self {
        Value::Date(
            dt.year,
            dt.month,
            dt.day,
            dt.hour,
            dt.minutes,
            dt.seconds,
            dt.micro_seconds,
        )
    }
}

impl From<crate::Time> for Value {
    fn from(t: crate::Time) -> Self {
        Value::Time(
            t.is_negative,
            t.days,
            t.hours,
            t.minutes,
            t.seconds,
            t.micro_seconds,
        )
    }
}

fn into_params<I: IntoIterator<Item = QueryValue>>(values: I) -> Vec<Value> {
    // Unfortunately we have to create a Vec here because the mysql library expects the parameters as a Vec.
    // If we pass something other than a Vec, the mysql crate will allocate one for us.
    values
        .into_iter()
        .map(|p| match p {
            QueryValue::String(v) => Value::from(v),
            QueryValue::I64(v) => Value::from(v),
            QueryValue::U64(v) => Value::from(v),
            QueryValue::Bytes(v) => Value::from(v),
            QueryValue::F32(v) => Value::from(v),
            QueryValue::F64(v) => Value::from(v),
            QueryValue::DateTime(v) => Value::from(v),
            QueryValue::Time(v) => Value::from(v),
            QueryValue::Null => Value::NULL,
        })
        .collect()
}

impl<X: Queryable> Database for X
where
    X: 'static,
{
    type Iter<'a, Q: SelectQuery>
    where
        Q::Columns: FromRow,
    = ResultIter<'a, 'a, 'a, Q>;

    fn typed_query<'a, Q: SelectQuery + QueryRoot>(
        &'a mut self,
        query: Q,
    ) -> Result<
        <<Q as SelectQuery>::Rows as CollectResults<
            <Q::Columns as FromRow>::Queried,
            Self::Iter<'a, Q>,
        >>::PartialOutput,
        mysql::Error,
    >
    where
        Q::Columns: FromRow,
        <Q as SelectQuery>::Rows:
            CollectResults<<Q::Columns as FromRow>::Queried, Self::Iter<'a, Q>>,
    {
        debug!("Preparing query: {} with {} params", Q::SQL_STR, Q::NUM_PARAMS);

        // TODO: Make this an array once rust supports it (currently gives an ICE)
        let mut params = vec![NULL; Q::NUM_PARAMS];
        query.collect_parameters(&mut params);

        debug!("Running query: {} with params: {:?}", Q::SQL_STR, params);
        let results = self.exec_iter(&Q::SQL_STR, into_params(params))?;

        <Q as SelectQuery>::Rows::collect_results(ResultIter {
            iter: results,
            _phantom: PhantomData,
        })
    }

    fn typed_exec<'a, Q: QueryRoot>(&'a mut self, query: Q) -> Result<usize, mysql::Error> {
        // TODO: Make this an array once rust supports it (currently gives an ICE)
        let mut params = vec![NULL; Q::NUM_PARAMS];
        query.collect_parameters(&mut params);

        debug!("Running query: {} with params: {:?}", Q::SQL_STR, params);
        let result = self.exec_iter(&Q::SQL_STR, into_params(params))?;

        Ok(result.last_insert_id().unwrap_or(0) as usize)
    }
}
