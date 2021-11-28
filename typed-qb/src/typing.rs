use crate::QueryValue;
use std::{convert::TryInto, fmt, marker::PhantomData};

pub trait Ty {
    type Base: BaseTy;
    type Nullable: IsNullable;
    type ModifyNullability<M: NullabilityModifier>: Ty;
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct KeepOriginalNullability;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct AllNullable;

pub trait NullabilityModifier {
    type MapNullability<N: IsNullable>: IsNullable;
}

impl NullabilityModifier for KeepOriginalNullability {
    type MapNullability<N: IsNullable> = N;
}

impl NullabilityModifier for AllNullable {
    type MapNullability<N: IsNullable> = Nullable;
}

pub struct SimpleTy<B: BaseTy, N: IsNullable>(PhantomData<(B, N)>);
impl<B: BaseTy, N: IsNullable> Ty for SimpleTy<B, N> {
    type Base = B;
    type Nullable = N;
    type ModifyNullability<M: NullabilityModifier> = SimpleTy<B, M::MapNullability<N>>;
}

impl<B: BaseTy, N: IsNullable> Clone for SimpleTy<B, N> {
    fn clone(&self) -> Self {
        Self(PhantomData)
    }
}

impl<B: BaseTy, N: IsNullable> Copy for SimpleTy<B, N> {}

impl<B: BaseTy, N: IsNullable> fmt::Debug for SimpleTy<B, N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("SimpleTy").finish()
    }
}

pub trait BaseTy {
    type Repr;

    fn parse(value: &QueryValue) -> Self::Repr;
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Bool;

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct F64;

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct F32;

pub trait Signedness {
    type BigInt;
    type Int;
    type MediumInt;
    type SmallInt;
    type TinyInt;

    fn parse_bigint(value: &QueryValue) -> Self::BigInt;
    fn parse_int(value: &QueryValue) -> Self::Int;
    fn parse_mediumint(value: &QueryValue) -> Self::MediumInt;
    fn parse_smallint(value: &QueryValue) -> Self::SmallInt;
    fn parse_tinyint(value: &QueryValue) -> Self::TinyInt;
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Unsigned;

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Signed;

impl Signedness for Unsigned {
    type BigInt = u64;
    type Int = u32;
    type MediumInt = u32;
    type SmallInt = u16;
    type TinyInt = u8;

    fn parse_bigint(value: &QueryValue) -> Self::BigInt {
        match value {
            QueryValue::I64(v) => (*v).try_into().unwrap(),
            QueryValue::U64(v) => (*v).try_into().unwrap(),
            QueryValue::String(v) => (*v).parse().unwrap(),
            QueryValue::Bytes(v) => std::str::from_utf8(&v).unwrap().parse().unwrap(),
            _ => unreachable!(),
        }
    }

    fn parse_int(value: &QueryValue) -> Self::Int {
        match value {
            QueryValue::I64(v) => (*v).try_into().unwrap(),
            QueryValue::U64(v) => (*v).try_into().unwrap(),
            QueryValue::String(v) => (*v).parse().unwrap(),
            QueryValue::Bytes(v) => std::str::from_utf8(&v).unwrap().parse().unwrap(),
            _ => unreachable!(),
        }
    }

    fn parse_mediumint(value: &QueryValue) -> Self::MediumInt {
        match value {
            QueryValue::I64(v) => (*v).try_into().unwrap(),
            QueryValue::U64(v) => (*v).try_into().unwrap(),
            QueryValue::String(v) => (*v).parse().unwrap(),
            QueryValue::Bytes(v) => std::str::from_utf8(&v).unwrap().parse().unwrap(),
            _ => unreachable!(),
        }
    }

    fn parse_smallint(value: &QueryValue) -> Self::SmallInt {
        match value {
            QueryValue::I64(v) => (*v).try_into().unwrap(),
            QueryValue::U64(v) => (*v).try_into().unwrap(),
            QueryValue::String(v) => (*v).parse().unwrap(),
            QueryValue::Bytes(v) => std::str::from_utf8(&v).unwrap().parse().unwrap(),
            _ => unreachable!(),
        }
    }

    fn parse_tinyint(value: &QueryValue) -> Self::TinyInt {
        match value {
            QueryValue::I64(v) => (*v).try_into().unwrap(),
            QueryValue::U64(v) => (*v).try_into().unwrap(),
            QueryValue::String(v) => (*v).parse().unwrap(),
            QueryValue::Bytes(v) => std::str::from_utf8(&v).unwrap().parse().unwrap(),
            _ => unreachable!(),
        }
    }
}

impl Signedness for Signed {
    type BigInt = i64;
    type Int = i32;
    type MediumInt = i32;
    type SmallInt = i16;
    type TinyInt = i8;

    fn parse_bigint(value: &QueryValue) -> Self::BigInt {
        match value {
            QueryValue::I64(v) => (*v).try_into().unwrap(),
            QueryValue::U64(v) => (*v).try_into().unwrap(),
            QueryValue::String(v) => v.parse().unwrap(),
            QueryValue::Bytes(v) => std::str::from_utf8(&v).unwrap().parse().unwrap(),
            _ => unreachable!(),
        }
    }

    fn parse_int(value: &QueryValue) -> Self::Int {
        match value {
            QueryValue::I64(v) => (*v).try_into().unwrap(),
            QueryValue::U64(v) => (*v).try_into().unwrap(),
            QueryValue::String(v) => v.parse().unwrap(),
            QueryValue::Bytes(v) => std::str::from_utf8(&v).unwrap().parse().unwrap(),
            _ => unreachable!(),
        }
    }

    fn parse_mediumint(value: &QueryValue) -> Self::MediumInt {
        match value {
            QueryValue::I64(v) => (*v).try_into().unwrap(),
            QueryValue::U64(v) => (*v).try_into().unwrap(),
            QueryValue::String(v) => v.parse().unwrap(),
            QueryValue::Bytes(v) => std::str::from_utf8(&v).unwrap().parse().unwrap(),
            _ => unreachable!(),
        }
    }

    fn parse_smallint(value: &QueryValue) -> Self::SmallInt {
        match value {
            QueryValue::I64(v) => (*v).try_into().unwrap(),
            QueryValue::U64(v) => (*v).try_into().unwrap(),
            QueryValue::String(v) => v.parse().unwrap(),
            QueryValue::Bytes(v) => std::str::from_utf8(&v).unwrap().parse().unwrap(),
            _ => unreachable!(),
        }
    }

    fn parse_tinyint(value: &QueryValue) -> Self::TinyInt {
        match value {
            QueryValue::I64(v) => (*v).try_into().unwrap(),
            QueryValue::U64(v) => (*v).try_into().unwrap(),
            QueryValue::String(v) => v.parse().unwrap(),
            QueryValue::Bytes(v) => std::str::from_utf8(&v).unwrap().parse().unwrap(),
            _ => unreachable!(),
        }
    }
}

pub trait MergeSigns {
    type Result: Signedness;
}

impl MergeSigns for (Signed, Signed) {
    type Result = Signed;
}
impl MergeSigns for (Signed, Unsigned) {
    type Result = Unsigned;
}
impl MergeSigns for (Unsigned, Signed) {
    type Result = Unsigned;
}
impl MergeSigns for (Unsigned, Unsigned) {
    type Result = Unsigned;
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct BigInt<S: Signedness>(S);

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Int<S: Signedness>(S);

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct MediumInt<S: Signedness>(S);

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SmallInt<S: Signedness>(S);

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct TinyInt<S: Signedness>(S);

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Text;

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct DateTime;

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Time;

impl BaseTy for Bool {
    type Repr = bool;

    fn parse(value: &QueryValue) -> Self::Repr {
        match value {
            QueryValue::I64(v) => *v != 0,
            QueryValue::U64(v) => *v != 0,
            // TODO: does this use byte values 0x30 and 0x31 instead of zero and non-zero
            QueryValue::Bytes(b) => b[0] != 0,
            _ => unreachable!(),
        }
    }
}

impl BaseTy for F64 {
    type Repr = f64;

    fn parse(value: &QueryValue) -> Self::Repr {
        match value {
            QueryValue::F32(f) => *f as f64,
            QueryValue::F64(f) => *f,
            _ => unreachable!(),
        }
    }
}

impl BaseTy for F32 {
    type Repr = f32;

    fn parse(value: &QueryValue) -> Self::Repr {
        match value {
            QueryValue::F32(f) => *f,
            // TODO: Should we allow this?
            QueryValue::F64(f) => *f as f32,
            _ => unreachable!(),
        }
    }
}

impl<S: Signedness> BaseTy for BigInt<S> {
    type Repr = S::BigInt;

    fn parse(value: &QueryValue) -> Self::Repr {
        S::parse_bigint(value)
    }
}

impl<S: Signedness> BaseTy for Int<S> {
    type Repr = S::Int;

    fn parse(value: &QueryValue) -> Self::Repr {
        S::parse_int(value)
    }
}

impl<S: Signedness> BaseTy for MediumInt<S> {
    type Repr = S::MediumInt;

    fn parse(value: &QueryValue) -> Self::Repr {
        S::parse_mediumint(value)
    }
}

impl<S: Signedness> BaseTy for SmallInt<S> {
    type Repr = S::SmallInt;

    fn parse(value: &QueryValue) -> Self::Repr {
        S::parse_smallint(value)
    }
}

impl<S: Signedness> BaseTy for TinyInt<S> {
    type Repr = S::TinyInt;

    fn parse(value: &QueryValue) -> Self::Repr {
        S::parse_tinyint(value)
    }
}

impl BaseTy for Text {
    type Repr = String;

    fn parse(value: &QueryValue) -> Self::Repr {
        match value {
            QueryValue::String(s) => s.clone(),
            QueryValue::Bytes(b) => std::str::from_utf8(&b).unwrap().to_string(),
            value => unreachable!("creating a string from {:?}", value),
        }
    }
}

fn parse_datetime_string(s: &str) -> crate::DateTime {
    if s.len() < 10 {
        panic!("invalid datetime string: {}", s);
    }

    let (y, m, d) = (&s[..4], &s[5..7], &s[8..10]);
    assert_eq!(&s[4..5], "-");
    assert_eq!(&s[7..8], "-");
    let (year, month, day) = (y.parse().unwrap(), m.parse().unwrap(), d.parse().unwrap());

    let (hour, minutes, seconds) = if s.len() >= 19 {
        assert_eq!(&s[13..14], ":");
        assert_eq!(&s[16..17], ":");
        (
            s[11..13].parse().unwrap(),
            s[14..16].parse().unwrap(),
            s[17..19].parse().unwrap(),
        )
    } else {
        Default::default()
    };

    let micro_seconds = if s.len() > 20 {
        assert_eq!(&s[19..20], ".");
        let mut v: u32 = s[20..].parse().unwrap();
        for _ in 0..(6 - s[20..].len()) {
            v *= 10;
        }

        v
    } else {
        0
    };

    crate::DateTime {
        year,
        month,
        day,
        hour,
        minutes,
        seconds,
        micro_seconds,
    }
}

fn parse_time_string(s: &str) -> crate::Time {
    if s.len() < 8 {
        panic!("invalid datetime string: {}", s);
    }

    let (is_negative, s) = if &s[..1] == "-" {
        (true, &s[1..])
    } else {
        (false, s)
    };

    let colon = s.find(':').unwrap();
    let hours = s[..colon].parse().unwrap();

    let minutes = s[colon + 1..colon + 3].parse().unwrap();
    let seconds = s[colon + 4..colon + 6].parse().unwrap();

    assert_eq!(&s[colon + 3..colon + 4], ":");

    let rest = &s[colon + 6..];
    let micro_seconds = if rest.len() > 0 {
        let mut v: u32 = rest.parse().unwrap();
        for _ in 0..(6 - rest.len()) {
            v *= 10;
        }

        v
    } else {
        0
    };

    crate::Time {
        is_negative,
        days: 0,
        hours,
        minutes,
        seconds,
        micro_seconds,
    }
}

impl BaseTy for DateTime {
    type Repr = crate::DateTime;

    fn parse(value: &QueryValue) -> Self::Repr {
        match value {
            QueryValue::DateTime(d) => d.clone(),
            QueryValue::Bytes(b) => parse_datetime_string(std::str::from_utf8(&b).unwrap()),
            QueryValue::String(s) => parse_datetime_string(s.as_ref()),
            value => unreachable!("creating a DateTime from {:?}", value),
        }
    }
}

impl BaseTy for Time {
    type Repr = crate::Time;

    fn parse(value: &QueryValue) -> Self::Repr {
        match value {
            QueryValue::Time(t) => t.clone(),
            QueryValue::Bytes(b) => parse_time_string(std::str::from_utf8(&b).unwrap()),
            QueryValue::String(s) => parse_time_string(s.as_ref()),
            value => unreachable!("creating a Time from {:?}", value),
        }
    }
}

pub trait AnyInt {
    type Sign: Signedness;
}
impl<S: Signedness> AnyInt for BigInt<S> {
    type Sign = S;
}
impl<S: Signedness> AnyInt for Int<S> {
    type Sign = S;
}
impl<S: Signedness> AnyInt for MediumInt<S> {
    type Sign = S;
}
impl<S: Signedness> AnyInt for SmallInt<S> {
    type Sign = S;
}
impl<S: Signedness> AnyInt for TinyInt<S> {
    type Sign = S;
}

pub trait IsNullable {
    type Repr<T>;

    fn parse<T: BaseTy>(value: &QueryValue) -> Self::Repr<T::Repr>;
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Nullable;

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct NonNullable;

impl IsNullable for Nullable {
    type Repr<T> = Option<T>;

    fn parse<T: BaseTy>(value: &QueryValue) -> Self::Repr<T::Repr> {
        match value {
            QueryValue::Null => None,
            value => Some(T::parse(value)),
        }
    }
}

impl IsNullable for NonNullable {
    type Repr<T> = T;

    fn parse<T: BaseTy>(value: &QueryValue) -> Self::Repr<T::Repr> {
        T::parse(value)
    }
}

pub trait CombineNullability {
    type Result: IsNullable;
}

impl CombineNullability for (Nullable, Nullable) {
    type Result = Nullable;
}
impl CombineNullability for (Nullable, NonNullable) {
    type Result = Nullable;
}
impl CombineNullability for (NonNullable, Nullable) {
    type Result = Nullable;
}
impl CombineNullability for (NonNullable, NonNullable) {
    type Result = NonNullable;
}

pub trait IsGrouped {}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Grouped;

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Ungrouped;

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Undetermined;

impl IsGrouped for Grouped {}
impl IsGrouped for Ungrouped {}
impl IsGrouped for Undetermined {}

pub trait CombineGrouping {
    type Result: IsGrouped;
}

impl CombineGrouping for (Grouped, Grouped) {
    type Result = Grouped;
}
impl CombineGrouping for (Ungrouped, Ungrouped) {
    type Result = Ungrouped;
}

impl CombineGrouping for (Grouped, Undetermined) {
    type Result = Grouped;
}
impl CombineGrouping for (Ungrouped, Undetermined) {
    type Result = Ungrouped;
}

impl CombineGrouping for (Ungrouped, Grouped) {
    type Result = Grouped;
}
impl CombineGrouping for (Grouped, Ungrouped) {
    type Result = Grouped;
}

impl CombineGrouping for (Undetermined, Grouped) {
    type Result = Grouped;
}
impl CombineGrouping for (Undetermined, Ungrouped) {
    type Result = Ungrouped;
}
impl CombineGrouping for (Undetermined, Undetermined) {
    type Result = Undetermined;
}

pub trait MergeNumbers {
    type Result: BaseTy;
}

impl<X: AnyInt, Y: AnyInt> MergeNumbers for (X, Y)
where
    (X::Sign, Y::Sign): MergeSigns,
{
    type Result = BigInt<<(X::Sign, Y::Sign) as MergeSigns>::Result>;
}
impl<X: AnyInt> MergeNumbers for (X, F64) {
    type Result = F64;
}
impl<Y: AnyInt> MergeNumbers for (F64, Y) {
    type Result = F64;
}

#[cfg(test)]
mod tests {
    use super::BaseTy;
    use crate::QueryValue;

    #[test]
    pub fn datetime_from_string() {
        let dt = QueryValue::String("2011-10-09 08:07:06.111111".to_string());
        let d = super::DateTime::parse(&dt);
        assert_eq!(
            d,
            crate::DateTime {
                year: 2011,
                month: 10,
                day: 9,
                hour: 8,
                minutes: 7,
                seconds: 6,
                micro_seconds: 111_111,
            }
        );
        assert_eq!(
            crate::DateTime::from(&chrono::NaiveDateTime::from(d.clone())),
            d
        );

        let dt = QueryValue::String("2011-10-09 08:07:06.5".to_string());
        let d = super::DateTime::parse(&dt);
        assert_eq!(
            d,
            crate::DateTime {
                year: 2011,
                month: 10,
                day: 9,
                hour: 8,
                minutes: 7,
                seconds: 6,
                micro_seconds: 500_000,
            }
        );
        assert_eq!(
            crate::DateTime::from(&chrono::NaiveDateTime::from(d.clone())),
            d
        );

        let dt = QueryValue::String("2011-10-09 08:07:06.54".to_string());
        let d = super::DateTime::parse(&dt);
        assert_eq!(
            d,
            crate::DateTime {
                year: 2011,
                month: 10,
                day: 9,
                hour: 8,
                minutes: 7,
                seconds: 6,
                micro_seconds: 540_000,
            }
        );
        assert_eq!(
            crate::DateTime::from(&chrono::NaiveDateTime::from(d.clone())),
            d
        );
    }
}
