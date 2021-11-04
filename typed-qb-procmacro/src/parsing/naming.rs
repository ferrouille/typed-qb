use std::fmt::Debug;

pub trait NamingStrategy: Debug {
    fn translate<'a>(&self, name: &str) -> String;
}

#[derive(Debug, Default)]
pub struct IdentityNamingStrategy;

impl NamingStrategy for IdentityNamingStrategy {
    fn translate(&self, name: &str) -> String {
        name.to_owned()
    }
}

#[derive(Debug, Default)]
pub struct SnakeCaseNamingStrategy;

impl NamingStrategy for SnakeCaseNamingStrategy {
    fn translate(&self, name: &str) -> String {
        let mut is_capital_case = true;
        let mut s = String::with_capacity(name.len());
        for c in name.chars() {
            if c.is_uppercase() {
                if is_capital_case {
                    s.extend(c.to_lowercase());
                } else {
                    is_capital_case = true;
                    s.push('_');
                    s.extend(c.to_lowercase());
                }
            } else {
                is_capital_case = false;
                s.push(c);
            }
        }

        s
    }
}
