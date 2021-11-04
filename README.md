# `typed-qb`: a compile-time typed "query builder"

`typed-qb` is a compile-time, typed, query builder. The goal of this crate is to explore the gap between compile-time verification of raw SQL queries (like `sqlx`) and runtime generation of queries via query builders (like `diesel`).

The query is transformed into an SQL query string at compile time. If code compiles and the schema in the code matches the database, it should be (*almost*) impossible to write queries that produce errors.

## Usage
You can specify your table structure by pasting the output of `SHOW CREATE TABLE ..` into the `tables!` macro. You will need to clean up the backticks (`), because Rust cannot parse them. For example:

```rust
tables! {
    CREATE TABLE Users (
        Id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT,
        Name VARCHAR(64) NOT NULL,
        PRIMARY KEY(Id)
    );
}
```

Field names will automatically be converted to `snake_case` if you write them in `camelCase` or `PascalCase`. You can then query the table as follows:

```rust
let conn = ...; // some connection

let all_users = conn.typed_query(
    Users::query(|user| data! {
        id: user.id,
        name: user.name,
    })
)?;

for user in all_users {
    let user = user?;
    println!("{} ({})", user.id, user.name);
}
```

The query is transformed *at compile-time* into:
```sql
SELECT `t0`.`Id` AS `id`, `t0`.`Name` AS `name` 
FROM `Users` AS t0
```

You can specify `WHERE`, `ORDER BY`, etc. clauses with the `select` function:

```rust
Users::query(|user| select(data! {
    id: user.id,
    name: user.name,
}, |selected| {
    expr!(selected.id < 5)
        .order_by(selected.name.asc()
            .then_by(selected.id.desc())
        )
        .limit::<3>()
}))
```

The type returned by `typed_query` depends on the query itself. The two queries above return an iterator over an anonymous `struct`. If a single value is selected, for example:

```rust
let user_count = conn.typed_query(Users::query(|_| expr!(COUNT(*))))?;
```

The type of user_count will be a `u64`.

See [more examples](typed-qb/examples/select.rs).

## (In)stability
This project requires Rust nightly, and uses a bunch of features, some incomplete. You might encounter ICEs. There will probably be many breaking changes to `typed-qb` itself as well. You probably shouldn't use `typed-qb` for serious projects.

## License
`typed-qb` is licensed under the Mozilla Public License 2.0 (MPL2.0). See the `LICENSE` file.