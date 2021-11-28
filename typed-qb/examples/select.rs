#![feature(generic_associated_types)]

use mysql::{OptsBuilder, Pool};
use typed_qb::{prelude::*, qualifiers::AsWhere};

typed_qb::tables! {
    CREATE TABLE Users (
        Id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT,
        Name VARCHAR(64) NOT NULL,
        PRIMARY KEY(Id)
    );

    CREATE TABLE Questions (
        Id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT,
        AskedById INT(11) UNSIGNED NOT NULL,
        Text TEXT NOT NULL,
        PRIMARY KEY(Id),
        KEY IX_Users_AskedById (AskedById),
        CONSTRAINT FK_Questions_Users_AskedById FOREIGN KEY (AskedById) REFERENCES Users (Id) ON DELETE CASCADE
    );
}

#[allow(dead_code)]
#[derive(typed_qb::QueryInto, Debug)]
struct User {
    id: UserId,
    name: String,
}

#[derive(Debug)]
struct UserId(u32);

impl From<u32> for UserId {
    fn from(id: u32) -> Self {
        UserId(id)
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let opts = OptsBuilder::new()
        .user(Some("typed_db"))
        .pass(Some("secret123"))
        .db_name(Some("typed_db"));
    let pool = Pool::new(opts)?;

    let mut conn = pool.get_conn()?;

    // A SELECT query can be constructed with Table::query(..)
    let q = Users::query(|user| {
        data! {
            // equivalent to `user`.`id` AS `id`
            id: user.id,

            // `a.b` will be expanded to `b: a.b`
            user.name,
        }
    });

    // The query is compiled into a constant at compile time.
    // It can also be accessed via ToSql::sql_str()
    println!("The first query is: {:?}", typed_qb::ToSql::sql_str(&q));

    // The query can be executed with `typed_query`. It returns an iterator for queries that return zero or more results.
    let all_items = conn.typed_query(q)?;
    for result in all_items {
        let result = result?;

        // Note that we can access all the requested fields as struct fields.
        // It's impossible to access a non-existant field here.
        println!("Item with id={}: {:?}", result.id, result);
    }

    // You can also collect into existing structs
    let users: Vec<User> = conn
        .typed_query(Users::query(|user| {
            data! { as User:
                user.id,
                user.name,
            }
        }))?
        .to_vec()?;

    println!("All users in a custom struct: {:?}", users);

    let user_ids: Vec<UserId> = conn
        .typed_query(Users::query(|user| data! { as UserId: user.id }))?
        .to_vec()?;
    println!("All user IDs in a custom struct: {:?}", user_ids);

    // Parameters can be specified with variables:
    let id = 2;
    let name_by_id = conn.typed_query(Users::query(|user|
        // The select(.., ..) function can be used to specify WHERE, ORDER BY, GROUP BY, HAVING, LIMIT, etc.
        select(data! {
            user.name,
        }, |_|
            // The expr! macro can be used to specify SQL expressions in a more friendly syntax.
            // This is equivalent to "WHERE `user`.`id` = :id"
            expr!(user.id = :id)
                // LIMIT 1
                .limit::<1>()
        )))?;
    // The parameters will be constructed at runtime.
    // The query is still transformed into a raw SQL string at compile time.

    // name_by_id is an Option<String>, since we're only selecting a single column and the query never returns more than one row.
    println!("Name of user with id = 2: {:?}", name_by_id);

    let user_count = conn.typed_query(Users::query(|_|
        // The [...] is expanded into expr!(...)
        data! { [COUNT(*)] }))?;

    // user_count is an u64, since our query always returns a single row of a single column
    println!("Items on wishlist: {}", user_count);

    // You can also leave out the data! { .. } for single column results
    let user_count = conn.typed_query(Users::query(|_| expr!(COUNT(*))))?;

    println!("Items on wishlist: {}", user_count);

    let user_stats = conn
        .typed_query(Users::query(|user| {
            data! {
                user.id,
                user.name,
                // For subqueries, the number of columns returned is checked at compile time (must be 1 column)
                // For now, `as_where()` is needed if no further GROUP BY, LIMIT, etc. is specified
                questions_asked: Questions::query(|question|
                    select(expr!(COUNT(*)).as_selected_data(), |_|
                        expr!(question.asked_by_id = user.id).as_where()
                    )
                )
            }
        }))?
        .to_vec()?;
    println!("User stats: {:?}", user_stats);

    // The same query as the previous one, but with a JOIN instead of a subquery
    let user_stats = conn
        .typed_query(Users::query(|user| {
            Questions::left_join(
                // Equivalent of: ON question.asked_by_id = user.id
                |question| expr!(question.asked_by_id = user.id),
                |question| {
                    select(
                        data! {
                            id: question.id,
                            name: user.name,
                            questions_asked: [COUNT(*)],
                        },
                        |selected| {
                            AllRows.group_by(selected.id).order_by(
                                expr!(selected.questions_asked)
                                    .desc()
                                    // You can chain ORDER BYs with then_by(..)
                                    // The expr!(..) macro is not needed for simple fields
                                    .then_by(selected.name.asc()),
                            )
                        },
                    )
                },
            )
        }))?
        .to_vec()?;
    println!("User stats: {:?}", user_stats);

    // Unfortunately it's currently still quite difficult to dynamically construct queries at runtime.
    // For example, what if we want to sort our results depending on a variable:
    let sort_desc = false;

    // You can use the lift_if!{..} macro to make this easier.
    // The lift_if!{..} macro can help with this by reducing the amount of duplicate code needed:
    typed_qb::lift_if! {
        let users = conn.typed_query(Users::query(|user| select(data! {
            user.id,
            user.name,
        }, |_| AllRows
            .order_by(if sort_desc {
                user.name.desc()
            } else {
                user.name.asc()
            })
        )))?.to_vec()?;

        println!("Users: {:?}", users);
    }

    // The lift_if! macro DUPLICATES all code. So the code above would be expanded to:
    if sort_desc {
        let users = conn
            .typed_query(Users::query(|user| {
                select(
                    data! {
                        user.id,
                        user.name,
                    },
                    |_| AllRows.order_by(user.name.desc()),
                )
            }))?
            .to_vec()?;

        println!("Users: {:?}", users);
    } else {
        let users = conn
            .typed_query(Users::query(|user| {
                select(
                    data! {
                        user.id,
                        user.name,
                    },
                    |selected| AllRows.order_by(selected.name.asc()),
                )
            }))?
            .to_vec()?;

        println!("Users: {:?}", users);
    }

    Ok(())
}
