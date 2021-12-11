#![feature(generic_associated_types)]

use typed_qb::{prelude::*, qualifiers::AsWhere, QueryRoot};

// These "tests" are here because they cause compiler errors when placed in the main lib.
// See the test in select.rs that is commented out.

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

fn consume<R: QueryRoot>(_: R) {}

fn main() {
    let k: i32 = 5;
    consume(Questions::query(|question| {
        Users::left_join(
            |user| expr!(user.id = question.asked_by_id),
            |user| {
                select(
                    data! {
                        question.id,
                        num: Questions::count(|q| expr!(q.asked_by_id = user.id).as_where()),
                    },
                    |_| expr!(question.id = :k).as_where(),
                )
            },
        )
    }));
}
