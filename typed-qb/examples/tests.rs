#![feature(generic_associated_types)]

use typed_qb::{prelude::*, qualifiers::AsWhere, QueryRoot};

// These "tests" are here because they cause compiler errors when placed in the main lib.
// See the test in select.rs that is commented out.
// TODO: Try moving this back to the lib once 

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

fn sql<R: QueryRoot>(_: R) -> &'static str { R::SQL_STR }

fn main() {
    let k: i32 = 5;
    let q = sql(Questions::query(|question| {
        Users::left_join(
            |user| expr!(user.id = question.asked_by_id),
            |user| {
                select(
                    data! {
                        question.id,
                        num: Questions::count(|q| expr!(q.asked_by_id = user.id).as_where()),
                    },
                    |selected| expr!(selected.id = :k).as_where(),
                )
            },
        )
    }));

    assert_eq!(q, "(SELECT `t4`.`Id` AS `f0`, (SELECT COUNT(*) AS `f2` FROM `Questions` AS t3 WHERE (`t3`.`AskedById` = `t4`.`Id`)) AS `f1` FROM `Questions` AS t4 LEFT JOIN `Users` AS t4 ON (`t4`.`Id` = `t4`.`AskedById`) WHERE (`f0` = ?))");
}
