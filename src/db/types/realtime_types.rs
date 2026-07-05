use sqlx::{Postgres, QueryBuilder};

use crate::{
    db::types::InsertDB,
    transit_realtime::{trip_update::StopTimeUpdate, *},
};

impl FeedMessage {
    fn map(&self) -> Vec<sqlx::QueryBuilder<'static, sqlx::Postgres>> {
        let mut qb = QueryBuilder::<Postgres>::new("");
        vec![qb].extend(self.header.map())
    }
}

impl FeedHeader {
    fn map(&self) -> Vec<sqlx::QueryBuilder<'static, sqlx::Postgres>> {
        let mut qb = QueryBuilder::<Postgres>::new("");
        vec![qb]
    }
}

impl InsertDB for FeedMessage {
    fn insert_into(qb: &mut sqlx::QueryBuilder<sqlx::Postgres>) {
        todo!()
    }

    fn value(self, qb: &mut sqlx::query_builder::Separated<sqlx::Postgres, &'static str>) {
        todo!()
    }
}

impl InsertDB for FeedHeader {
    fn insert_into(qb: &mut sqlx::QueryBuilder<sqlx::Postgres>) {
        todo!()
    }

    fn value(self, qb: &mut sqlx::query_builder::Separated<sqlx::Postgres, &'static str>) {
        todo!()
    }
}

impl InsertDB for FeedEntity {
    fn insert_into(qb: &mut sqlx::QueryBuilder<sqlx::Postgres>) {
        todo!()
    }

    fn value(self, qb: &mut sqlx::query_builder::Separated<sqlx::Postgres, &'static str>) {
        todo!()
    }
}

impl InsertDB for TripUpdate {
    fn insert_into(qb: &mut sqlx::QueryBuilder<sqlx::Postgres>) {
        todo!()
    }

    fn value(self, qb: &mut sqlx::query_builder::Separated<sqlx::Postgres, &'static str>) {
        todo!()
    }
}

impl InsertDB for StopTimeUpdate {
    fn insert_into(qb: &mut sqlx::QueryBuilder<sqlx::Postgres>) {
        todo!()
    }

    fn value(self, qb: &mut sqlx::query_builder::Separated<sqlx::Postgres, &'static str>) {
        todo!()
    }
}
