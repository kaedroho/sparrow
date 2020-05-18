use super::term_dictionary::TermId;
use super::data_dictionary::FieldId;

#[derive(Debug, Clone, serde_derive::Serialize, serde_derive::Deserialize)]
pub enum Query {
    MatchAll,
    MatchNone,
    Term(FieldId, TermId),
    Phrase(FieldId, Vec<TermId>),
    Or(Vec<Query>),
    And(Vec<Query>),
    Filter(Box<Query>, Box<Query>),
    Exclude(Box<Query>, Box<Query>),
    Boost(Box<Query>, f32),
}

impl Query {
    pub fn match_all() -> Query {
        Query::MatchAll
    }

    pub fn match_none() -> Query {
        Query::MatchNone
    }

    pub fn term(field: FieldId, term: TermId) -> Query {
        Query::Term(field, term)
    }

    pub fn phrase(field: FieldId, terms: Vec<TermId>) -> Query {
        Query::Phrase(field, terms)
    }

    pub fn or(queries: Vec<Query>) -> Query {
        Query::Or(queries)
    }

    pub fn and(queries: Vec<Query>) -> Query {
        Query::And(queries)
    }

    pub fn not(query: Query) -> Query {
        Query::exclude(Query::match_all(), query)
    }

    pub fn filter(query: Query, filter: Query) -> Query {
        Query::Filter(Box::new(query), Box::new(filter))
    }

    pub fn exclude(query: Query, filter: Query) -> Query {
        Query::Exclude(Box::new(query), Box::new(filter))
    }

    pub fn boost(query: Query, boost: f32) -> Query {
        Query::Boost(Box::new(query), boost)
    }
}
