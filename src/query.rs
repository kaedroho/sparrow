use super::term_dictionary::TermId;
use super::data_dictionary::FieldId;

#[derive(Debug, Clone, PartialEq, serde_derive::Serialize, serde_derive::Deserialize)]
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
        // Allocate new vec with the assumption that it would be the same size
        let mut processed_queries = Vec::with_capacity(queries.len());
        let mut contained_match_all = false;

        for query in queries {
            match query {
                Query::Term(..) | Query::Phrase(..) | Query::And(..) | Query::Filter(..) | Query::Exclude(..) | Query::Boost(..) => processed_queries.push(query),

                // Ignore MatchNone in Or queries
                Query::MatchNone => {},

                // Ignore MatchAll in Or queries
                // But we reinsert one if at least one exists so that the query still matches everything
                Query::MatchAll => contained_match_all = true,

                // Nest any Or queries
                // Note; we assume the nested Or query is already optimised
                Query::Or(queries) => processed_queries.extend(queries),
            }
        }

        if contained_match_all {
            processed_queries.push(Query::MatchAll);
        }

        match processed_queries.len() {
            // Original query either had no subqueries or they were all MatchNone
            0 => Query::MatchNone,

            1 => processed_queries.pop().unwrap(),

            _ => Query::Or(processed_queries),
        }
    }

    pub fn and(queries: Vec<Query>) -> Query {
        // Allocate new vec with the assumption that it would be the same size
        let mut processed_queries = Vec::with_capacity(queries.len());
        let mut contained_match_all = false;

        for query in queries {
            match query {
                Query::Term(..) | Query::Phrase(..) | Query::Or(..) | Query::Filter(..) | Query::Exclude(..) | Query::Boost(..) => processed_queries.push(query),

                // Ignore everything if there's a MatchNone
                Query::MatchNone => return Query::MatchNone,

                // Ignore MatchAll in And queries
                Query::MatchAll => contained_match_all = true,

                // Nest any And queries
                // Note; we assume the nested And query is already optimised
                Query::And(queries) => processed_queries.extend(queries),
            }
        }

        match processed_queries.len() {
            // Original query either had no subqueries or they were all MatchAll
            0 => if contained_match_all { Query::MatchAll } else { Query::MatchNone },

            1 => processed_queries.pop().unwrap(),

            _ => Query::And(processed_queries),
        }
    }

    pub fn not(query: Query) -> Query {
        Query::exclude(Query::match_all(), query)
    }

    pub fn filter(query: Query, filter: Query) -> Query {
        match (&query, &filter) {
            (Query::MatchNone, _) => Query::match_none(),
            (_, Query::MatchAll) => query,
            (_, Query::MatchNone) => Query::match_none(),
            (_, Query::Filter(filter_query, filter)) if **filter_query == Query::MatchAll => Query::filter(query, *filter.clone()),
            (_, Query::Exclude(filter_query, filter)) if **filter_query == Query::MatchAll => Query::exclude(query, *filter.clone()),
            _ => Query::Filter(Box::new(query), Box::new(filter)),
        }
    }

    pub fn exclude(query: Query, filter: Query) -> Query {
        match (&query, &filter) {
            (Query::MatchNone, _) => Query::match_none(),
            (_, Query::MatchAll) => Query::match_none(),
            (_, Query::MatchNone) => query,
            (_, Query::Filter(exclude_query, filter)) if **exclude_query == Query::MatchAll => Query::exclude(query, *filter.clone()),
            (_, Query::Exclude(exclude_query, filter)) if **exclude_query == Query::MatchAll => Query::filter(query, *filter.clone()),
            _ => Query::Exclude(Box::new(query), Box::new(filter)),
        }
    }

    pub fn boost(query: Query, boost: f32) -> Query {
        Query::Boost(Box::new(query), boost)
    }
}

#[cfg(test)]
mod tests {
    use crate::term_dictionary::TermId;
    use crate::data_dictionary::FieldId;
    use super::Query;

    #[test]
    fn test_match_all() {
        assert_eq!(Query::match_all(), Query::MatchAll);
    }

    #[test]
    fn test_match_none() {
        assert_eq!(Query::match_none(), Query::MatchNone);
    }

    #[test]
    fn test_term() {
        assert_eq!(Query::term(FieldId(1), TermId(123)), Query::Term(FieldId(1), TermId(123)));
    }

    #[test]
    fn test_phrase() {
        assert_eq!(Query::phrase(FieldId(1), vec![TermId(123), TermId(456), TermId(789)]), Query::Phrase(FieldId(1),  vec![TermId(123), TermId(456), TermId(789)]));
    }

    #[test]
    fn test_or() {
        assert_eq!(
            Query::or(vec![Query::Term(FieldId(1), TermId(123)), Query::Term(FieldId(1), TermId(456))]),
            Query::Or(vec![Query::Term(FieldId(1), TermId(123)), Query::Term(FieldId(1), TermId(456))])
        );

        // Nested Or queries should be inlined
        assert_eq!(
            Query::or(vec![Query::Or(vec![Query::Term(FieldId(1), TermId(123)), Query::Term(FieldId(1), TermId(456))]), Query::Term(FieldId(1), TermId(789))]),
            Query::Or(vec![Query::Term(FieldId(1), TermId(123)), Query::Term(FieldId(1), TermId(456)), Query::Term(FieldId(1), TermId(789))])
        );

        // But not And queries
        assert_eq!(
            Query::or(vec![Query::And(vec![Query::Term(FieldId(1), TermId(123)), Query::Term(FieldId(1), TermId(456))]), Query::Term(FieldId(1), TermId(789))]),
            Query::Or(vec![Query::And(vec![Query::Term(FieldId(1), TermId(123)), Query::Term(FieldId(1), TermId(456))]), Query::Term(FieldId(1), TermId(789))])
        );

        // Single element queries should be unwrapped
        assert_eq!(
            Query::or(vec![Query::Term(FieldId(1), TermId(123))]),
            Query::Term(FieldId(1), TermId(123))
        );

        // MatchNone should be ignored
        assert_eq!(
            Query::or(vec![Query::Term(FieldId(1), TermId(123)), Query::MatchNone]),
            Query::Term(FieldId(1), TermId(123))
        );

        // Or query containing only MatchNone should be MatchNone
        assert_eq!(
            Query::or(vec![Query::MatchNone, Query::MatchNone]),
            Query::MatchNone
        );

        // MatchAll should be left in
        // This is so that the query still matches everything and the scoring for that term still applies
        assert_eq!(
            Query::or(vec![Query::Term(FieldId(1), TermId(123)), Query::MatchAll]),
            Query::Or(vec![Query::Term(FieldId(1), TermId(123)), Query::MatchAll]),
        );

        // Multiple MatchAll should be reduced to one
        assert_eq!(
            Query::or(vec![Query::Term(FieldId(1), TermId(123)), Query::MatchAll, Query::MatchAll]),
            Query::Or(vec![Query::Term(FieldId(1), TermId(123)), Query::MatchAll]),
        );

        // If all MatchAll, then the query should be MatchAll
        assert_eq!(
            Query::or(vec![Query::MatchAll, Query::MatchAll]),
            Query::MatchAll
        );
    }

    #[test]
    fn test_and() {
        assert_eq!(
            Query::and(vec![Query::Term(FieldId(1), TermId(123)), Query::Term(FieldId(1), TermId(456))]),
            Query::And(vec![Query::Term(FieldId(1), TermId(123)), Query::Term(FieldId(1), TermId(456))])
        );

        // Nested And queries should be inlined
        assert_eq!(
            Query::and(vec![Query::And(vec![Query::Term(FieldId(1), TermId(123)), Query::Term(FieldId(1), TermId(456))]), Query::Term(FieldId(1), TermId(789))]),
            Query::And(vec![Query::Term(FieldId(1), TermId(123)), Query::Term(FieldId(1), TermId(456)), Query::Term(FieldId(1), TermId(789))])
        );

        // But not Or queries
        assert_eq!(
            Query::and(vec![Query::Or(vec![Query::Term(FieldId(1), TermId(123)), Query::Term(FieldId(1), TermId(456))]), Query::Term(FieldId(1), TermId(789))]),
            Query::And(vec![Query::Or(vec![Query::Term(FieldId(1), TermId(123)), Query::Term(FieldId(1), TermId(456))]), Query::Term(FieldId(1), TermId(789))])
        );

        // Single element queries should be unwrapped
        assert_eq!(
            Query::and(vec![Query::Term(FieldId(1), TermId(123))]),
            Query::Term(FieldId(1), TermId(123))
        );

        // If any MatchNone, the query should be MatchNone
        assert_eq!(
            Query::and(vec![Query::Term(FieldId(1), TermId(123)), Query::MatchNone]),
            Query::MatchNone
        );

        // MatchAll should be ignored
        assert_eq!(
            Query::and(vec![Query::Term(FieldId(1), TermId(123)), Query::MatchAll]),
            Query::Term(FieldId(1), TermId(123))
        );

        // If all MatchAll, then the query should be MatchAll
        assert_eq!(
            Query::and(vec![Query::MatchAll, Query::MatchAll]),
            Query::MatchAll
        );
    }

    #[test]
    fn test_not() {
        assert_eq!(
            Query::not(Query::Term(FieldId(1), TermId(123))),
            Query::Exclude(Box::new(Query::MatchAll), Box::new(Query::Term(FieldId(1), TermId(123))))
        );

        assert_eq!(
            Query::not(Query::MatchAll),
            Query::MatchNone
        );

        assert_eq!(
            Query::not(Query::MatchNone),
            Query::MatchAll
        );

        assert_eq!(
            Query::not(Query::not(Query::Term(FieldId(1), TermId(123)))),
            Query::Filter(Box::new(Query::MatchAll), Box::new(Query::Term(FieldId(1), TermId(123))))
        );
    }

    #[test]
    fn test_filter() {
        assert_eq!(
            Query::filter(Query::Term(FieldId(1), TermId(123)), Query::Term(FieldId(1), TermId(456))),
            Query::Filter(Box::new(Query::Term(FieldId(1), TermId(123))), Box::new(Query::Term(FieldId(1), TermId(456))))
        );

        assert_eq!(
            Query::filter(Query::MatchAll, Query::Term(FieldId(1), TermId(456))),
            Query::Filter(Box::new(Query::MatchAll), Box::new(Query::Term(FieldId(1), TermId(456))))
        );

        assert_eq!(
            Query::filter(Query::Term(FieldId(1), TermId(123)), Query::MatchAll),
            Query::Term(FieldId(1), TermId(123))
        );

        assert_eq!(
            Query::filter(Query::MatchNone, Query::MatchAll),
            Query::MatchNone
        );

        assert_eq!(
            Query::filter(Query::MatchAll, Query::MatchNone),
            Query::MatchNone
        );

        // When you nest two filters and the inner one has a query of MatchAll, we can simplify this
        assert_eq!(
            Query::filter(
                Query::Term(FieldId(1), TermId(123)),
                Query::Filter(
                    Box::new(Query::MatchAll),
                    Box::new(Query::Term(FieldId(1), TermId(456)))
                )
            ),
            Query::Filter(
                Box::new(Query::Term(FieldId(1), TermId(123))),
                Box::new(Query::Term(FieldId(1), TermId(456)))
            )
        );

        // But not when the inner filter's query isn't MatchAll
        assert_eq!(
            Query::filter(
                Query::Term(FieldId(1), TermId(123)),
                Query::Filter(
                    Box::new(Query::Term(FieldId(1), TermId(456))),
                    Box::new(Query::Term(FieldId(1), TermId(789)))
                )
            ),
            Query::Filter(
                Box::new(Query::Term(FieldId(1), TermId(123))),
                Box::new(Query::Filter(
                    Box::new(Query::Term(FieldId(1), TermId(456))),
                    Box::new(Query::Term(FieldId(1), TermId(789)))
                ))
            )
        );

        // We can also simplify when a filter is filtered by an exclude who's query is MatchAll
        assert_eq!(
            Query::filter(
                Query::Term(FieldId(1), TermId(123)),
                Query::Exclude(
                    Box::new(Query::MatchAll),
                    Box::new(Query::Term(FieldId(1), TermId(456)))
                )
            ),
            Query::Exclude(
                Box::new(Query::Term(FieldId(1), TermId(123))),
                Box::new(Query::Term(FieldId(1), TermId(456))),
            )
        );
    }

    #[test]
    fn test_exclude() {
        assert_eq!(
            Query::exclude(Query::Term(FieldId(1), TermId(123)), Query::Term(FieldId(1), TermId(456))),
            Query::Exclude(Box::new(Query::Term(FieldId(1), TermId(123))), Box::new(Query::Term(FieldId(1), TermId(456))))
        );

        assert_eq!(
            Query::exclude(Query::MatchAll, Query::Term(FieldId(1), TermId(456))),
            Query::Exclude(Box::new(Query::MatchAll), Box::new(Query::Term(FieldId(1), TermId(456))))
        );

        assert_eq!(
            Query::exclude(Query::Term(FieldId(1), TermId(123)), Query::MatchNone),
            Query::Term(FieldId(1), TermId(123))
        );

        assert_eq!(
            Query::exclude(Query::MatchNone, Query::MatchAll),
            Query::MatchNone
        );

        assert_eq!(
            Query::exclude(Query::MatchAll, Query::MatchNone),
            Query::MatchAll
        );

        // When you nest two excludes and the inner one has a query of MatchAll, we can simplify this
        assert_eq!(
            Query::exclude(
                Query::Term(FieldId(1), TermId(123)),
                Query::Exclude(
                    Box::new(Query::MatchAll),
                    Box::new(Query::Term(FieldId(1), TermId(456)))
                )
            ),
            Query::Filter(
                Box::new(Query::Term(FieldId(1), TermId(123))),
                Box::new(Query::Term(FieldId(1), TermId(456)))
            )
        );

        // But not when the inner exclude's query isn't MatchAll
        assert_eq!(
            Query::exclude(
                Query::Term(FieldId(1), TermId(123)),
                Query::Filter(
                    Box::new(Query::Term(FieldId(1), TermId(456))),
                    Box::new(Query::Term(FieldId(1), TermId(789)))
                )
            ),
            Query::Exclude(
                Box::new(Query::Term(FieldId(1), TermId(123))),
                Box::new(Query::Filter(
                    Box::new(Query::Term(FieldId(1), TermId(456))),
                    Box::new(Query::Term(FieldId(1), TermId(789)))
                ))
            )
        );

        // We can also simplify when an exclude is filtered by a filter who's query is MatchAll
        assert_eq!(
            Query::exclude(
                Query::Term(FieldId(1), TermId(123)),
                Query::Filter(
                    Box::new(Query::MatchAll),
                    Box::new(Query::Term(FieldId(1), TermId(456)))
                )
            ),
            Query::Exclude(
                Box::new(Query::Term(FieldId(1), TermId(123))),
                Box::new(Query::Term(FieldId(1), TermId(456))),
            )
        );
    }

    #[test]
    fn test_boost() {
        assert_eq!(Query::boost(Query::Term(FieldId(1), TermId(123)), 2.0), Query::Boost(Box::new(Query::Term(FieldId(1), TermId(123))), 2.0));
    }
}
