#![feature(proc_macro_hygiene, decl_macro)]

#[macro_use] extern crate rocket;

use std::collections::HashMap;
use rocket::State;
use rocket_contrib::json::Json;
use fnv::FnvHashMap;
use std::sync::RwLock;

use sparrow::{Database, Document};
use sparrow::tsvector::{TSVector, TSVectorTerm};
use sparrow::term_dictionary::{TermId, TermDictionary};
use sparrow::data_dictionary::{FieldConfig, DataDictionary};
use sparrow::query::Query;

#[derive(Debug, Clone, serde_derive::Serialize, serde_derive::Deserialize)]
pub struct Token {
    pub term: String,
    pub position: usize,
}

fn tokenvec_to_tsvector(tokenvec: &Vec<Token>, dict: &mut TermDictionary) -> TSVector {
    let mut terms: FnvHashMap<TermId, TSVectorTerm> = FnvHashMap::default();

    for token in tokenvec {
        let term = dict.get_or_insert(&token.term);
        let term_entry = terms.entry(term).or_default();
        term_entry.positions.push(token.position);
        term_entry.weight += 1.0;
    }

    TSVector { terms, length: tokenvec.len() }
}

#[derive(Debug, Clone, serde_derive::Serialize, serde_derive::Deserialize)]
pub struct DocumentSource {
    pub pk: String,
    pub fields: HashMap<String, Vec<Token>>,
}

impl DocumentSource {
    pub fn as_document(&self, term_dict: &mut TermDictionary, data_dict: &DataDictionary) -> Document {
        let mut fields = FnvHashMap::default();
        let mut copy_fields = FnvHashMap::default();

        for (field, tokens) in &self.fields {
            if let Some((field_id, field_config)) = data_dict.get_by_name(field) {
                let mut tsvector = tokenvec_to_tsvector(tokens, term_dict);
                // Apply field boost and document length normalisation
                // Note: we multiply the weight by the average field length at query time
                tsvector.boost(field_config.boost / tsvector.length as f32);
                fields.insert(field_id, tsvector);

                if !field_config.copy_to.is_empty() {
                    copy_fields.insert(field_id, field_config.copy_to.clone());
                }
            }
        }

        for (source_field, destination_fields) in copy_fields {
            if let Some(source) = fields.get(&source_field) {
                // Work around borrow checker
                // FIXME: Make this faster
                let source = source.clone();

                for destination_field in destination_fields {
                    let destination = fields.entry(destination_field).or_default();
                    destination.append(&source);
                }
            }
        }

        Document { fields }
    }
}

#[derive(Debug, Clone, serde_derive::Serialize, serde_derive::Deserialize)]
pub enum QuerySource {
    MatchAll,
    MatchNone,
    Term {
        field: String,
        term: String,
    },
    Phrase {
        field: String,
        terms: Vec<String>,
    },
    Or(Vec<QuerySource>),
    And(Vec<QuerySource>),
    Filter {
        query: Box<QuerySource>,
        filter: Box<QuerySource>,
    },
    Exclude {
        query: Box<QuerySource>,
        filter: Box<QuerySource>,
    },
    Boost {
        query: Box<QuerySource>,
        boost: f32
    },
}

impl QuerySource {
    pub fn as_query(&self, term_dict: &TermDictionary, data_dict: &DataDictionary) -> Query {
        match self {
            QuerySource::MatchAll => Query::match_all(),
            QuerySource::MatchNone => Query::match_all(),
            QuerySource::Term { field, term } => {
                if let Some(field_id) = data_dict.field_names.get(field) {
                    if let Some(term_id) = term_dict.terms.get(term) {
                        return Query::term(*field_id, *term_id);
                    }
                }

                Query::match_none()
            }
            QuerySource::Phrase { field, terms } => {
                if let Some(field_id) = data_dict.field_names.get(field) {
                    if let Some(term_ids) = terms.into_iter().map(|term| term_dict.terms.get(term).cloned()).collect::<Option<Vec<_>>>() {
                        return Query::phrase(*field_id, term_ids);
                    }
                }

                Query::match_none()
            }
            QuerySource::Or(queries) => {
                Query::or(queries.iter().map(|query| query.as_query(&term_dict, &data_dict)).collect())
            }
            QuerySource::And(queries) => {
                Query::and(queries.iter().map(|query| query.as_query(&term_dict, &data_dict)).collect())
            }
            QuerySource::Filter { query, filter } => {
                Query::filter(query.as_query(&term_dict, &data_dict), filter.as_query(&term_dict, &data_dict))
            }
            QuerySource::Exclude { query, filter } => {
                Query::exclude(query.as_query(&term_dict, &data_dict), filter.as_query(&term_dict, &data_dict))
            }
            QuerySource::Boost { query, boost } => {
                Query::boost(query.as_query(&term_dict, &data_dict), *boost)
            }
        }
    }
}

#[get("/")]
fn index() -> &'static str {
    "Hello, world!"
}

#[post("/insert", format = "application/json", data = "<doc>")]
fn insert(doc: Json<DocumentSource>) -> &'static str {
    "Hello, world!"
}

#[post("/bulk", format = "application/json", data = "<docs>")]
fn bulk(db: State<RwLock<Database>>, docs: Json<Vec<DocumentSource>>) -> &'static str {
    let mut db = db.write().unwrap();
    let data_dictionary = db.data_dictionary.clone();
    for source in docs.iter() {
        let doc = source.as_document(&mut db.term_dictionary, &data_dictionary);
        db.insert_document(source.pk.to_owned(), doc);
    }

    "Hello, world!"
}

#[derive(Debug, serde_derive::Serialize)]
struct SearchResult {
    pk: String,
    score: f32,
}

#[post("/search", format = "application/json", data = "<query>")]
fn search(db: State<RwLock<Database>>, query: Json<QuerySource>) -> Json<Vec<SearchResult>> {
    let db = db.read().unwrap();

    let mut documents = db.query(&query.as_query(&db.term_dictionary, &db.data_dictionary));
    documents.sort_by(|a,b| a.1.partial_cmp(&b.1).unwrap().reverse());

    Json(documents.into_iter().map(|(document_id, score)| SearchResult { pk: db.id_to_pk.get(&document_id).expect("Document does not have PK").to_owned(), score }).collect::<Vec<SearchResult>>())
}

#[post("/reset")]
fn reset() -> &'static str {
    "Hello, world!"
}

fn main() {
    let mut db = Database::default();

    let all_text_field = db.data_dictionary.insert("all_text".to_owned(), FieldConfig::default());
    db.data_dictionary.insert("pk".to_owned(), FieldConfig::default());
    db.data_dictionary.insert("content_type".to_owned(), FieldConfig::default());
    db.data_dictionary.insert("_partials".to_owned(), FieldConfig::default());
    db.data_dictionary.insert("name".to_owned(), FieldConfig::default().boost(2.0).copy_to(all_text_field));
    db.data_dictionary.insert("title".to_owned(), FieldConfig::default().boost(2.0).copy_to(all_text_field));
    db.data_dictionary.insert("summary".to_owned(), FieldConfig::default().copy_to(all_text_field));

    rocket::ignite().manage(RwLock::new(db)).mount("/", routes![index, insert, bulk, search, reset]).launch();
}
