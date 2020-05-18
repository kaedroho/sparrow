pub mod tsvector;
pub mod term_dictionary;
pub mod data_dictionary;

use std::collections::hash_map::HashMap;
use std::iter::FromIterator;
use fnv::{FnvHashMap, FnvHashSet};

use tsvector::TSVector;
use term_dictionary::{TermId, TermDictionary};
use data_dictionary::{FieldId, DataDictionary};

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, serde_derive::Serialize)]
#[serde(transparent)]
pub struct DocumentId(u32);

#[derive(Debug, Clone, serde_derive::Serialize, serde_derive::Deserialize)]
pub struct Token {
    pub term: String,
    pub position: usize,
}

#[derive(Debug, Clone, serde_derive::Serialize, serde_derive::Deserialize)]
pub struct DocumentSource {
    pub fields: HashMap<String, Vec<Token>>,
}

impl DocumentSource {
    pub fn as_document(&self, term_dict: &mut TermDictionary, data_dict: &DataDictionary) -> Document {
        let mut fields = FnvHashMap::default();
        let mut copy_fields = FnvHashMap::default();

        for (field, tokens) in &self.fields {
            if let Some((field_id, field_config)) = data_dict.get_by_name(field) {
                let mut tsvector = TSVector::from_tokens(tokens, term_dict);
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

#[derive(Debug, Clone)]
pub struct Document {
    pub fields: FnvHashMap<FieldId, TSVector>,
}

#[derive(Debug, Default)]
pub struct InvertedIndex {
    pub postings: FnvHashMap<TermId, Vec<(DocumentId, FnvHashSet<usize>, f32)>>,
    pub total_documents: usize,
    pub total_terms: usize,
}

impl InvertedIndex {
    pub fn insert_tsvector(&mut self, document_id: DocumentId, tsvector: &TSVector) {
        for (term, term_info) in &tsvector.terms {
            let postings_list = self.postings.entry(*term).or_default();
            postings_list.push((document_id, FnvHashSet::from_iter(term_info.positions.iter().cloned()), term_info.weight));
        }

        self.total_documents += 1;
        self.total_terms += tsvector.length;
    }

    pub fn term_document_frequency(&self, term: TermId) -> usize {
        self.postings.get(&term).map(|postings_list| postings_list.len()).unwrap_or(0)
    }

    pub fn term_total_frequency(&self, term: TermId) -> usize {
        self.postings.get(&term).map(|postings_list| postings_list.iter().map(|posting| posting.1.len()).sum()).unwrap_or(0)
    }

    pub fn docs_with_term(&self, term: TermId) -> Vec<DocumentId> {
        self.postings.get(&term).map(|postings_list| postings_list.iter().map(|posting| posting.0).collect()).unwrap_or_default()
    }

    pub fn docs_with_phrase(&self, terms: &Vec<TermId>) -> Vec<DocumentId> {
        // Get posting list for each term. Only continue if all terms have a posting list
        let posting_lists = match terms.into_iter().map(|term| self.postings.get(term)).collect::<Option<Vec<_>>>() {
            Some(posting_lists) => posting_lists,
            None => return Vec::new(),
        };

        // Initialise results with values from first posting list
        let first_posting_list = match posting_lists.first() {
            Some(first_posting_list) => first_posting_list,
            None => return Vec::new(),
        };
        let mut results: FnvHashMap<DocumentId, FnvHashSet<usize>> = first_posting_list.iter().map(|(document_id, positions, _)| (*document_id, positions.clone())).collect();

        // For each subsequent term, check that each document contains the term in the position after the previous one
        for posting_list in posting_lists.into_iter().skip(1) {
            let mut seen_docs = FnvHashSet::default();
            for (document_id, positions, _) in posting_list {
                if let Some(result) = results.get_mut(document_id) {
                    seen_docs.insert(document_id);
                    *result = result.iter().filter(|position| positions.contains(&(*position + 1))).map(|position| position + 1).collect();
                }
            }

            // Remove any documents that either didn't contain that term or didn't have any positions that are straight after the previous term
            results = results.into_iter().filter(|(document_id, positions)| seen_docs.contains(document_id) && !positions.is_empty()).collect();
        }

        results.into_iter().map(|(document_id, _)| document_id).collect()
    }

    fn calculate_normalizer(&self, term: TermId) -> f32 {
        let inverse_document_frequency = 1.0 / (self.term_document_frequency(term) as f32 + 1.0).log2();
        let field_length_normalizer = 1.0 / (self.total_documents as f32 / self.total_terms as f32);
        inverse_document_frequency * field_length_normalizer
    }

    pub fn search(&self, term: TermId) -> Vec<(DocumentId, f32)> {
        let normalizer = self.calculate_normalizer(term);
        self.postings.get(&term).map(|postings_list| postings_list.iter().map(|posting| (posting.0, posting.2 * normalizer)).collect()).unwrap_or_default()
    }

    pub fn phrase_search(&self, terms: &Vec<TermId>) -> Vec<(DocumentId, f32)> {
        // Get posting list for each term. Only continue if all terms have a posting list
        let posting_lists = match terms.into_iter().map(|term| self.postings.get(term).map(|posting_list| (term, posting_list))).collect::<Option<Vec<_>>>() {
            Some(posting_lists) => posting_lists,
            None => return Vec::new(),
        };

        // Initialise results with values from first posting list
        let first_posting_list = match posting_lists.first() {
            Some(first_posting_list) => first_posting_list,
            None => return Vec::new(),
        };
        let normalizer = self.calculate_normalizer(*first_posting_list.0);
        let mut results: FnvHashMap<DocumentId, (FnvHashSet<usize>, f32)> = first_posting_list.1.iter().map(|(document_id, positions, weight)| (*document_id, (positions.clone(), weight * normalizer))).collect();

        // For each subsequent term, check that each document contains the term in the position after the previous one
        for (term, posting_list) in posting_lists.into_iter().skip(1) {
            let normalizer = self.calculate_normalizer(*term);
            let mut seen_docs = FnvHashSet::default();

            for (document_id, positions, weight) in posting_list {
                if let Some(result) = results.get_mut(document_id) {
                    seen_docs.insert(document_id);
                    result.0 = result.0.iter().filter(|position| positions.contains(&(*position + 1))).map(|position| position + 1).collect();
                    result.1 += weight * normalizer;
                }
            }

            // Remove any documents that either didn't contain that term or didn't have any positions that are straight after the previous term
            results = results.into_iter().filter(|(document_id, (positions, _))| seen_docs.contains(document_id) && !positions.is_empty()).collect()
        }

        results.into_iter().map(|(document_id, (_, score))| (document_id, score)).collect()
    }
}

#[derive(Debug, Clone, serde_derive::Serialize, serde_derive::Deserialize)]
pub enum Query {
    MatchAll,
    MatchNone,
    Term(FieldId, TermId),
    Phrase(FieldId, Vec<TermId>),
    Or(Vec<Query>),
    And(Vec<Query>),
    Filter(Box<Query>, Box<Query>),
    Boost(Box<Query>, f32),
}

#[derive(Debug, Default)]
pub struct Database {
    next_document_id: u32,
    pub term_dictionary: TermDictionary,
    pub data_dictionary: DataDictionary,
    pub fields: FnvHashMap<FieldId, InvertedIndex>,
    pub docs: FnvHashMap<DocumentId, Document>,
    pub deleted_docs: FnvHashSet<DocumentId>,
}

impl Database {
    pub fn insert_document(&mut self, source: DocumentSource) -> DocumentId {
        let doc = source.as_document(&mut self.term_dictionary, &self.data_dictionary);

        let id = DocumentId(self.next_document_id);
        self.next_document_id += 1;
        for (field_id, tsvector) in &doc.fields {
            let field = self.fields.entry(*field_id).or_default();
            field.insert_tsvector(id, tsvector);
        }
        self.docs.insert(id, doc);
        id
    }

    pub fn delete_document(&mut self, document_id: DocumentId) {
        self.deleted_docs.insert(document_id);
    }

    pub fn simple_match(&self, query: &Query) -> Vec<DocumentId> {
        match query {
            Query::MatchAll => {
                (0..self.next_document_id as u32).map(|i| DocumentId(i as u32)).filter(|document_id| !self.deleted_docs.contains(document_id)).collect()
            }
            Query::MatchNone => {
                Vec::new()
            }
            Query::Term(field_id, term_id) => {
                if let Some(field) = self.fields.get(field_id) {
                    field.docs_with_term(*term_id).into_iter().filter(|document_id| !self.deleted_docs.contains(document_id)).collect()
                } else {
                    Vec::new()
                }
            }
            Query::Phrase(field_id, terms) => {
                if let Some(field) = self.fields.get(field_id) {
                    field.docs_with_phrase(terms).into_iter().filter(|document_id| !self.deleted_docs.contains(document_id)).collect()
                } else {
                    Vec::new()
                }
            }
            Query::Or(queries) => {
                let mut results: FnvHashSet<DocumentId> = FnvHashSet::default();

                for query in queries {
                    for document_id in self.simple_match(&query) {
                        results.insert(document_id);
                    }
                }

                results.into_iter().collect()
            }
            Query::And(queries) => {
                let mut results: FnvHashMap<DocumentId, usize> = FnvHashMap::default();

                for query in queries {
                    for document_id in self.simple_match(&query) {
                        let result = results.entry(document_id).or_default();
                        *result += 1;
                    }
                }

                results.into_iter().filter(|(_, result)| *result == queries.len()).map(|(document_id, _)| document_id).collect()
            }
            Query::Filter(query, filter) => {
                self.simple_match(&Query::And(vec![*query.clone(), *filter.clone()]))
            }
            Query::Boost(query, _boost) => {
                self.simple_match(&query)
            }
        }
    }

    pub fn query(&self, query: &Query) -> Vec<(DocumentId, f32)> {
        match query {
            Query::MatchAll => {
                (0..self.next_document_id).map(|i| DocumentId(i as u32)).filter(|document_id| !self.deleted_docs.contains(document_id)).map(|document_id| (document_id, 0.0)).collect()
            }
            Query::MatchNone => {
                Vec::new()
            }
            Query::Term(field_id, term_id) => {
                if let Some(field) = self.fields.get(field_id) {
                    field.search(*term_id).into_iter().filter(|(document_id, _)| !self.deleted_docs.contains(document_id)).collect()
                } else {
                    Vec::new()
                }
            }
            Query::Phrase(field_id, terms) => {
                if let Some(field) = self.fields.get(field_id) {
                    field.phrase_search(terms).into_iter().filter(|(document_id, _)| !self.deleted_docs.contains(document_id)).collect()
                } else {
                    Vec::new()
                }
            }
            Query::Or(queries) => {
                let mut results: FnvHashMap<DocumentId, f32> = FnvHashMap::default();

                for query in queries {
                    for (document_id, score) in self.query(&query) {
                        *results.entry(document_id).or_default() += score;
                    }
                }

                results.into_iter().collect()
            }
            Query::And(queries) => {
                #[derive(Default)]
                struct Result {
                    score: f32,
                    query_count: usize,
                }

                let mut results: FnvHashMap<DocumentId, Result> = FnvHashMap::default();

                for query in queries {
                    for (document_id, score) in self.query(&query) {
                        let result = results.entry(document_id).or_default();
                        result.score += score;
                        result.query_count += 1;
                    }
                }

                results.into_iter().filter(|(_, result)| result.query_count == queries.len()).map(|(document_id, result)| (document_id, result.score)).collect()
            }
            Query::Filter(query, filter) => {
                #[derive(Default)]
                struct Result {
                    score: f32,
                    passed_filter: bool,
                }

                let mut results: FnvHashMap<DocumentId, Result> = FnvHashMap::default();

                for (document_id, score) in self.query(&query) {
                    let result = results.entry(document_id).or_default();
                    result.score += score;
                }


                for document_id in self.simple_match(&filter) {
                    if let Some(result) = results.get_mut(&document_id) {
                        result.passed_filter = true;
                    }
                }

                results.into_iter().filter(|(_, result)| result.passed_filter).map(|(document_id, result)| (document_id, result.score)).collect()
            }
            Query::Boost(query, boost) => {
                if *boost == 0.0 {
                    self.simple_match(&query).into_iter().map(|document_id| (document_id, 0.0)).collect()
                } else {
                    self.query(&query).into_iter().map(|(document_id, score)| (document_id, score * boost)).collect()
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use super::{Database, DocumentSource, Token};

    pub fn tokenize_string(string: &str) -> Vec<Token> {
        let mut current_position = 0;
        string.split_whitespace().map(|string| {
            current_position += 1;
            Token { term: string.trim_matches(|c: char| !c.is_alphanumeric()).to_lowercase(), position: current_position }
        }).filter(|token| token.term.len() < 100).collect()
    }

    #[test]
    fn it_works() {
        let mut db = Database::default();
        let mut fields = HashMap::new();
        fields.insert("title".to_owned(), tokenize_string("hello world this is a test hello"));
        db.insert_document(DocumentSource { fields });
    }
}
