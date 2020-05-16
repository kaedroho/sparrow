pub mod tsvector;
pub mod tokenize;
pub mod term_dictionary;

use std::collections::hash_map::HashMap;
use fnv::FnvHashMap;

use tsvector::TSVector;
use term_dictionary::TermDictionary;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, serde_derive::Serialize)]
pub struct DocumentId(u32);

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, serde_derive::Serialize)]
pub struct TermId(u32);

#[derive(Debug)]
pub struct DocumentSource {
    pub fields: HashMap<String, TSVector>,
}

#[derive(Debug, Default, serde_derive::Serialize)]
pub struct InvertedIndex {
    pub postings: FnvHashMap<TermId, Vec<(DocumentId, Vec<usize>, f32)>>,
    pub total_documents: usize,
    pub total_terms: usize,
}

impl InvertedIndex {
    pub fn insert_tsvector(&mut self, document: DocumentId, tsvector: TSVector) {
        for (term, term_info) in tsvector.terms {
            let postings_list = self.postings.entry(term).or_default();
            postings_list.push((document, term_info.positions, term_info.weight));
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

    pub fn search(&self, term: TermId) -> Vec<(DocumentId, f32)> {
        let inverse_document_frequency = 1.0 / (self.term_document_frequency(term) as f32 + 1.0).log2();
        let field_length_normalizer = 1.0 / (self.total_documents as f32 / self.total_terms as f32);
        let normalizer = inverse_document_frequency * field_length_normalizer;

        self.postings.get(&term).map(|postings_list| postings_list.iter().map(|posting| (posting.0, posting.2 * normalizer)).collect()).unwrap_or_default()
    }
}

#[derive(Debug, Default, serde_derive::Serialize)]
pub struct Database {
    next_document_id: u32,
    pub dictionary: TermDictionary,
    pub fields: HashMap<String, InvertedIndex>,
}

impl Database {
    pub fn insert_document(&mut self, source: DocumentSource) -> DocumentId {
        let id = DocumentId(self.next_document_id);
        self.next_document_id += 1;
        for (field_name, tsvector) in source.fields {
            let field = self.fields.entry(field_name.to_owned()).or_default();
            field.insert_tsvector(id, tsvector);
        }
        id
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use super::tsvector::TSVector;
    use super::tokenize::tokenize_string;
    use super::{Database, DocumentSource};

    #[test]
    fn it_works() {
        let mut db = Database::default();
        let tokens = tokenize_string("hello world this is a test hello");
        let tsvector = TSVector::from_tokens(&tokens, &mut db.dictionary);
        dbg!(tokens);
        dbg!(&tsvector);

        let mut fields = HashMap::new();
        fields.insert("title".to_owned(), tsvector);
        db.insert_document(DocumentSource { fields });
    }
}
