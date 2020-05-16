pub mod tsvector;
pub mod term_dictionary;

use std::collections::hash_map::HashMap;
use fnv::FnvHashMap;

use tsvector::TSVector;
use term_dictionary::TermDictionary;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, serde_derive::Serialize)]
#[serde(transparent)]
pub struct DocumentId(u32);

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, serde_derive::Serialize, serde_derive::Deserialize)]
#[serde(transparent)]
pub struct TermId(u32);

#[derive(Debug, Clone, serde_derive::Serialize, serde_derive::Deserialize)]
pub struct Token {
    pub term: String,
    pub position: usize,
    #[serde(default = "default_token_weight")]
    pub weight: f32,
}

fn default_token_weight() -> f32 {
    1.0
}

#[derive(Debug, Clone, serde_derive::Serialize, serde_derive::Deserialize)]
pub struct DocumentSource {
    pub fields: HashMap<String, Vec<Token>>,
}

impl DocumentSource {
    pub fn as_document(&self, term_dict: &mut TermDictionary) -> Document {
        let mut fields = HashMap::new();

        for (field, tokens) in &self.fields {
            fields.insert(field.to_owned(), TSVector::from_tokens(tokens, term_dict));
        }

        Document { fields }
    }
}

#[derive(Debug, Clone)]
pub struct Document {
    pub fields: HashMap<String, TSVector>,
}

#[derive(Debug, Default)]
pub struct InvertedIndex {
    pub postings: FnvHashMap<TermId, Vec<(DocumentId, Vec<usize>, f32)>>,
    pub total_documents: usize,
    pub total_terms: usize,
}

impl InvertedIndex {
    pub fn insert_tsvector(&mut self, document: DocumentId, tsvector: &TSVector) {
        for (term, term_info) in &tsvector.terms {
            let postings_list = self.postings.entry(*term).or_default();
            postings_list.push((document, term_info.positions.clone(), term_info.weight));
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

#[derive(Debug, Default)]
pub struct Database {
    next_document_id: u32,
    pub dictionary: TermDictionary,
    pub fields: HashMap<String, InvertedIndex>,
    pub docs: HashMap<DocumentId, Document>,
}

impl Database {
    pub fn insert_document(&mut self, source: DocumentSource) -> DocumentId {
        let doc = source.as_document(&mut self.dictionary);

        let id = DocumentId(self.next_document_id);
        self.next_document_id += 1;
        for (field_name, tsvector) in &doc.fields {
            let field = self.fields.entry(field_name.to_owned()).or_default();
            field.insert_tsvector(id, tsvector);
        }
        self.docs.insert(id, doc);
        id
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
            Token { term: string.trim_matches(|c: char| !c.is_alphanumeric()).to_lowercase(), weight: 1.0, position: current_position }
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
