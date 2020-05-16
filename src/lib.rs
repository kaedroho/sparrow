use std::collections::hash_map::HashMap;
use std::ops::Add;
use fnv::FnvHashMap;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, serde_derive::Serialize)]
pub struct DocumentId(u32);

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, serde_derive::Serialize)]
pub struct TermId(u32);

#[derive(Debug, Clone)]
pub struct TSVectorTerm {
    pub positions: Vec<usize>,
    pub weight: f32,
}

impl Default for TSVectorTerm {
    fn default() -> TSVectorTerm {
        TSVectorTerm {
            positions: Vec::new(),
            weight: 0.0,
        }
    }
}

#[derive(Debug)]
pub struct TSVector {
    pub length: usize,
    pub terms: FnvHashMap<TermId, TSVectorTerm>,
}

impl Add<&TSVector> for &TSVector {
    type Output = TSVector;

    fn add(self, other: &TSVector) -> TSVector {
        let mut terms = FnvHashMap::default();

        fn add_terms(terms: &mut FnvHashMap<TermId, TSVectorTerm>, terms_to_add: &FnvHashMap<TermId, TSVectorTerm>, start_position: usize) {
            for (term, other_term_info) in terms_to_add {
                if let Some(mut term_info) = terms.get_mut(term) {
                    for position in &other_term_info.positions {
                        term_info.positions.push(start_position + position);
                    }
                    term_info.weight += other_term_info.weight;
                } else {
                    terms.insert(*term, other_term_info.clone());
                }
            }
        }

        add_terms(&mut terms, &self.terms, 0);
        add_terms(&mut terms, &other.terms, self.length);

        TSVector {
            length: self.length + other.length,
            terms,
        }
    }
}

#[derive(Debug)]
pub struct Token {
    pub term: String,
    pub position: usize,
    pub weight: f32,
}

#[derive(Debug)]
pub struct DocumentSource {
    pub fields: HashMap<String, TSVector>,
}

#[derive(Debug, Default, serde_derive::Serialize)]
pub struct TermDictionary {
    next_id: u32,
    pub terms: HashMap<String, TermId>,
    pub term_ids: FnvHashMap<TermId, String>,
}

impl TermDictionary {
    pub fn get_or_insert(&mut self, term: &str) -> TermId {
        if let Some(term_id) = self.terms.get(term) {
            term_id.clone()
        } else {
            let id = TermId(self.next_id);
            self.next_id += 1;
            self.terms.insert(term.to_owned(), id);
            self.term_ids.insert(id, term.to_owned());
            id
        }
    }
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

pub fn tokenize_string(string: &str) -> Vec<Token> {
    let mut current_position = 0;
    string.split_whitespace().map(|string| {
        current_position += 1;
        Token { term: string.trim_matches(|c: char| !c.is_alphanumeric()).to_lowercase(), weight: 1.0, position: current_position }
    }).filter(|token| token.term.len() < 100).collect()
}

pub fn tokens_to_tsvector(tokens: &Vec<Token>, dict: &mut TermDictionary) -> TSVector {
    let mut terms: FnvHashMap<TermId, TSVectorTerm> = FnvHashMap::default();

    for token in tokens {
        let term = dict.get_or_insert(&token.term);
        let mut term_entry = terms.entry(term).or_default();

        term_entry.positions.push(token.position);
        term_entry.weight += token.weight;
    }

    let field_length = tokens.len() as f32;

    for (_, term) in &mut terms.iter_mut() {
        term.weight = (term.weight + 1.0).log2();

        // Divide weights by field length
        term.weight /= field_length;
    }

    return TSVector { terms, length: tokens.len() };
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use super::{tokenize_string, tokens_to_tsvector, Database, DocumentSource};

    #[test]
    fn it_works() {
        let mut db = Database::default();
        let tokens = tokenize_string("hello world this is a test hello");
        let tsvector = tokens_to_tsvector(&tokens, &mut db.dictionary);
        dbg!(tokens);
        dbg!(&tsvector);

        let mut fields = HashMap::new();
        fields.insert("title".to_owned(), tsvector);
        db.insert_document(DocumentSource { fields });
    }
}
