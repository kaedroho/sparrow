use std::collections::hash_map::HashMap;
use fnv::FnvHashMap;

use super::TermId;

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
