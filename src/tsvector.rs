use std::ops::Add;
use fnv::FnvHashMap;

use super::TermId;
use super::tokenize::Token;
use super::term_dictionary::TermDictionary;

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

impl TSVector {
    pub fn from_tokens(tokens: &Vec<Token>, dict: &mut TermDictionary) -> TSVector {
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

        TSVector { terms, length: tokens.len() }
    }
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
