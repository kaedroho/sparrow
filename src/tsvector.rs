use std::ops::Add;
use fnv::FnvHashMap;

use super::TermId;

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

#[derive(Debug, Default, Clone)]
pub struct TSVector {
    pub length: usize,
    pub terms: FnvHashMap<TermId, TSVectorTerm>,
}

impl TSVector {
    pub fn boost(&mut self, boost: f32) {
        for term in self.terms.values_mut() {
            term.weight *= boost;
        }
    }

    pub fn append(&mut self, other: &TSVector) {
        for (term, other_term_info) in &other.terms {
            if let Some(mut term_info) = self.terms.get_mut(term) {
                for position in &other_term_info.positions {
                    term_info.positions.push(self.length + position);
                }
                term_info.weight += other_term_info.weight;
            } else {
                self.terms.insert(*term, other_term_info.clone());
            }
        }

        self.length += other.length;
    }
}

impl Add<&TSVector> for &TSVector {
    type Output = TSVector;

    fn add(self, other: &TSVector) -> TSVector {
        let mut new = self.clone();
        new.append(other);
        new
    }
}
