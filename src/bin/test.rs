use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;
use std::collections::HashMap;

use sparrow::{Database, Token, DocumentSource, Query};
use sparrow::data_dictionary::FieldConfig;

#[derive(Debug, serde_derive::Deserialize)]
struct Document {
    title: String,
    summary: String,
}

fn main() {
    let mut db = Database::default();
    let all_text_field = db.data_dictionary.insert("all_text".to_owned(), FieldConfig::default());
    let title_field = db.data_dictionary.insert("title".to_owned(), FieldConfig::default().boost(2.0).copy_to(all_text_field));
    db.data_dictionary.insert("summary".to_owned(), FieldConfig::default().copy_to(all_text_field));

    let mut sources = HashMap::new();

    if let Ok(lines) = read_lines("./test.json") {
        for line in lines {
            if let Ok(line) = line {
                if let Ok(doc) = serde_json::from_str::<Document>(&line) {
                    let title = tokenize_string(&doc.title);
                    let summary = tokenize_string(&doc.summary);

                    let mut fields = HashMap::new();
                    fields.insert("title".to_owned(), title);
                    fields.insert("summary".to_owned(), summary);

                    let doc_source = DocumentSource { fields };
                    let id = db.insert_document(doc_source.clone());
                    sources.insert(id, doc.title);
                }
            }
        }
    }

    let query = Query::And(vec![
        Query::Term(title_field, db.term_dictionary.get_or_insert("karl")),
        Query::Term(title_field, db.term_dictionary.get_or_insert("hobley")),
    ]);

    let mut documents = db.query(&query);

    documents.sort_by(|a,b| a.1.partial_cmp(&b.1).unwrap().reverse());

    for (document_id, score) in documents {
        dbg!(serde_json::to_string(sources.get(&document_id).unwrap()).unwrap(), score);
    }
}

// The output is wrapped in a Result to allow matching on errors
// Returns an Iterator to the Reader of the lines of the file.
fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where P: AsRef<Path>, {
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

pub fn tokenize_string(string: &str) -> Vec<Token> {
    let mut current_position = 0;
    string.split_whitespace().map(|string| {
        current_position += 1;
        Token { term: string.trim_matches(|c: char| !c.is_alphanumeric()).to_lowercase(), position: current_position }
    }).filter(|token| token.term.len() < 100).collect()
}
