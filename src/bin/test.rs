use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;
use std::collections::HashMap;

use sparrow::{Database, tokenize::tokenize_string, tsvector::TSVector, DocumentSource};

#[derive(Debug, serde_derive::Deserialize)]
struct Document {
    title: String,
    summary: String,
}

fn main() {
    let mut db = Database::default();
    let mut sources = HashMap::new();

    if let Ok(lines) = read_lines("./test.json") {
        for line in lines {
            if let Ok(line) = line {
                if let Ok(doc) = serde_json::from_str::<Document>(&line) {
                    let title = TSVector::from_tokens(&tokenize_string(&doc.title), &mut db.dictionary);
                    let summary = TSVector::from_tokens(&tokenize_string(&doc.summary), &mut db.dictionary);

                    let mut fields = HashMap::new();
                    fields.insert("all_text".to_owned(), &title + &summary);
                    fields.insert("title".to_owned(), title);
                    fields.insert("summary".to_owned(), summary);

                    let doc_source = DocumentSource { fields };
                    let id = db.insert_document(doc_source);
                    sources.insert(id, doc.title);
                }
            }
        }
    }

    let mut documents = db.fields.get("all_text").unwrap().search(db.dictionary.get_or_insert("test"));

    documents.sort_by(|a,b| a.1.partial_cmp(&b.1).unwrap().reverse());

    for (document_id, score) in documents {
        dbg!(sources.get(&document_id), score);
    }
}

// The output is wrapped in a Result to allow matching on errors
// Returns an Iterator to the Reader of the lines of the file.
fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where P: AsRef<Path>, {
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}
