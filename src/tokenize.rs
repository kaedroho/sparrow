#[derive(Debug)]
pub struct Token {
    pub term: String,
    pub position: usize,
    pub weight: f32,
}

pub fn tokenize_string(string: &str) -> Vec<Token> {
    let mut current_position = 0;
    string.split_whitespace().map(|string| {
        current_position += 1;
        Token { term: string.trim_matches(|c: char| !c.is_alphanumeric()).to_lowercase(), weight: 1.0, position: current_position }
    }).filter(|token| token.term.len() < 100).collect()
}
