use std::collections::hash_map::HashMap;
use fnv::FnvHashMap;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, serde_derive::Serialize, serde_derive::Deserialize)]
#[serde(transparent)]
pub struct FieldId(u32);

#[derive(Debug, Clone, serde_derive::Serialize)]
pub struct FieldConfig {
    pub boost: f32,
}

impl FieldConfig {
    pub fn set_boost(&self, boost: f32) -> FieldConfig {
        let mut new = self.clone();
        new.boost = boost;
        new
    }

    pub fn add_boost(&self, boost: f32) -> FieldConfig {
        let mut new = self.clone();
        new.boost *= boost;
        new
    }
}

impl Default for FieldConfig {
    fn default() -> FieldConfig {
        FieldConfig {
            boost: 1.0,
        }
    }
}

#[derive(Debug, Default, serde_derive::Serialize)]
pub struct DataDictionary {
    next_field_id: u32,
    pub field_names: HashMap<String, FieldId>,
    pub fields: FnvHashMap<FieldId, FieldConfig>,
}

impl DataDictionary {
    pub fn insert(&mut self, name: String, config: FieldConfig) -> FieldId {
        let id = FieldId(self.next_field_id);
        self.next_field_id += 1;
        self.field_names.insert(name, id);
        self.fields.insert(id, config);
        id
    }

    pub fn get(&self, field_id: FieldId) -> Option<&FieldConfig> {
        self.fields.get(&field_id)
    }

    pub fn get_by_name(&self, name: &str) -> Option<(FieldId, &FieldConfig)> {
        self.field_names.get(name).map(|field_id| {
            let field_config = self.fields.get(field_id).expect("Field name with invalid field id");

            (*field_id, field_config)
        })
    }
}
