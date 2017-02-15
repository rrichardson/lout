use std::fs::{self, File};
use std::time::{Instant, Duration};
use serde_json::value::Value as JValue;
use serde_json;
use csv::Writer as CSVWriter;
use csv::Result as CSVResult;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

#[derive(Serialize, Deserialize)]
struct Field {
    path : String,
    typename : Option<String>
}

#[derive(Serialize, Deserialize)]
struct Mapping {
    if_has_key : Option<String>,
    fields : BTreeMap<String, Field>
}

#[derive(Serialize, Deserialize)]
struct Schema {
    record_type_key : String,
    mappings : BTreeMap<String, Mapping>
}

struct Writer { 
    csvwriter : CSVWriter<File>,
    count : u64,
    path : PathBuf,
    next_write : Instant
}

pub struct Translator<F>
    where F : FnMut(&Path, &str, u64) -> bool
{
    outfiles : BTreeMap<String, Writer>,
    schema : Schema,
    write_interval : Duration,
    retry_interval : Duration,
    write_cb  : F
}

fn fetch_schema(path : &Path) -> Result<Schema, String> {
    let schemafile = File::open(path).map_err(|e| e.to_string())?;
    serde_json::from_reader(schemafile).map_err(|e| e.to_string())
}

impl<F> Translator<F>
    where F : FnMut(&Path, &str, u64) -> bool
{
    pub fn new(
           write_dir: &Path,
           write_interval : Duration,
           retry_interval : Duration,
           schema_file : &Path,
           write_cb : F) -> Translator<F> {

        let schema = fetch_schema(schema_file).unwrap();
        let res : BTreeMap<String, Writer> = 
            schema.mappings.iter().fold( BTreeMap::new(), |mut m, (k,mapping)| {
                let path = write_dir.join(k);
                let columns : Vec<&String> = mapping.fields.keys().collect();
                let w = Self::new_writer(&path, true, &columns).unwrap();
                m.insert(k.clone(), Writer {
                    path : PathBuf::from(path),
                    csvwriter : w,
                    count : 0,
                    next_write : Instant::now() + write_interval
                });
                m
            });
        
        return Translator {
            outfiles : res,
            schema : schema,
            write_interval : write_interval,
            retry_interval : retry_interval,
            write_cb : write_cb
        }
    }

    fn new_writer(path : &Path, _truncate : bool, columns : &Vec<&String>) -> Result<CSVWriter<File>, String> {

        let f = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path).map_err(|e| e.to_string())?;

        let mut w = CSVWriter::from_writer(f); //.flexible(true);

        // add the header
        w.encode(columns).unwrap();

        Ok(w)
    }

    pub fn process(&mut self, jval : &JValue) -> Option<u64> 
    {
        let ref appname = jval.get(&self.schema.record_type_key);
        if appname.is_none() { return None; }
        let app = appname.unwrap().as_str().unwrap();
        let mapping = self.schema.mappings.get(app);
        if mapping.is_none() { return None; }
        let mapping = mapping.unwrap();
        match mapping.if_has_key {
            Some(ref ifhk) => {
                if jval.get(ifhk).is_none() {
                    return None
                }
            },
            None => {}
        }
        let mut result = Vec::<String>::with_capacity(mapping.fields.len());

        for (_, field) in mapping.fields.iter() {
            match jval.pointer(&field.path) {
                Some(val) => {
                    if val.is_number() {
                        match val.as_f64() {
                            Some(v) => result.push(v.to_string()),
                            None => result.push("0".to_owned())
                        }
                    } else {
                        match val.as_str() {
                            Some(v) => result.push(v.to_owned()),
                            None => result.push(String::new())
                        }
                    }
                },
                None => {result.push(String::new())}
            }
        }

        if result.is_empty() {
            return None
        }
       
        let ref mut writer = self.outfiles.get_mut(app).unwrap();
        match writer.csvwriter.write(result.iter()) {
            Ok(_) => { writer.count += 1;},
            Err(e) => { error!("{}", e.to_string()); }
        }

        if (writer.next_write <= Instant::now()) && writer.count > 0 {
            writer.csvwriter.flush().unwrap();
            let ref mut cb = self.write_cb;
            if cb(&writer.path, app, writer.count) {
                writer.next_write = Instant::now() + self.write_interval;
                writer.count = 0;
                let columns : Vec<&String> = mapping.fields.keys().collect();
                writer.csvwriter = Self::new_writer(&writer.path, true, &columns).unwrap();
            } else {
                writer.next_write = Instant::now() + self.retry_interval;
            }
        }
        Some(writer.count)
    }
}
