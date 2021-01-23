use elasticsearch::{
    http::{
        request::JsonBody,
        transport::Transport,
    },
    BulkParts, Elasticsearch,
};
use itertools::Itertools;
use ring::digest;
use serde_json::Value;
use std::env;
use std::fs;
use std::collections::HashMap;

#[macro_use]
extern crate serde_json;

fn update_keys_from_header(line: &str, keys: &mut Vec<String>) {
    let split_header = line.split(' ').collect::<Vec<&str>>();
    if split_header[0] == "#Fields:" {
        // Since the files being parsed could be concatenations over several years,
        // refresh the array of keys every time a new header is encountered.
        keys.clear();
        for field_name in split_header[1..].iter() {
            keys.push(
                field_name.replace("(", "-").replace(")", "")
            );
        }
    }
}

fn add_metadata(data: &mut HashMap<String,String>) {
    let get_params = data[&String::from("cs-uri-query")].clone();
    for param in get_params.split('&') {
        let param_split = param.split('=').collect::<Vec<&str>>();
        if param_split.len() == 2 {
            let (key, value) = (param_split[0], param_split[1]);
            if key == "pk_campaign" {
                data.insert(String::from("campaign"), value.to_string());
            }
        }
    }
}

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        panic!("Expected two arguments: <elasticsearch url> <file>");
    }

    let transport = Transport::single_node(&args[1]).expect("Unable to create ES transport");
    let client = Elasticsearch::new(transport);

    const CHUNK_SIZE: usize = 100;

    let file = fs::read_to_string(&args[2]).expect("Failed to open file");
    let chunks = 
        file
        .split('\n')
        .chunks(CHUNK_SIZE);

    let mut keys: Vec<String> = Vec::new();

    for chunk in &chunks {
        let mut body: Vec<JsonBody<_>> = Vec::with_capacity(2*CHUNK_SIZE);
        for line in chunk {
            if line.chars().count() == 0 { continue; }
            if line.chars().nth(0) == Some('#') {
                update_keys_from_header(line, &mut keys);
                continue;
            }

            let hash = digest::digest(&digest::SHA256, line.as_bytes());
            body.push(json!({"index": {
                "_id": format!("{:x?}", hash)[7..18],
                "pipeline": "cfn-logs",
            }}).into());

            let mut data = 
                keys.iter().map(|r| r.clone())
                    .zip(line.split('\t').map(|s| s.to_string()))
                    .collect::<HashMap<String, _>>();
            add_metadata(&mut data);
            body.push(json!(data).into());
        }
        let response = 
            client
            .bulk(BulkParts::Index("test1"))
            .body(body)
            .send()
            .await?;
        let response_body = response.json::<Value>().await?;
        let successful = response_body["errors"].as_bool().unwrap() == false;
        assert!(successful, format!("Error from Elasticsearch: {:?}", response_body));
    }

    Ok(())
}
