use std::env;
use std::fs;
use std::collections::HashMap;
use itertools::Itertools;
use elasticsearch::{
//    cat::CatIndicesParts,
    http::{
        request::JsonBody,
        transport::Transport,
    },
//    indices::{
//        IndicesCreateParts, IndicesDeleteParts, IndicesExistsParts, IndicesPutSettingsParts,
//    },
    BulkParts, Elasticsearch,
};
use ring::digest;

#[macro_use]
extern crate serde_json;
use serde_json::Value;

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        panic!("Expected two arguments: <elasticsearch url> <file>");
    }

    let transport = Transport::single_node(&args[1]).expect("Unable to create ES transport");
    let client = Elasticsearch::new(transport);

    //let response = client.cat().indices(CatIndicesParts::None).send().await?;
    //println!("{:?}", response.text().await?);

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
            // Skip headers
            if line.chars().nth(0) == Some('#') {
                let parts = line.split(' ').collect::<Vec<&str>>();
                if parts[0] == "#Fields:" {
                    // Since the files being parsed could be concatenations over several years,
                    // refresh the array of keys every time a new header is encountered.
                    keys.clear();
                    for field_name in parts[1..].iter() {
                        keys.push(
                            field_name.replace("(", "-").replace(")", "")
                        );
                    }
                }
            } 

            let hash = digest::digest(&digest::SHA256, line.as_bytes());
            body.push(json!({"index": {
                "_id": format!("{:x?}", hash)[7..18],
            }}).into());
            let data = keys.iter().zip(line.split('\t')).collect::<HashMap<_, _>>();
            body.push(json!(data).into());
            /*body.push(json!({
                "line": line,
            }).into());*/
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
