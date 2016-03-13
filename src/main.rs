/// # finance-miner: used to grab historical financial data from the internet
///
/// This tool downloads and stores instrument data into the given directory

#[macro_use]
extern crate prettytable;
extern crate encoding;
extern crate argparse;
extern crate hyper;
#[macro_use]
extern crate log;
extern crate regex;

extern crate yaml_rust;
use std::str::FromStr;
use yaml_rust::yaml;
use yaml_rust::{YamlLoader, YamlEmitter, Yaml};

use prettytable::Table;
use prettytable::row::Row;
use prettytable::cell::Cell;

use encoding::{Encoding, DecoderTrap};
use argparse::{ArgumentParser, Store, List};
use std::string::String;
use std::vec::Vec;
use std::collections::{HashMap, BTreeMap};
use std::io::Read;
use std::ops::Deref;

use hyper::client::response::Response;

use std::io::Write;

use hyper::header::{Headers, AcceptCharset, Charset, qitem};
use log::{LogRecord, LogLevel, LogMetadata, SetLoggerError, LogLevelFilter};

struct SimpleLogger;

impl log::Log for SimpleLogger {
    fn enabled(&self, metadata: &LogMetadata) -> bool {
        metadata.level() <= LogLevel::Info
    }

    fn log(&self, record: &LogRecord) {
        if self.enabled(record.metadata()) {
            println!("{} - {}", record.level(), record.args());
        }
    }
}

trait FromYaml where Self: Sized {
    fn from_yaml(yaml: &Yaml) -> Result<Self>;
}

trait ToYaml<T> {
    fn from_yaml(target: &Self) -> Yaml;
}

struct Options {
    log_level: String,
    targets: Vec<String>,
}

const SLEEP_BETWEEN_REQUESTS: u64 = 3000;
const ICHARTS_URI: &'static str = "http://www.finam.ru/cache/icharts/icharts.js";
const MARKETS_BASE_URI: &'static str = "http://www.finam.ru/profile/";
const LOGGER: &'static str = "finance-logger";


#[derive(Debug)]
enum ParseError {
    BlockNotFound(String),
    YamlError(yaml_rust::scanner::ScanError),
}

#[derive(Debug)]
enum MinerError {
    Http(hyper::error::Error),
    Io(std::io::Error),
    HttpErrorStatus(Response),
    Utf8(std::str::Utf8Error),
    EncodingError(String),
    YamlError(yaml_rust::scanner::ScanError),
    YamlConversionError(String),
    ParseError(ParseError),
}


type Result<T> = std::result::Result<T, MinerError>;

impl From<hyper::error::Error> for MinerError {
    fn from(err: hyper::error::Error) -> MinerError {
        MinerError::Http(err)
    }
}

impl From<std::io::Error> for MinerError {
    fn from(err: std::io::Error) -> MinerError {
        MinerError::Io(err)
    }
}

impl From<std::str::Utf8Error> for MinerError {
    fn from(err: std::str::Utf8Error) -> MinerError {
        MinerError::Utf8(err)
    }
}

impl From<yaml_rust::scanner::ScanError> for MinerError {
    fn from(err: yaml_rust::scanner::ScanError) -> MinerError {
        MinerError::YamlError(err)
    }
}

impl From<ParseError> for MinerError {
    fn from(err: ParseError) -> MinerError {
        MinerError::ParseError(err)
    }
}

impl From<yaml_rust::scanner::ScanError> for ParseError {
    fn from(source: yaml_rust::scanner::ScanError) -> ParseError {
        ParseError::YamlError(source)
    }
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            ParseError::BlockNotFound(ref details) => write!(f, "Data not found in document: {}", details),
            ParseError::YamlError(ref err) => err.fmt(f),
        }
    }
}

impl std::fmt::Display for MinerError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            MinerError::Http(ref err) => err.fmt(f),
            MinerError::Io(ref err) => err.fmt(f),
            MinerError::HttpErrorStatus(ref err) => write!(f, "HTTP request unsuccessful"),
            MinerError::Utf8(ref err) => err.fmt(f),
            MinerError::EncodingError(ref msg) => write!(f, "Encoding error: {}", msg),
            MinerError::YamlError(ref err) => err.fmt(f),
            MinerError::ParseError(ref err) => err.fmt(f),
            MinerError::YamlConversionError(ref msg) => write!(f, "Yaml conversion error: {}", msg),
        }
    }
}

impl std::error::Error for ParseError {
    fn description(&self) -> &str {
        match *self {
            ParseError::BlockNotFound(ref err) => "Block we're searching not found in the document",
            ParseError::YamlError(ref err) => "Block cannot be parsed as Yaml document",
        }
    }
}

impl std::error::Error for MinerError {
    fn description(&self) -> &str {
        match *self {
            MinerError::Http(ref err) => err.description(),
            MinerError::Io(ref err) => err.description(),
            MinerError::HttpErrorStatus(ref err) => "HTTP request unsuccessful",
            MinerError::Utf8(ref err) => err.description(),
            MinerError::EncodingError(ref err) => err,
            MinerError::YamlError(ref err) => err.description(),
            MinerError::ParseError(ref err) => err.description(),
            MinerError::YamlConversionError(ref msg) => "Cannot convet between object and Yaml",
        }
    }
}


fn ensure_http_success(response: Response) -> Result<Response> {
    if response.status.is_success() {
        info!("{}: Got success response: {}", LOGGER, response.status);
        Ok(response)
    } else {
        Err(MinerError::HttpErrorStatus(response))
    }
}


fn download_finam_doc(uri: &str) -> Result<String> {
    info!("{}: Downloading financial data from: {}", LOGGER, uri);
    hyper::Client::new()
        .get(uri)
        .header(AcceptCharset(vec![qitem(Charset::Ext("utf-8".to_string()))]))
        .send()
        .map_err(|err| From::from(err))
        .and_then(ensure_http_success)
        .and_then(|mut res| {
            let mut result = Vec::<u8>::new();
            try!(res.read_to_end(&mut result));
            Ok(result)
        })
        .and_then(|res| {
            encoding::all::WINDOWS_1251.decode(res.as_slice(), DecoderTrap::Replace).
                map_err(|err| MinerError::EncodingError(err.deref().to_string()))
        })
}

fn extract_yaml_from_doc(regex: &str, body: &str) -> Vec<Yaml> {
    let re = regex::Regex::new(regex).unwrap();

    let mut yamls_parsed = Vec::<Yaml>::new();

    for captures in re.captures_iter(body) {
        for capture in captures.iter().skip(1).filter(|cap| cap.is_some()) {
            match YamlLoader::load_from_str(capture.unwrap()) {
                Ok(yamls_found) => yamls_parsed.extend(yamls_found),
                Err(err) => warn!("{}: Cannot extract yaml: {}", LOGGER, err)
            }
        }
    }

    yamls_parsed
}

#[derive(Default)]
#[derive(Debug)]
struct Emitent {
    internal_id: u64,  /// Internal id of the struct
    id: String,
    name: String,
    market_id: String,
    market_name: String,
    uri: String,
    code: String,
}


impl FromYaml for Emitent {
    fn from_yaml(yaml: &Yaml) -> Result<Emitent> {
        let mut result = Emitent::default();
        let hash = match yaml.as_hash() {
            Some(hash) => hash,
            None => return Err(MinerError::YamlConversionError("Root value is not hash".to_string())),
        };

        result.internal_id = try!(
            hash.get(&Yaml::from_str("internal_id")).ok_or(MinerError::YamlConversionError("internal_id not found".to_string()))
                .and_then(|yaml| yaml.as_i64().ok_or(MinerError::YamlConversionError("internal_id is not int".to_string())))
                .and_then(|internal_id| Ok(internal_id as u64)));

        result.id = try!(
            hash.get(&Yaml::from_str("id")).ok_or(MinerError::YamlConversionError("id not found".to_string()))
                .and_then(|yaml| yaml.as_str().ok_or(MinerError::YamlConversionError("id is not string".to_string())))
                .and_then(|id| Ok(id.to_string())));

        Ok(result)
    }
}

fn yaml_to_string(yaml: &Yaml) -> String {
    match yaml {
        &Yaml::Real(ref id) | &Yaml::String(ref id) => id.clone(),
        &Yaml::Integer(id) => id.to_string(),
        &Yaml::Boolean(id) => id.to_string(),
        &Yaml::Array(ref data) => { warn!("{}: Unexpected id: {:?}", LOGGER, data); String::new() },
        &Yaml::Hash(ref data) => { warn!("{}: Unexpected id: {:?}", LOGGER, data); String::new() },
        &Yaml::Alias(data) => { warn!("{}: Unexpected id: {}", LOGGER, data); String::new() },
        &Yaml::Null => { warn!("{}: Unexpected id: {:?}", LOGGER, Yaml::Null); String::new() },
        &Yaml::BadValue => { warn!("{}: Unexpected id: {:?}", LOGGER, Yaml::BadValue); String::new() },
    }
}

fn download_emitent_info(uri: &str) -> Result<Yaml> {
    let document = try!(download_finam_doc(uri));
    let re = regex::Regex::new(r"Main.issue = (.*);").unwrap();

    re.captures(&document).ok_or(ParseError::BlockNotFound("Emitent captures not found".to_string()))
        .and_then(|captures| captures.at(1).ok_or(ParseError::BlockNotFound("Emitent capture doesn't exist".to_string())))
        .and_then(|capture| YamlLoader::load_from_str(capture).map_err(|err| From::from(err)))
        .and_then(|yamls| {
            yamls.first().ok_or(ParseError::BlockNotFound("Yaml block cannot be decoded".to_string()))

        })
        .map(|first| first.clone())
        .map_err(|err| From::from(err))
}

fn download_emitents_data() -> Vec<Emitent> {
    let icharts = download_finam_doc(ICHARTS_URI)
        .and_then(|charts| {
            Ok(charts.replace("\r\n", "").replace("\n", "").replace(r"\'", "''"))
        })
        .unwrap_or_else(|err| { println!("{}", err); std::process::exit(2); });

    let emitent_id_sources: Vec<Yaml> = extract_yaml_from_doc(r"var aEmitentIds = (\[.*?\]);", &icharts);

    assert!(emitent_id_sources.len() == 1);

    let emitent_ids_yaml = match emitent_id_sources.first().unwrap() {
        &Yaml::Array(ref data) => data.clone(),
        _ => Vec::<Yaml>::new()
    };

    let mut emitents = HashMap::<usize, Emitent>::new();

    for (internal_id, emitent_id) in emitent_ids_yaml.iter().enumerate() {
        let id = yaml_to_string(emitent_id);

        emitents.insert(internal_id, Emitent { internal_id: internal_id as u64, id: id, ..Default::default() });
    }

    let emitent_name_sources: Vec<Yaml> = extract_yaml_from_doc(r"var aEmitentNames = (\[.*?\]);", &icharts);

    assert!(emitent_name_sources.len() == 1);

    let emitent_names_yaml = match emitent_name_sources.first().unwrap() {
        &Yaml::Array(ref data) => data.clone(),
        _ => Vec::<Yaml>::new()
    };

    for (internal_id, emitent_name) in emitent_names_yaml.iter().enumerate() {
        if let Some(emitent) = emitents.get_mut(&internal_id) {
            emitent.name = yaml_to_string(emitent_name);
        } else {
            warn!("{}: Emitent with internal id: {} not found", LOGGER, internal_id);
        }
    }

    let emitent_market_sources: Vec<Yaml> = extract_yaml_from_doc(r"var aEmitentMarkets = (\[.*?\]);", &icharts);

    assert!(emitent_market_sources.len() == 1);

    let emitent_markets_yaml = match emitent_market_sources.first().unwrap() {
        &Yaml::Array(ref data) => data.clone(),
        _ => Vec::<Yaml>::new()
    };


    for (internal_id, emitent_market) in emitent_markets_yaml.iter().enumerate() {
        if let Some(emitent) = emitents.get_mut(&internal_id) {
            emitent.market_id = yaml_to_string(emitent_market);
        } else {
            warn!("{}: Emitent with internal id: {} not found", LOGGER, internal_id);
        }
    }

    let mut emitents_by_id = HashMap::<String, Emitent>::new();

    for (_, emitent) in emitents.drain() {
        emitents_by_id.insert(emitent.id.clone(), emitent);
    }

    let emitent_uris_sources: Vec<Yaml> = extract_yaml_from_doc(r"var aEmitentUrls = (\{.*?\});", &icharts);

    assert!(emitent_uris_sources.len() == 1);

    let emitent_uris_yaml = match emitent_uris_sources.first().unwrap() {
        &Yaml::Hash(ref data) => data.clone(),
        _ => BTreeMap::<Yaml, Yaml>::new()
    };

    for (id, uri) in emitent_uris_yaml.iter() {
        if let Some(emitent) = emitents_by_id.get_mut(&yaml_to_string(id)) {
            emitent.uri = yaml_to_string(uri);
        } else {
            warn!("{}: Emitent with id: {} not found", LOGGER, &yaml_to_string(id));
        }
    }

    let mut result = Vec::<Emitent>::new();

    for (_, emitent) in emitents_by_id.drain() {
        result.push(emitent);
    }

    result
}

fn main() {
    log::set_logger(|max_log_level| {
        max_log_level.set(LogLevelFilter::Debug);
        Box::new(SimpleLogger)
    }).unwrap();

    // let mut options = Options {
    //     log_level: "INFO".to_string(),
    //     targets: vec!()
    // };
    // {
    //     let mut parser = ArgumentParser::new();
    //     parser.set_description("Stocks market financial data miner");
    //     parser.refer(&mut options.targets)
    //         .add_argument("target", List, "targets")
    //         .required();
    //     parser.parse_args_or_exit();
    // }

    let mut table = Table::new();

    for emitent in download_emitents_data().iter_mut().take(3) {

        let yaml = match download_emitent_info(&format!("{}/{}", MARKETS_BASE_URI, &emitent.uri)) {
            Ok(yaml) => yaml,
            Err(err) => {
                println!("Cannot get emitent data: {}", err);
                continue;
            }
        };

        emitent.code = yaml.as_hash()
            .and_then(|hash| hash.get(&Yaml::from_str("quote")))
            .and_then(|yaml| yaml.as_hash())
            .and_then(|hash| hash.get(&Yaml::from_str("code")))
            .and_then(|yaml| yaml.as_str())
            .unwrap_or_else(|| {
                info!("{}: Code cannot be decoded for: {}", LOGGER, emitent.name);
                ""
            })
            .to_string();

        emitent.market_name = yaml.as_hash()
            .and_then(|hash| hash.get(&Yaml::from_str("quote")))
            .and_then(|yaml| yaml.as_hash())
            .and_then(|hash| hash.get(&Yaml::from_str("market")))
            .and_then(|yaml| yaml.as_hash())
            .and_then(|hash| hash.get(&Yaml::from_str("title")))
            .and_then(|yaml| yaml.as_str())
            .unwrap_or_else(|| {
                info!("{}: Market name cannot be decoded for: {}", LOGGER, emitent.name);
                ""
            })
            .to_string();

        println!("{:?}", emitent);
        std::thread::sleep(std::time::Duration::new(SLEEP_BETWEEN_REQUESTS, 0));
    }

    // table.printstd();

    // return;
}
