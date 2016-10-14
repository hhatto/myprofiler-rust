#[macro_use]
extern crate mysql;
#[macro_use]
extern crate lazy_static;
extern crate getopts;
extern crate users;
extern crate time;
extern crate regex;

use std::collections::HashMap;
use std::{env, process, thread};
use std::time::Duration;
use mysql::{Pool, Value};
use mysql::value::from_value;
use getopts::Options;
use users::{get_current_uid, get_user_by_uid};
use time::{now, strftime};
use regex::Regex;

const QUERY_SHOW_PROCESS: &'static str = "SHOW FULL PROCESSLIST";

lazy_static! {
	static ref NORMALIZE_PATTERNS: Vec<NormalizePattern<'static>> = vec![
	    NormalizePattern::new(Regex::new(r" +").unwrap(), " "),
	    NormalizePattern::new(Regex::new(r#"[+-]{0,1}\b\d+\b"#).unwrap(), "N"),
        NormalizePattern::new(Regex::new(r"\b0x[0-9A-Fa-f]+\b").unwrap(), "0xN"),
	    NormalizePattern::new(Regex::new(r#"(\\')"#).unwrap(), ""),
	    NormalizePattern::new(Regex::new(r#"(\\")"#).unwrap(), ""),
	    NormalizePattern::new(Regex::new(r"'[^']+'").unwrap(), "S"),
	    NormalizePattern::new(Regex::new(r#""[^"]+""#).unwrap(), "S"),
	    NormalizePattern::new(Regex::new(r"(([NS]\s*,\s*){4,})").unwrap(), "...")
    ];
}

struct Summarizer {
    counts: HashMap<String, i64>,
}
impl Summarizer {
    fn new() -> Summarizer {
        Summarizer { counts: HashMap::new() }
    }
    fn update(&mut self, queries: Vec<String>) {
        for query in queries {
            let count = self.counts.entry(query).or_insert(0);
            *count += 1;
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
struct FullProcessList {
    id: u64,
    user: String,
    host: String,
    db: String,
    command: String,
    time: i32,
    state: String,
    info: String,
}

#[derive(Debug)]
struct ProcessList {
    info: String,
}

struct NormalizePattern<'a> {
    re: Regex,
    subs: &'a str,
}

impl<'a> NormalizePattern<'a> {
    fn new(re: Regex, subs: &'a str) -> NormalizePattern<'a> {
        NormalizePattern {
            re: re,
            subs: subs,
        }
    }
    fn normalize(&self, text: &'a str) -> String {
        self.re.replace_all(text, self.subs)
    }
}

macro_rules! value2string {
    ($row:expr, $value:expr) => (
        match $row.take($value) {
            Some(v) => {
                if v == Value::NULL { "".to_string() } else { from_value(v) }
            },
            None => "".to_string()
        }
    )
}

macro_rules! opts2v {
    ($m:expr, $opts:expr, $opt:expr, $t:ty, $default:expr) => (
    match $m.opt_str($opt) {
        Some(v) => {
            match v.parse::<$t>() {
                Ok(v) => v,
                Err(e) => {
                    println!("e={:?}", e);
                    print_usage($opts);
                    process::exit(1);
                },
            }
        },
        None => $default,
    }
    )
}

pub fn normalize_query(text: &str) -> String {
    let mut t = text.to_string();
    for pat in NORMALIZE_PATTERNS.iter() {
        t = pat.normalize(t.as_str());
    }
    t.to_string()
}

fn get_process_list(pool: &Pool) -> Vec<ProcessList> {
    let procs: Vec<ProcessList> = pool.prep_exec(QUERY_SHOW_PROCESS, ())
        .map(|ret| {
            ret.map(|x| x.unwrap())
                .map(|mut row| ProcessList { info: value2string!(row, "Info") })
                .filter(|x| !x.info.is_empty() && x.info != QUERY_SHOW_PROCESS.to_string())
                .collect()
        })
        .unwrap();
    procs
}

fn print_usage(opts: Options) {
    print!("{}", opts.usage("Usage: myprofiler [options]"));
}

fn main() {
    let mut opts = Options::new();
    opts.optopt("h", "host", "mysql hostname", "HOSTNAME");
    opts.optopt("u", "user", "mysql user", "USER");
    opts.optopt("p", "password", "mysql password", "PASSWORD");
    opts.optopt("", "port", "mysql port", "PORT");
    opts.optopt("", "top", "print top N query", "N");
    opts.optopt("i", "interval", "(float) Sampling interval", "N.M");
    opts.optopt("",
                "delay",
                "(int) Show summary for each `delay` samples. -interval=0.1 -delay=30 shows summary for every 3sec",
                "N");
    let args: Vec<String> = env::args().collect();
    let matches = match opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(e) => {
            print_usage(opts);
            println!("{:?}", e);
            return;
        }
    };

    let host = match matches.opt_str("host") {
        Some(v) => v,
        None => "localhost".to_string(),
    };
    let user = match matches.opt_str("user") {
        Some(v) => v,
        None => get_user_by_uid(get_current_uid()).unwrap().name().to_string(),
    };
    let password = match matches.opt_str("password") {
        Some(v) => v,
        None => "".to_string(),
    };
    let port = opts2v!(matches, opts, "port", i32, 3306);
    let interval = opts2v!(matches, opts, "interval", f32, 1.0);
    let delay = opts2v!(matches, opts, "delay", i32, 1);

    let pool = Pool::new_manual(1,
                                1,
                                format!("mysql://{user}:{password}@{host}:{port}",
                                        user = user,
                                        password = password,
                                        host = host,
                                        port = port)
                                    .as_str())
        .unwrap();

    let mut summ = Summarizer::new();

    let mut cnt = 0;
    loop {
        let mut procs = get_process_list(&pool);
        for process in procs.iter_mut() {
            let info = normalize_query(process.info.as_str());
            (*process).info = info;
        }

        summ.update(procs.iter().map(|x| x.info.clone()).collect());

        cnt += 1;
        if cnt >= delay {
            cnt = 0;
            let t = now().to_local();
            println!("##  {}.{:03} {}",
                     strftime("%Y-%m-%d %H:%M:%S", &t).unwrap(),
                     t.tm_nsec / 1000_000,
                     strftime("%z", &t).unwrap());
            summ.update(procs.iter().map(|x| x.info.clone()).collect());
            for (k, v) in summ.counts.iter() {
                println!("{:-4} {}", v, k);
            }
        }

        thread::sleep(Duration::from_millis((1000. * interval) as u64));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize() {
        let data = vec![("IN ('a', 'b', 'c')", "IN (S, S, S)"),
                        ("IN ('a', 'b', 'c', 'd', 'e')", "IN (...S)"),
                        ("IN (1, 2, 3)", "IN (N, N, N)"),
                        ("IN (0x1, 2, 3)", "IN (0xN, N, N)"),
                        ("IN (1, 2, 3, 4, 5)", "IN (...N)"),
                        ];
        for (pat, ret) in data {
            println!("vv | {:?}, {:?}", normalize_query(pat), ret);
            assert!(normalize_query(pat) == ret);
        }
    }
}
