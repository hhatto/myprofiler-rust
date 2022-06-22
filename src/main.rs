use getopts::Options;
use mysql::prelude::*;
use mysql::from_value;
use mysql::{Opts, Pool, Value};
use once_cell::sync::Lazy;
use regex::Regex;
use std::borrow::Cow;
use std::collections::HashMap;
use std::time::Duration;
use std::{env, process, thread};
use time::{now, strftime};
use users::{get_current_uid, get_user_by_uid};

const QUERY_SHOW_PROCESS: &'static str = "SHOW FULL PROCESSLIST";

static NORMALIZE_PATTERNS: Lazy<Vec<NormalizePattern<'static>>> = Lazy::new(|| {
    vec![
        NormalizePattern::new(Regex::new(r" +").expect("fail regex compile: +"), " "),
        NormalizePattern::new(Regex::new(r#"[+-]{0,1}\b\d+\b"#).expect("fail regex compile: digit"), "N"),
        NormalizePattern::new(Regex::new(r"\b0x[0-9A-Fa-f]+\b").expect("fail regex compile: hex"), "0xN"),
        NormalizePattern::new(Regex::new(r#"(\\')"#).expect("fail regex compile: single quote"), ""),
        NormalizePattern::new(Regex::new(r#"(\\")"#).expect("fail regex compile: double quote"), ""),
        NormalizePattern::new(Regex::new(r"'[^']+'").expect("fail regex compile: string1"), "S"),
        NormalizePattern::new(Regex::new(r#""[^"]+""#).expect("fail regex compile: string2"), "S"),
        NormalizePattern::new(Regex::new(r"(([NS]\s*,\s*){4,})").expect("fail regex compile: long"), "..."),
    ]
});

trait Summarize {
    fn new(limit: u32) -> Self;
    fn show(&mut self, n_query: u32);
    fn update(&mut self, queries: Vec<String>);
}

fn show_summary(summ: &HashMap<String, i64>, n_query: u32) {
    let mut pp: Vec<_> = summ.iter().collect();
    pp.sort_by(|a, b| b.1.cmp(a.1));

    let mut cnt = 0;
    for (k, v) in pp {
        println!("{:-4} {}", v, k);
        cnt += 1;
        if cnt >= n_query {
            break;
        }
    }
}

struct Summarizer {
    counts: HashMap<String, i64>,
}
impl Summarize for Summarizer {
    fn new(_: u32) -> Summarizer {
        Summarizer { counts: HashMap::new() }
    }

    fn show(&mut self, n_query: u32) {
        show_summary(&self.counts, n_query);
    }

    fn update(&mut self, queries: Vec<String>) {
        for query in queries {
            let count = self.counts.entry(query).or_insert(0);
            *count += 1;
        }
    }
}

#[derive(Debug)]
struct QueryCount {
    q: String,
    n: i64,
}
struct RecentSummarizer {
    counts: Vec<Vec<QueryCount>>,
    limit: u32,
}
impl Summarize for RecentSummarizer {
    fn new(limit: u32) -> RecentSummarizer {
        RecentSummarizer {
            counts: vec![],
            limit: limit,
        }
    }

    fn show(&mut self, n_query: u32) {
        let mut summ = HashMap::new();
        for qcs in &self.counts {
            for qc in qcs {
                let query = qc.q.clone();
                let count = summ.entry(query).or_insert(0);
                *count += qc.n;
            }
        }
        show_summary(&summ, n_query);
    }

    fn update(&mut self, queries: Vec<String>) {
        let mut qs = queries;
        let mut qc = Vec::<QueryCount>::new();
        if self.counts.len() >= self.limit as usize {
            self.counts.remove(0);
        }
        qs.sort_by(|a, b| a.cmp(b));

        let mut last_query = "";
        for query in qs.iter() {
            if last_query != query.as_str() {
                qc.push(QueryCount { q: query.clone(), n: 0 });
                last_query = query.as_str();
            }
            let l = qc.last_mut().expect("fail get last query string");
            l.n += 1;
        }
        self.counts.push(qc);
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
        NormalizePattern { re: re, subs: subs }
    }
    fn normalize(&self, text: &'a str) -> Cow<'a, str> {
        self.re.replace_all(text, self.subs)
    }
}

struct MyprofilerOption {
    interval: f32,
    delay: i32,
    top: u32,
}

macro_rules! value2string {
    ($row:expr, $value:expr) => {
        match $row.take($value) {
            Some(v) => {
                if v == Value::NULL {
                    "".to_string()
                } else {
                    from_value(v)
                }
            }
            None => "".to_string(),
        }
    };
}

macro_rules! opts2v {
    ($m:expr, $opts:expr, $opt:expr, $t:ty, $default:expr) => {
        match $m.opt_str($opt) {
            Some(v) => match v.parse::<$t>() {
                Ok(v) => v,
                Err(e) => {
                    println!("e={:?}", e);
                    print_usage($opts);
                    process::exit(1);
                }
            },
            None => $default,
        }
    };
}

pub fn normalize_query(text: &str) -> String {
    let mut t = text.to_string();
    for pat in NORMALIZE_PATTERNS.iter() {
        t = pat.normalize(t.as_str()).into();
    }
    t.to_string()
}

fn get_process_list(pool: &Pool) -> Vec<ProcessList> {
    let mut conn = pool.get_conn().unwrap();
    let procs: Vec<ProcessList> = conn
        .exec_iter(QUERY_SHOW_PROCESS, ())
        .map(|ret| {
            ret.map(|x| x.unwrap())
                .map(|mut row| ProcessList {
                    info: value2string!(row, "Info"),
                })
                .filter(|x| !x.info.is_empty() && x.info != QUERY_SHOW_PROCESS.to_string())
                .collect()
        })
        .expect("fail get process list");
    procs
}

fn print_usage(opts: Options) {
    print!("{}", opts.usage("Usage: myprofiler [options]"));
}

fn exec_profile<T: Summarize>(pool: &Pool, mut summ: T, options: &MyprofilerOption) {
    let mut cnt = 0;
    loop {
        let mut procs = get_process_list(&pool);
        for process in procs.iter_mut() {
            let info = normalize_query(process.info.as_str());
            (*process).info = info;
        }

        summ.update(procs.iter().map(|x| x.info.clone()).collect());

        cnt += 1;
        if cnt >= options.delay {
            cnt = 0;
            let t = now().to_local();
            println!(
                "##  {}.{:03} {}",
                strftime("%Y-%m-%d %H:%M:%S", &t).expect("fail strftime(ymdhms)"),
                t.tm_nsec / 1000_000,
                strftime("%z", &t).expect("fail strftime(z)")
            );
            summ.show(options.top);
        }

        thread::sleep(Duration::from_millis((1000. * options.interval) as u64));
    }
}

fn main() -> Result<(), String> {
    let mut opts = Options::new();
    opts.optopt("h", "host", "mysql hostname", "HOSTNAME");
    opts.optopt("u", "user", "mysql user", "USER");
    opts.optopt("p", "password", "mysql password", "PASSWORD");
    opts.optopt("", "port", "mysql port", "PORT");
    opts.optopt("", "top", "print top N query (default: 10)", "N");
    opts.optopt("", "last", "last N samples are summarized. 0 means summarize all samples", "N");
    opts.optopt("i", "interval", "(float) Sampling interval", "N.M");
    opts.optopt(
        "",
        "delay",
        "(int) Show summary for each `delay` samples. -interval=0.1 -delay=30 shows summary for every 3sec",
        "N",
    );
    let args: Vec<String> = env::args().collect();
    let matches = match opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(e) => {
            print_usage(opts);
            println!("{}", e);
            process::exit(1);
        }
    };

    let host = match matches.opt_str("host") {
        Some(v) => v,
        None => "localhost".to_string(),
    };
    let user = match matches.opt_str("user") {
        Some(v) => v,
        None => get_user_by_uid(get_current_uid())
            .expect("fail get uid")
            .name()
            .to_string(),
    };
    let password = match matches.opt_str("password") {
        Some(v) => v,
        None => "".to_string(),
    };
    let port = opts2v!(matches, opts, "port", i32, 3306);
    let last = opts2v!(matches, opts, "last", u32, 0);
    let options = MyprofilerOption {
        interval: opts2v!(matches, opts, "interval", f32, 1.0),
        delay: opts2v!(matches, opts, "delay", i32, 1),
        top: opts2v!(matches, opts, "top", u32, 10),
    };

    let url = format!(
        "mysql://{user}:{password}@{host}:{port}",
        user = user,
        password = password,
        host = host,
        port = port
    );
    let opts = Opts::from_url(url.as_str()).expect("invalid dsn");
    let pool = Pool::new_manual(1, 1, opts).expect("fail get mysql connection");

    if last == 0 {
        let summ: Summarizer = Summarize::new(last);
        exec_profile(&pool, summ, &options);
    } else {
        let summ: RecentSummarizer = Summarize::new(last);
        exec_profile(&pool, summ, &options);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize() {
        let data = vec![
            ("IN ('a', 'b', 'c')", "IN (S, S, S)"),
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
