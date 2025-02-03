pub mod threadpool;

use std::net::{TcpListener, TcpStream};
use std::io::{Write, BufRead, BufReader};
use std::time::Duration;
use regex::Regex;
use std::thread::sleep;
use threadpool::ThreadPool;

fn invalid_request(stream: &mut TcpStream) {
    println!("Bad request.");
    stream.write_all(b"HTTP/1.1 400 Bad Request\r\n\
                    Content-Type: text/text\r\n\
                    Content-Length: 71\r\n\r\n\
                    Invalid request.").ok();
}

fn handle_connection(mut stream: TcpStream) {
    let mut reader = BufReader::new(&stream);

    // Parse header
    let mut buf = String::new();

    if reader.read_line(&mut buf).is_err() {
        invalid_request(&mut stream);
        return;
    }

    sleep(Duration::from_secs(1));

    println!("New connection! Starting to parse header.");

    let mut path: String = String::new();
    let mut content: String = String::new();
        
    if let Some(buf_s) = buf.strip_suffix("\r\n") {
        buf = String::from(buf_s);
    }

    if let Some(req_capture) = Regex::new(r"GET (?<path>[A-z0-9_\/]+)(?:\?(?:content=(?<content>.*))|.*)? HTTP\/(?<version>\d\.\d)").unwrap()
        .captures(buf.as_str())
    {
        req_capture["path"].clone_into(&mut path);
        req_capture.name("content").map_or("", |m| m.as_str()).clone_into(&mut content);
        println!("Path: {:?}", &path);
        println!("Version: {:?}", &req_capture["version"]);
        println!("Content: {:?}", &content)
    }
    else {
        invalid_request(&mut stream);
        return;
    }
    
    let mut failed = false;
    println!("------- HEADER -------");
    buf = String::new();
    while reader.read_line(&mut buf).is_ok() {
        if let Some(buf_s) = buf.strip_suffix("\r\n") {
            buf = String::from(buf_s);
        }
        else {
            invalid_request(&mut stream);
            failed = true;
            break;
        }

        if buf.is_empty() {
            break;
        }
        if let Some(header_capture) = Regex::new(r"([A-z\-]+):(.*)").unwrap()
            .captures(buf.as_str())
        {
            println!("Name: {:?}, Content: {:?}", &header_capture[1], &header_capture[2].strip_prefix(" ").unwrap_or(&header_capture[2]));
        }

        buf = String::new();
    }
    println!("----------------------");

    if failed {
        return;
    }

    // Receive content
    let retn_str = String::from("Halo! You are accessing ") + path.as_str()
                    + "!\r\nYour content:\r\n" + content.as_str();

    let retn_str = String::from("HTTP/1.1 200 OK\r\n\
                    Server: Awsl\r\n\
                    Cache-Control: no-store\r\n\
                    Content-Type: text/text\r\n\
                    Content-Length:") + retn_str.len().to_string().as_str() + "\r\n\r\n" + retn_str.as_str();

    println!("Request parsed.\r\nReturning: {:?}", retn_str);

    stream.write_all(retn_str.as_bytes()).ok();
}

fn main() {
    let listener: TcpListener = TcpListener::bind("127.0.0.1:8000").expect("Cannot bind to address.");
    let pool: ThreadPool = ThreadPool::new(8);
    println!("Bind address: http://127.0.0.1:8000");

    for stream in listener.incoming() {
        let stream = stream.expect("Failed to listen on stream.");
        pool.execute(move || handle_connection(stream));
    }
}
