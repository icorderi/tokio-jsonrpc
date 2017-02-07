// Copyright (c) 2017 Michal 'vorner' Vaner <vorner@vorner.cz>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

// TODO: Some comments explaining what is happening

extern crate tokio_jsonrpc;

#[macro_use]
extern crate serde_json;
extern crate futures;
extern crate tokio_core;

use tokio_jsonrpc::{Message, LineCodec};
use tokio_jsonrpc::message::{Notification, Broken};

use futures::{Future, Sink, Stream};
use tokio_core::reactor::Core;
use tokio_core::net::TcpListener;
use tokio_core::io::Io;

fn main() {
    let mut core = Core::new().unwrap();
    let handle = core.handle();

    let listener = TcpListener::bind(&"127.0.0.1:2345".parse().unwrap(), &handle).unwrap();
    let connections = listener.incoming();
    let service = connections.for_each(|(stream, _)| {
        let jsonized = stream.framed(LineCodec);
        let (w, r) = jsonized.split();
        let answers = r.filter_map(|message| {
            println!("A message received: {:?}", message);
            // TODO: We probably want some more convenient handling, like more handy methods on
            // Message.
            match message {
                Ok(Message::Request(ref req)) => {
                    println!("Got method {}", req.method);
                    if req.method == "echo" {
                        Some(req.reply(json!([req.method, req.params])))
                    } else {
                        Some(req.error(-32601, format!("Unknown method {}", req.method), None))
                    }
                },
                Ok(Message::Notification(Notification { ref method, .. })) => {
                    println!("Got notification {}", method);
                    None
                },
                Err(Broken::Unmatched(_)) => Some(Message::error(-32600, "Not a JSONRPC message".to_owned(), None)),
                Err(Broken::SyntaxError(ref e)) => Some(Message::error(-32700, e.to_owned(), None)),
                _ => None,
            }
        });
        let sent = w.send_all(answers)
            .map(|_| ())
            .map_err(|_| {
                // TODO Something with the error ‒ logging?
                ()
            });
        // Do the sending in the background
        handle.spawn(sent);
        Ok(())
    });
    core.run(service).unwrap();
}
