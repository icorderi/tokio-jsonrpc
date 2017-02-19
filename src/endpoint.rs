// Copyright 2017 tokio-jsonrpc Developers
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! The endpoint of the JSON RPC connection
//!
//! This module helps building the endpoints of the connection. The endpoints act as both client
//! and server at the same time. If you want a client-only endpoint, use
//! [`EmptyServer`](struct.EmptyServer.html) as the server. If you want a server-only endpoint,
//! simply don't call any RPCs or notifications.

use message::{Broken, Message, Parsed, Response, Request, Notification};

use std::io::{Error as IoError, ErrorKind};
use std::collections::HashMap;
use std::rc::Rc;
use std::time::Duration;
use std::cell::RefCell;

use serde::Serialize;
use serde_json::{Value, to_value};
use futures::{Future, IntoFuture, Stream, Sink};
use futures::stream::{self, Once, empty};
use futures_mpsc::{channel, Sender};
use relay::{channel as relay_channel, Sender as RelaySender};
use tokio_core::reactor::{Handle, Timeout};

/// A handle to control the server
///
/// An instance is provided to each [`Server`](trait.Server.html) callback and it can be used to
/// manipulate the server (currently only to terminate the server).
#[derive(Clone)]
pub struct ServerCtl;

impl ServerCtl {
    /// Stop answering RPCs and calling notifications
    ///
    /// Also terminate the connection if the client handle has been dropped.
    pub fn terminate(&mut self) {
        unimplemented!();
    }
}

/// The server endpoint
///
/// This is usually implemented by the end application and provides the actual functionality of the
/// RPC server. It allows composition of more servers together.
///
/// In future it might be possible to generate servers with the help of some macros. Currently it
/// is up to the developer to handle conversion of parameters, etc.
///
/// The default implementations of the callbacks return None, indicating that the given method is
/// not known. It allows implementing only rpcs or only notifications without having to worry about
/// the other callback. If you want a server that knows nothing at all, use
/// [`EmptyServer`](struct.EmptyServer.html).
pub trait Server {
    /// The successfull result of the RPC call.
    type Success: Serialize;
    /// The result of the RPC call
    ///
    /// Once the future resolves, the value or error is sent to the client as the reply. The reply
    /// is wrapped automatically.
    type RPCCallResult: IntoFuture<Item = Self::Success, Error = (i64, String, Option<Value>)>;
    /// The result of the RPC call
    ///
    /// As the client doesn't expect anything in return, both the success and error results are
    /// thrown away and therefore (). However, it still makes sense to distinguish success and
    /// error.
    // TODO: Why do we need 'static here and not above?
    type NotificationResult: IntoFuture<Item = (), Error = ()> + 'static;
    /// Called when the client requests something
    ///
    /// This is a callback from the [endpoint](struct.Endpoint.html) when the client requests
    /// something. If the method is unknown, it shall return `None`. This allows composition of
    /// servers.
    ///
    /// Conversion of parameters and handling of errors is up to the implementer of this trait.
    fn rpc(&self, _ctl: &mut ServerCtl, _method: &str, _params: &Option<Value>) -> Option<Self::RPCCallResult> {
        None
    }
    /// Called when the client sends a notification
    ///
    /// This is a callback from the [endpoint](struct.Endpoint.html) when the client requests
    /// something. If the method is unknown, it shall return `None`. This allows composition of
    /// servers.
    ///
    /// Conversion of parameters and handling of errors is up to the implementer of this trait.
    fn notification(&self, _ctl: &mut ServerCtl, _method: &str, _params: &Option<Value>) -> Option<Self::NotificationResult> {
        None
    }
    /// Called when the endpoint is initialized
    ///
    /// It provides a default empty implementation, which can be overriden to hook onto the
    /// initialization.
    fn initialized(&self, _ctl: &mut ServerCtl) {}
}

// Our own BoxFuture & friends that is *not* send. We don't do send.
type BoxFuture<T, E> = Box<Future<Item = T, Error = E>>;
type FutureMessage = BoxFuture<Option<Message>, IoError>;
type BoxStream<T, E> = Box<Stream<Item = T, Error = E>>;
type FutureMessageStream = BoxStream<FutureMessage, IoError>;

type IDMap = Rc<RefCell<HashMap<String, RelaySender<Response>>>>;

// A future::stream::once that takes only the success value, for convenience.
fn once<T, E>(item: T) -> Once<T, E> {
    stream::once(Ok(item))
}

fn shouldnt_happen<E>(_: E) -> IoError {
    IoError::new(ErrorKind::Other, "Shouldn't happen")
}

fn do_request<RPCServer: Server + 'static>(server: &RPCServer, ctl: &mut ServerCtl, request: Request) -> FutureMessage {
    match server.rpc(ctl, &request.method, &request.params) {
        None => Box::new(Ok(Some(Message::error(-32601, "Method not found".to_owned(), Some(Value::String(request.method.clone()))))).into_future()),
        Some(future) => {
            Box::new(future.into_future().then(move |result| match result {
                Err((code, msg, data)) => Ok(Some(request.error(code, msg, data))),
                Ok(result) => Ok(Some(request.reply(to_value(result).expect("Trying to return a value that can't be converted to JSON")))),
            }))
        },
    }
}

fn do_notification<RPCServer: Server>(server: &RPCServer, ctl: &mut ServerCtl, notification: Notification) -> FutureMessage {
    match server.notification(ctl, &notification.method, &notification.params) {
        None => Box::new(Ok(None).into_future()),
        Some(future) => Box::new(future.into_future().then(|_| Ok(None))),
    }
}

// To process a batch using the same set of parallel executors as the whole server, we produce a
// stream of the computations which return nothing, but gather the results. Then we add yet another
// future at the end of that stream that takes the gathered results and wraps them into the real
// message ‒ the result of the whole batch.
fn do_batch<RPCServer: Server + 'static>(server: &RPCServer, ctl: &mut ServerCtl, idmap: &IDMap, msg: Vec<Message>) -> FutureMessageStream {
    // Create a large enough channel. We may be unable to pick up the results until the final
    // future gets its turn, so shorter one could lead to a deadlock.
    let (sender, receiver) = channel(msg.len());
    // Each message produces a single stream of futures. Create that streams (right now, so we
    // don't have to keep server long into the future).
    let small_streams: Vec<_> = msg.into_iter()
        .map(|sub| -> Result<_, IoError> {
            let sender = sender.clone();
            // This part is a bit convoluted. The do_msg returns a stream of futures. We want to
            // take each of these futures (the outer and_then), run it to completion (the inner
            // and_then), send its result through the sender if it provided one and then
            // convert the result to None.
            //
            // Note that do_msg may return arbitrary number of work futures, but only at most one
            // of them is supposed to provide a resulting value which would be sent through the
            // channel. Unfortunately, there's no way to know which one it'll be, so we have to
            // clone the sender all over the place.
            //
            // Also, it is a bit unfortunate how we need to allocate so many times here. We may try
            // doing something about that in the future, but without implementing custom future and
            // stream types, this seems the best we can do.
            let all_sent = do_msg(server, ctl, idmap, Ok(sub)).and_then(move |future_message| -> Result<FutureMessage, _> {
                let sender = sender.clone();
                let msg_sent = future_message.and_then(move |response: Option<Message>| -> FutureMessage {
                    match response {
                        None => Box::new(Ok(None).into_future()),
                        Some(msg) => {
                            Box::new(sender.send(msg)
                                .map_err(shouldnt_happen)
                                .map(|_| None))
                        },
                    }
                });
                Ok(Box::new(msg_sent))
            });
            Ok(all_sent)
        })
        .collect();
    // We make it into a stream of streams and flatten it to get one big stream
    let subs_stream = stream::iter(small_streams).flatten();
    // Once all the results are produced, wrap them into a batch and return that one
    let collected = receiver.collect()
        .map_err(shouldnt_happen)
        .map(|results| if results.is_empty() {
            // The spec says to send nothing at all if there are no results
            None
        } else {
            Some(Message::Batch(results))
        });
    let streamed: Once<FutureMessage, _> = once(Box::new(collected));
    // Connect the wrapping single-item stream after the work stream
    Box::new(subs_stream.chain(streamed))
}

// Handle single message and turn it into an arbitrary number of futures that may be worked on in
// parallel, but only at most one of which returns a response message
fn do_msg<RPCServer: Server + 'static>(server: &RPCServer, ctl: &mut ServerCtl, idmap: &IDMap, msg: Parsed) -> FutureMessageStream {
    match msg {
        Err(broken) => {
            let err: FutureMessage = Ok(Some(broken.reply())).into_future().boxed();
            Box::new(once(err))
        },
        Ok(Message::Request(req)) => Box::new(once(do_request(server, ctl, req))),
        Ok(Message::Notification(notif)) => Box::new(once(do_notification(server, ctl, notif))),
        Ok(Message::Batch(batch)) => do_batch(server, ctl, idmap, batch),
        Ok(Message::UnmatchedSub(value)) => do_msg(server, ctl, idmap, Err(Broken::Unmatched(value))),
        Ok(Message::Response(response)) => {
            let maybe_sender = response.id
                .as_str()
                .and_then(|id| idmap.borrow_mut().remove(id));
            if let Some(sender) = maybe_sender {
                sender.complete(response);
            }
            // TODO: Else ‒ some logging
            Box::new(empty())
        },
    }
}

/// A RPC server that knows no methods
///
/// You can use this if you want to have a client-only [Endpoint](struct.Endpoint.html). It simply
/// terminates the server part right away.
pub struct EmptyServer;

impl Server for EmptyServer {
    type Success = ();
    type RPCCallResult = Result<(), (i64, String, Option<Value>)>;
    type NotificationResult = Result<(), ()>;
    fn initialized(&self, ctl: &mut ServerCtl) {
        ctl.terminate();
    }
}

#[derive(Clone)]
pub struct Client {
    idmap: IDMap,
    sender: Sender<Message>,
    handle: Handle,
}

pub type Notified = BoxFuture<Client, IoError>;
pub type RPCFinished = BoxFuture<Option<Response>, IoError>;
pub type RPCSent = BoxFuture<(Client, RPCFinished), IoError>;

impl Client {
    // TODO: This interface sounds a bit awkward.
    pub fn call(self, method: String, params: Option<Value>, timeout: Option<Duration>) -> RPCSent {
        // We have to deconstruct self now, because the sender's send takes ownership for it for a
        // while. We construct it back once the message is passed on.
        let idmap = self.idmap;
        let handle = self.handle;
        let msg = Message::request(method, params);
        let id = match msg {
            Message::Request(Request { id: Value::String(ref id), .. }) => id.clone(),
            _ => unreachable!("We produce only string IDs"),
        };
        let (sender, receiver) = relay_channel();
        let received = receiver.map_err(shouldnt_happen).map(Some);
        let completed: RPCFinished = match timeout {
            Some(time) => {
                // If we were provided with a timeout, select what happens first.
                let timeout = match Timeout::new(time, &handle) {
                    Err(e) => return Box::new(Err(e).into_future()),
                    Ok(t) => t,
                };
                let idmap = idmap.clone();
                let id = id.clone();
                let completed = timeout
                    .map(|_| None)
                    .select(received)
                    .map(|(r, _)| r)
                    .map_err(|(e, _)| e)
                    // Make sure the ID/sender is removed even when timeout wins.
                    // This is a NOOP in case the real result arrives, since it is already deleted
                    // by then, but that doesn't matter and this is simpler.
                    .then(move |r| {
                        idmap.borrow_mut().remove(&id);
                        r
                    });
                Box::new(completed)
            },
            // If we don't have the timeout, simply pass the future to get the response through.
            None => Box::new(received),
        };
        idmap.borrow_mut().insert(id, sender);
        let sent = self.sender
            .send(msg)
            .map_err(shouldnt_happen)
            .map(move |sender| {
                let client = Client {
                    idmap: idmap,
                    sender: sender,
                    handle: handle,
                };
                (client, completed)
            });
        Box::new(sent)
    }
    pub fn notify(self, method: String, params: Option<Value>) -> Notified {
        let idmap = self.idmap;
        let handle = self.handle;
        let future = self.sender
            .send(Message::notification(method, params))
            .map_err(shouldnt_happen)
            .map(move |sender| {
                Client {
                    idmap: idmap,
                    sender: sender,
                    handle: handle,
                }
            });
        Box::new(future)
    }
}

// TODO: Some other interface to this? A builder?
pub fn endpoint<Connection, RPCServer>(handle: Handle, connection: Connection, server: RPCServer) -> Client
    where Connection: Stream<Item = Parsed, Error = IoError> + Sink<SinkItem = Message, SinkError = IoError> + Send + 'static,
          RPCServer: Server + 'static
{
    let (sender, receiver) = channel(32);
    let idmap = Rc::new(RefCell::new(HashMap::new()));
    let client = Client {
        idmap: idmap.clone(),
        sender: sender,
        handle: handle.clone(),
    };
    let (sink, stream) = connection.split();
    // Create a future for each received item that'll return something. Run some of them in
    // parallel.

    // TODO: Have a concrete enum-type for the futures so we don't have to allocate and box it.
    // TODO: The receiving parts of RPC answers goes somewhere here
    // TODO: Do something about the termination
    let answers = stream.map(move |parsed| do_msg(&server, &mut ServerCtl, &idmap, parsed))
        .flatten()
        .buffer_unordered(4)
        .filter_map(|message| message);
    // Take both the client RPCs and the answers
    let outbound = answers.select(receiver.map_err(shouldnt_happen));
    // And send them all
    let transmitted = sink.send_all(outbound);
    // Once the last thing is sent, we're done
    // TODO: Something with the errors
    handle.spawn(transmitted.map(|_| ()).map_err(|_| ()));
    client
}
