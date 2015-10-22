use std::any::Any;
use std::error::Error;
use std::fmt;
use std::fmt::Debug;
use std::result;
use std::sync::mpsc;
use std::sync::mpsc::{Sender, Receiver, TryRecvError, RecvError, SendError};
use std::thread;

use time::{Duration, SteadyTime};

pub type InitResult<E> = result::Result<Option<u64>, E>;
pub type ActorResult<T, X> = result::Result<T, ActorError<X>>;
pub type StartResult<T, E> = result::Result<Actor<T>, E>;

pub struct Actor<T> where T: Any + Send {
    pub sender: Sender<Message<T>>,
    pub receiver: Receiver<Message<T>>,
    pub handle: thread::JoinHandle<ActorResult<(), T>>,
}

impl<T> Actor<T> where T: Any + Send {
    /// Create a new actor handler struct.
    pub fn new(sender: Sender<Message<T>>, receiver: Receiver<Message<T>>, handle: thread::JoinHandle<ActorResult<(), T>>) -> Self {
        Actor {
            sender: sender,
            receiver: receiver,
            handle: handle,
        }
    }

    pub fn cast(&self, message: T) -> Result<(), ActorError<T>> {
        match self.sender.send(Message::Cast(message)) {
            Ok(()) => Ok(()),
            Err(err) => Err(ActorError::from(err)),
        }
    }

    pub fn call(&self, message: T) -> Result<T, ActorError<T>> {
        match self.sender.send(Message::Call(message)) {
            Ok(()) => {
                match self.receiver.recv() {
                    Ok(Message::Reply(msg)) => Ok(msg),
                    Ok(_) => panic!("must reply from a call!"),
                    Err(err) => Err(ActorError::from(err)),
                }
            },
            Err(err) => Err(ActorError::from(err)),
        }
    }
}

pub struct Builder<T: GenServer> {
    name: Option<String>,
    spec: T,
}

impl<A: GenServer> Builder<A> {
    pub fn new(spec: A) -> Self {
        Builder {
            name: None,
            spec: spec,
        }
    }

    pub fn name(mut self, name: String) -> Builder<A> {
        self.name = Some(name);
        self
    }

    /// Start an actor on a new thread and return an Actor.
    pub fn start(self, mut state: A::S) -> StartResult<A::T, A::E> {
        let (otx, orx) = mpsc::channel::<Message<A::T>>();
        let (itx, irx) = mpsc::channel::<Message<A::T>>();
        let initial_wait_ms = match self.spec.init(&mut state) {
            Ok(result) => result,
            Err(err) => return Err(err),
        };

        let thread_name = self.name.clone().unwrap_or(String::from("GenServer"));
        let handle = thread::Builder::new().name(thread_name).spawn(move || {
            let mut timeout: Option<SteadyTime> = None;
            if let Some(ms) = initial_wait_ms {
                set_timeout(ms, &mut timeout);
            }
            loop {
                if let Some(go_time) = timeout {
                    if go_time >= SteadyTime::now() {
                        match self.spec.handle_timeout(&mut state) {
                            HandleResult::Stop(reason, None) => try!(shutdown(reason, None, &otx)),
                            HandleResult::NoReply(Some(0)) => {
                                set_timeout(0, &mut timeout);
                                continue;
                            },
                            HandleResult::NoReply(new_timeout) => {
                                if let Some(ms) = new_timeout {
                                    set_timeout(ms, &mut timeout);
                                }
                            },
                            hr => panic!("unexpected `HandleResult` returned from handle_timeout: {:?}", hr),
                        }
                    }
                }
                match irx.try_recv() {
                    Ok(Message::Call(msg)) => {
                        match self.spec.handle_call(msg, &otx, &mut state) {
                            HandleResult::Reply(msg, new_timeout) => {
                                try!(otx.send(Message::Reply(msg)));
                                if let Some(ms) = new_timeout {
                                    set_timeout(ms, &mut timeout);
                                }
                            },
                            HandleResult::NoReply(new_timeout) => {
                                if let Some(ms) = new_timeout {
                                    set_timeout(ms, &mut timeout);
                                }
                            },
                            HandleResult::Stop(reason, reply) => try!(shutdown(reason, reply, &otx)),
                        }
                    },
                    Ok(Message::Cast(msg)) => {
                        match self.spec.handle_cast(msg, &mut state) {
                            HandleResult::Stop(reason, reply) => try!(shutdown(reason, reply, &otx)),
                            HandleResult::NoReply(new_timeout) => {
                                if let Some(ms) = new_timeout {
                                    set_timeout(ms, &mut timeout);
                                }
                            },
                            hr => panic!("unexpected `HandleResult` returned from handle_cast: {:?}", hr),
                        }
                    },
                    Ok(Message::Info(msg)) => {
                        match self.spec.handle_info(msg, &mut state) {
                            HandleResult::Stop(reason, reply) => try!(shutdown(reason, reply, &otx)),
                            HandleResult::NoReply(new_timeout) => {
                                if let Some(ms) = new_timeout {
                                    set_timeout(ms, &mut timeout);
                                }
                            },
                            hr => panic!("unexpected `HandleResult` returned from handle_info: {:?}", hr),
                        }
                    },
                    Ok(hr) => panic!("received unexpected message type: {:?}", hr),
                    Err(TryRecvError::Disconnected) => { break; },
                    Err(TryRecvError::Empty) => { },
                }
            }
            Ok(())
        }).unwrap();
        Ok(Actor::new(itx, orx, handle))
    }
}

#[derive(Debug)]
pub enum ActorError<T> where T: Any + Send {
    InitFailure(String),
    AbnormalShutdown(String),
    SendError(mpsc::SendError<Message<T>>),
    RecvError,
}

impl<T: Any + Send> From<mpsc::SendError<Message<T>>> for ActorError<T> {
    fn from(err: mpsc::SendError<Message<T>>) -> Self {
        ActorError::SendError(err)
    }
}

impl<T: Any + Send> From<mpsc::RecvError> for ActorError<T> {
    fn from(_err: mpsc::RecvError) -> Self {
        ActorError::RecvError
    }
}

#[derive(Debug)]
pub enum StopReason {
    Normal,
    Other(String),
}

#[derive(Debug)]
pub enum HandleResult<T> where T: Any + Send {
    Reply(T, Option<u64>),
    NoReply(Option<u64>),
    Stop(StopReason, Option<T>),
}

pub enum Message<T> where T: Any + Send {
    Call(T),
    Cast(T),
    Info(T),
    Reply(T),
}

impl<T> Debug for Message<T> where T: Any + Send + Debug {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &Message::Call(ref msg) => write!(f, "CALL: {:?}", msg),
            &Message::Cast(ref msg) => write!(f, "CAST: {:?}", msg),
            &Message::Info(ref msg) => write!(f, "INFO: {:?}", msg),
            &Message::Reply(ref msg) => write!(f, "REPLY: {:?}", msg)
        }
    }
}

pub trait GenServer : Send + 'static {
    type T: Send + Any + Debug;
    type S: Send + Any;
    type E: Error + 'static;

    fn init(&self, state: &mut Self::S) -> InitResult<Self::E>;

    fn handle_call(&self, _message: Self::T, _sender: &Sender<Message<Self::T>>, _state: &mut Self::S) -> HandleResult<Self::T> {
        panic!("handle_call callback not implemented");
    }
    fn handle_cast(&self, _message: Self::T, _state: &mut Self::S) -> HandleResult<Self::T> {
        panic!("handle_cast callback not implemented");
    }
    fn handle_info(&self, _message: Self::T, _state: &mut Self::S) -> HandleResult<Self::T> {
        HandleResult::NoReply(None)
    }
    fn handle_timeout(&self, _state: &mut Self::S) -> HandleResult<Self::T> {
        HandleResult::NoReply(None)
    }
}

fn set_timeout(wait_ms: u64, current_timeout: &mut Option<SteadyTime>) {
    *current_timeout = Some(SteadyTime::now() + Duration::milliseconds(wait_ms as i64));
}

fn shutdown<T: Any + Send>(reason: StopReason, reply: Option<T>, sender: &Sender<Message<T>>) -> Result<(), ActorError<T>> {
    if let Some(msg) = reply {
        let _result = sender.send(Message::Reply(msg));
    }
    match reason {
        StopReason::Normal => Ok(()),
        StopReason::Other(e) => Err(ActorError::AbnormalShutdown(e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt;
    use std::error::Error;
    use std::sync::mpsc::Sender;

    struct Worker;

    struct MyState {
        pub initialized: bool,
    }

    impl MyState {
        pub fn new() -> Self {
            MyState {
                initialized: false,
            }
        }
    }

    #[derive(Debug)]
    enum MyError {
        One,
    }

    #[derive(Debug)]
    enum MyMessage {
        InitState(bool),
        IsInitialized,
    }

    impl GenServer for Worker {
        type T = MyMessage;
        type S = MyState;
        type E = MyError;

        fn init(&self, state: &mut MyState) -> InitResult<MyError> {
            state.initialized = true;
            Ok(None)
        }

        fn handle_call(&self, msg: MyMessage, _sender: &Sender<Message<MyMessage>>, state: &mut MyState) -> HandleResult<MyMessage> {
            HandleResult::Reply(MyMessage::InitState(state.initialized), None)
        }
    }

    impl fmt::Display for MyError {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            match *self {
                MyError::One => write!(f, "test error"),
            }
        }
    }

    impl Error for MyError {
        fn description(&self) -> &str {
            match *self {
                MyError::One => "test error"
            }
        }
    }

    #[test]
    fn call() {
        let state = MyState::new();
        let actor = Builder::new(Worker).name(String::from("Whatever")).start(state).unwrap();
        match actor.call(MyMessage::IsInitialized).unwrap() {
            MyMessage::InitState(result) => assert_eq!(result, true),
            _ => assert_eq!(false, true),
        }
    }
}
