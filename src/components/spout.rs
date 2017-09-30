use futures::{Future, future};
use tokio_uds::UnixStream;
use tokio_core::reactor::Core;
use std::thread;
use std::sync::{Arc, Mutex};
use components::{Message, ComponentConfig};

pub trait BaseSpout{
    fn prepare(&mut self);
    fn next_tuple(&mut self);
}

#[derive(Clone)]
pub struct Spout{
    name: String,
    out_q: Arc<Mutex<Vec<Message>>>
}

impl Spout{
    pub fn new(name: &str) -> Self{
        let out_q = Arc::new(Mutex::new(vec![]));
        Spout{
            name: name.to_string(),
            out_q: out_q
        }
    }

    pub fn emit(&mut self, tuple: Message){
        let mut q = self.out_q.lock().unwrap();
        q.push(tuple);
    }

    pub fn start<T>(&mut self, mut spout: T, args: ComponentConfig) where T: BaseSpout{
        spout.prepare();
        let q_clone = self.out_q.clone();
        let sock_file = args.sock_file.clone();
        thread::spawn(move || {
            let mut core = Core::new().unwrap();
            let handle = core.handle();
            let socket = UnixStream::connect(sock_file, &handle).unwrap();
            let reg = Message::Local(args.component_id);
            let socket = core.run(reg.to_uds(socket)).unwrap();
            let lo = future::loop_fn::<_, UnixStream, _, _>(socket, |cc|{
                let next;
                {
                    let mut q = q_clone.lock().unwrap();
                    if q.len() == 0{
                        return future::ok(()).and_then(|_| Ok(future::Loop::Continue(cc))).boxed();
                    }
                    next = (*q).remove(0);
                }
                Box::new(next.to_uds(cc)
                            .and_then(|c| Ok(future::Loop::Continue(c))))
            });
            let _ = core.run(lo);
        });
        loop{
            spout.next_tuple();
        }
    }
}