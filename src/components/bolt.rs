use futures::{Future, future};
use tokio_uds::UnixStream;
use tokio_io::AsyncRead;
use tokio_core::reactor::{Core, Timeout};
use std::thread;
use std::time::Duration;
use std::sync::{Arc, Mutex};
use components::{Message, ComponentConfig};

pub trait BaseBolt{
    fn prepare(&mut self);
    fn process_tuple(&mut self, tuple: Message);
}

#[derive(Clone)]
pub struct Bolt{
    name: String,
    in_q: Arc<Mutex<Vec<Message>>>,
    out_q: Arc<Mutex<Vec<Message>>>
}

impl Bolt{
    pub fn new(name: &str) -> Self{
        let in_q = Arc::new(Mutex::new(vec![]));
        let out_q = Arc::new(Mutex::new(vec![]));
        Bolt{
            name: name.to_string(),
            in_q: in_q,
            out_q: out_q
        }
    }

    pub fn emit(&mut self, tuple: Message){
        let mut q = self.out_q.lock().unwrap();
        q.push(tuple);
    }

    pub fn start<T>(&mut self, mut bolt: T, args: ComponentConfig) where T: BaseBolt{
        bolt.prepare();
        let in_q_clone = self.in_q.clone();
        let out_q_clone = self.out_q.clone();
        let sock_file = args.sock_file.clone();
        thread::spawn(move || {
            let mut core = Core::new().unwrap();
            let handle = core.handle();
            let socket = UnixStream::connect(sock_file, &handle).unwrap();
            let reg = Message::Local(args.component_id);
            let socket = core.run(reg.to_uds(socket)).unwrap();
            let (rx, tx) = socket.split();
            let write = future::loop_fn::<_, UnixStream, _, _>(tx, |cc|{
                let next;
                {
                    let mut q = out_q_clone.lock().unwrap();
                    if q.len() == 0{
                        return Timeout::new(Duration::from_millis(100), &handle).unwrap()
                                .and_then(|_| Ok(future::Loop::Continue(cc))).boxed();
                    }
                    next = (*q).remove(0);
                }
                Box::new(next.to_half_uds(cc)
                            .and_then(|c| Ok(future::Loop::Continue(c))))
            });
            let read = future::loop_fn::<_, UnixStream, _, _>(rx, move |cc|{
                let in_q = in_q_clone.clone();
                Box::new(Message::from_half_uds(cc).and_then(move |m| {
                    {                    
                        let mut q = in_q.lock().unwrap();
                        q.push(m.0);
                    }
                    Ok(future::Loop::Continue(m.1))
                }))
            }).and_then(|_| Ok(()));
            let _ = core.run(write.join(read));
        });
        loop{
            let tuple;
            {
                let mut q = self.in_q.lock().unwrap();
                if q.len() == 0{
                    continue;
                }
                tuple = (*q).remove(0);
            }
            bolt.process_tuple(tuple);
        }
    }
}