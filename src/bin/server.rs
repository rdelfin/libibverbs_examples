use ibverbs::EndpointMsg;
use std::{env, net::TcpListener};

fn main() {
    let devices = ibverbs::devices().unwrap();
    let device = devices.iter().next().expect("no rdma device available");
    println!(
        "Using device named {:?} with GUID {}",
        device.name(),
        device.guid().unwrap()
    );

    let ctx = device.open().unwrap();

    let dev_attr = ctx.clone().query_device().unwrap();
    let pd = ctx.clone().alloc_pd().unwrap();
    let cq = ctx.create_cq(dev_attr.max_cqe, 0).unwrap();

    let mut mr = pd.allocate::<u64>(10 * 4096).unwrap();
    let laddr = (&mr[0..]).as_ptr() as u64;

    let qp_init = pd
        .create_qp(&cq, &cq, ibverbs::ibv_qp_type::IBV_QPT_RC)
        .allow_remote_rw()
        .build()
        .unwrap();

    let endpoint = qp_init.endpoint();

    let mut msg = EndpointMsg::from(endpoint);
    msg.rkey = mr.rkey();
    msg.raddr = ibverbs::RemoteAddr(laddr);

    let addr = env::var("RDMA_ADDR".to_string()).unwrap_or_else(|_| "0.0.0.0:9003".to_string());

    let listner = TcpListener::bind(addr).expect("Listener failed");
    let (mut stream, _addr) = listner.accept().expect("Accepting failed");

    println!("Connected!");

    let rmsg: EndpointMsg = bincode::deserialize_from(&mut stream)
        .unwrap_or_else(|e| panic!("ERROR: failed to recieve data: {}", e));

    let _rkey = rmsg.rkey;
    let _raddr = rmsg.raddr;
    let rendpoint = rmsg.into();

    bincode::serialize_into(&mut stream, &msg).unwrap();

    let _qp = qp_init
        .handshake(rendpoint)
        .unwrap_or_else(|e| panic!("ERROR: failed to handshake: {}", e));

    println!("RDMA handshake successfull");
    let mut last_val = 0;
    mr[0] = last_val;

    loop {
        if mr[0] != last_val {
            println!("Someone has written to the memory region, got: {}", mr[0]);
            last_val = mr[0];
        }
    }
}
