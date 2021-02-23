use ibverbs::EndpointMsg;
use std::net::TcpStream;

const WR_ID: u64 = 9_926_239_128_092_127_829;

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
    let laddr = ibverbs::RemoteAddr((&mr[0..]).as_ptr() as u64);
    let lkey = mr.rkey();

    let qp_init = {
        let qp_builder = pd.create_qp(&cq, &cq, ibverbs::ibv_qp_type::IBV_QPT_RC); // client access flags default to ALLOW_LOCAL_WRITES which is ok
        qp_builder.build().unwrap()
    };

    // This info will be sended to the remote server,
    // but we also expect to get the same insformation set from the server later
    let rmsg = {
        let mut msg = ibverbs::EndpointMsg::from(qp_init.endpoint());
        msg.rkey = lkey;
        msg.raddr = laddr;

        let mut stream = TcpStream::connect("10.253.0.1:9003").unwrap();

        // Sending info for RDMA handshake over TcpStream;
        bincode::serialize_into(&mut stream, &msg).unwrap();

        // Recieving and desirializing info from the server
        let rmsg: EndpointMsg = bincode::deserialize_from(&mut stream).unwrap();
        rmsg
    };
    let rkey = rmsg.rkey;
    let raddr = rmsg.raddr;
    let rendpoint = rmsg.into();

    let qp = qp_init.handshake(rendpoint).unwrap();

    mr[0] = 456;

    let mut completions = [ibverbs::ibv_wc::default()];

    // Write
    unsafe {
        qp.post_write_single(&mr, raddr.0, rkey.0, WR_ID, true)
            .unwrap();
    }
    loop {
        let completed = cq
            .poll(&mut completions)
            .expect("ERROR: Could not poll CQ.");
        if completed.is_empty() {
            continue;
        }
        if completed.iter().any(|wc| wc.wr_id() == WR_ID) {
            break;
        }
    }

    // Read
    unsafe {
        qp.post_read_single(&mr, raddr.0, rkey.0, WR_ID, true)
            .unwrap();
    }
    loop {
        let completed = cq
            .poll(&mut completions)
            .expect("ERROR: Could not poll CQ.");
        if completed.is_empty() {
            continue;
        }
        if completed.iter().any(|wc| wc.wr_id() == WR_ID) {
            break;
        }
    }
}
