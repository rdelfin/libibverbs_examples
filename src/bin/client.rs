use ibverbs::EndpointMsg;
use std::{error, net::TcpStream};

const WR_ID: u64 = 9_926_239_128_092_127_829;

fn main() -> Result<(), Box<dyn error::Error>> {
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
    let mut mr = pd.allocate::<u8>(7864320 + 1024 * 1024).unwrap();
    let laddr = ibverbs::RemoteAddr((&mr[0..]).as_ptr() as u64);
    let lkey = mr.rkey();

    let qp_init = {
        pd.create_qp(&cq, &cq, ibverbs::ibv_qp_type::IBV_QPT_RC)
            .allow_remote_rw()
            .build()
            .unwrap()
    };

    // This info will be sent to the remote server,
    // but we also expect to get the same information set from the server later
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
    // let rkey = rmsg.rkey;
    // let raddr = rmsg.raddr;

    let mut qp = qp_init.handshake(rmsg.into()).unwrap();

    // Write
    unsafe {
        qp.post_receive(&mut mr, 0..7864320, WR_ID)?;
    }
    println!("Data at mr[0] = {}", mr[0]);

    Ok(())
}
