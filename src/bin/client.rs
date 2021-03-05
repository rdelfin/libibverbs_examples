use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use ibverbs::EndpointMsg;
use std::{error, io::Cursor, net::TcpStream, time::SystemTime};
use structopt::StructOpt;

const WR_ID: u64 = 9_926_239_128_092_127_829;

#[derive(StructOpt, Debug, Clone)]
#[structopt(name = "rdma_client", version = "0.1")]
struct Opt {
    #[structopt(short = "p", long)]
    server_port: u16,
    #[structopt(short = "s", long)]
    server_address: String,
}

fn main() -> Result<(), Box<dyn error::Error>> {
    let opt = Opt::from_args();
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
    let mut mr = pd.allocate::<u8>(7864320 + 8 + 1024 * 1024).unwrap();
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

        let mut stream = TcpStream::connect(format!("{}:{}", opt.server_address, opt.server_port))?;

        // Sending info for RDMA handshake over TcpStream;
        bincode::serialize_into(&mut stream, &msg).unwrap();

        // Recieving and desirializing info from the server
        let rmsg: EndpointMsg = bincode::deserialize_from(&mut stream).unwrap();
        rmsg
    };
    // let rkey = rmsg.rkey;
    // let raddr = rmsg.raddr;

    let last_ts: u64 = 0;
    write_to(&mut mr[0..8], &u64_to_network(last_ts)?[..], 8);

    let mut qp = qp_init.handshake(rmsg.into()).unwrap();

    let mut completions = [ibverbs::ibv_wc::default()];

    loop {
        unsafe {
            qp.post_receive(&mut mr, 0..7864320, WR_ID)?;
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
        let image_ts = network_to_u64(&mr[..])?;
        let curr_ts = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)?
            .as_nanos() as u64;

        println!(
            "Delay: {:.4}",
            (curr_ts as i64 - image_ts as i64) as f64 * 1e-6
        );
    }
}

fn network_to_u64(data: &[u8]) -> Result<u64, Box<dyn error::Error>> {
    let mut rdr = Cursor::new(data);
    Ok(rdr.read_u64::<BigEndian>()?)
}

fn u64_to_network(val: u64) -> Result<Vec<u8>, Box<dyn error::Error>> {
    let mut data = vec![];
    data.write_u64::<BigEndian>(val)?;
    Ok(data)
}

fn write_to(dst: &mut [u8], src: &[u8], nelems: usize) {
    for i in 0..nelems {
        dst[i] = src[i];
    }
}
