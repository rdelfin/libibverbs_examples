use ibverbs::EndpointMsg;
use std::{env, error, fs, net::TcpListener, path::PathBuf};

const WR_ID: u64 = 9_926_239_128_092_127_829;

fn main() -> Result<(), Box<dyn error::Error>> {
    let images = load_images(PathBuf::from("data"), "RGB8")?;
    let bytes_per_image = images[0].len();

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

    let mut mr = pd.allocate::<u8>(bytes_per_image).unwrap();
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

    let mut qp = qp_init
        .handshake(rendpoint)
        .unwrap_or_else(|e| panic!("ERROR: failed to handshake: {}", e));
    let mut completions = [ibverbs::ibv_wc::default()];

    println!("Copying image...");
    for i in 0..bytes_per_image {
        mr[i] = images[0][i];
    }
    println!("Done!");

    // println!("RDMA handshake successful");
    unsafe {
        qp.post_send(&mut mr, 0..bytes_per_image, WR_ID)?;
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

    println!("Someone has written to the memory region, got: {}", mr[0]);

    Ok(())
}

fn load_images(dir: PathBuf, ext: &str) -> Result<Vec<Vec<u8>>, Box<dyn error::Error>> {
    Ok(fs::read_dir(dir.as_path())?
        .filter(|e| match e {
            Ok(entry) => {
                let is_file = {
                    match entry.file_type() {
                        Ok(ftype) => ftype.is_file(),
                        Err(_) => false,
                    }
                };
                let correct_ext = { entry.path().extension().unwrap() == ext };
                is_file && correct_ext
            }
            Err(_) => false,
        })
        .map(|e| fs::read(e.unwrap().path().as_path()))
        .collect::<Result<Vec<_>, _>>()?
        .iter()
        .map(|data| data[32..].into())
        .collect())
}
