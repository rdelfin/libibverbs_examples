use ibverbs::EndpointMsg;
use std::{error, fs, net::TcpStream, path::PathBuf};

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
    let rkey = rmsg.rkey;
    let raddr = rmsg.raddr;

    let qp = qp_init.handshake(rmsg.into()).unwrap();

    println!("Copying image...");
    for i in 0..bytes_per_image {
        mr[i] = images[0][i];
    }
    println!("Done!");

    let mut completions = [ibverbs::ibv_wc::default()];

    // Write
    unsafe {
        qp.post_write_buf(&mr, bytes_per_image, raddr.0, rkey.0, WR_ID, true)
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
