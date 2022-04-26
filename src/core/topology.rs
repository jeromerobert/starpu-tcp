use super::Rank;
use std::env;
const LOCALHOST: &str = "127.0.0.1";
/// TODO: add full port range configuration (like btl_tcp_port_min_v4 btl_tcp_port_range_v4
/// oob_tcp_static_ipv4_ports oob_tcp_dynamic_ipv4_ports oob_tcp_if_include)
trait Topology {
    /// Check if this finder is able to work. If it does not work the
    /// next one will be tested
    fn supported(&mut self) -> bool;
    fn bindAddr(&self) -> String;
    fn rank(&self) -> Rank;
    fn world_size(&self) -> Rank;
}

/// This trait provides ways to find the rank 0 process network address.
/// This is typically found in environment variables set by LSF or SLURM.
trait Rank0Getter {
    /// Check if this finder is able to work. If it does not work the
    /// next one will be tested
    fn supported(&mut self) -> bool;
    fn rank0(&self) -> String;
}

struct LSFRank0Getter {
    rank0: String,
}

impl LSFRank0Getter {
    fn new() -> Self {
        Self {
            rank0: String::from(""),
        }
    }
}

struct OpenMPITopology {
    world_size: Rank,
    rank: Rank,
}

impl OpenMPITopology {
    fn new() -> Self {
        Self {
            rank: 0,
            world_size: 0,
        }
    }
}

impl Topology for OpenMPITopology {
    fn supported(&mut self) -> bool {
        match (
            env::var("OMPI_COMM_WORLD_RANK"),
            env::var("OMPI_COMM_WORLD_SIZE"),
        ) {
            (Ok(r), Ok(ws)) => {
                self.rank = r.parse().unwrap();
                self.world_size = ws.parse().unwrap();
                true
            }
            _ => false,
        }
    }
    fn bindAddr(&self) -> String {
        match env::var("OMPI_MCA_btl_tcp_if_include") {
            Ok(e) => e,
            // With OpenMPI the default is 0.0.0.0. Here we increase security
            _ => String::from(LOCALHOST),
        }
    }
    fn rank(&self) -> Rank {
        self.rank
    }
    fn world_size(&self) -> Rank {
        self.world_size
    }
}

impl Rank0Getter for LSFRank0Getter {
    fn supported(&mut self) -> bool {
        // LSB_MCPU_HOSTS looks like "machine1 32 machine2 32 ..."
        match env::var("LSB_MCPU_HOSTS") {
            Ok(r) => {
                let tokens: Vec<_> = r.split(' ').collect();
                self.rank0 = tokens[0].to_string();
                true
            }
            _ => false,
        }
    }
    fn rank0(&self) -> String {
        self.rank0.clone()
    }
}

pub struct Config {
    bindAddr: String,
    rank: Rank,
    world_size: Rank,
    rank0: String,
}

impl Config {
    pub fn create() -> Config {
        let mut r = Config {
            bindAddr: String::from(LOCALHOST),
            rank: 0,
            world_size: 1,
            rank0: String::from(LOCALHOST),
        };
        let l: [Box<dyn Topology + Send>; 1] = [
            Box::new(OpenMPITopology::new()),
            // TODO add MPICHTopology
        ];
        for mut i in l {
            if i.supported() {
                r.bindAddr = i.bindAddr();
                r.rank = i.rank();
                r.world_size = i.world_size();
            }
        }
        let l: [Box<dyn Rank0Getter + Send>; 1] = [
            Box::new(LSFRank0Getter::new()),
            // TODO add SLURM
        ];
        for mut i in l {
            if i.supported() {
                r.rank0 = i.rank0();
            }
        }
        if r.rank0 == LOCALHOST {
            r.bindAddr = String::from(LOCALHOST);
        }
        r
    }

    pub fn bindAddr(&self) -> String {
        self.bindAddr.clone()
    }

    pub fn rank(&self) -> Rank {
        self.rank
    }

    pub fn world_size(&self) -> Rank {
        self.world_size
    }

    pub fn rank0(&self) -> String {
        self.rank0.clone()
    }
}
