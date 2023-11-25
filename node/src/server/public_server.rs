use crate::clusters::cluster::Cluster;
use crate::server::tcp_server;
use std::rc::Rc;

const NAME: &str = "Iggy public server";

pub fn start(address: &str, cluster: Rc<Cluster>) {
    tcp_server::start(NAME, address, cluster)
}
