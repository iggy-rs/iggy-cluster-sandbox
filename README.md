# Iggy Cluster Sandbox

Sandbox for the future implementation of [Iggy.rs](https://iggy.rs) clustering feature, currently based on [Raft](https://raft.github.io) consensus algorithm.

To compile the project, install the nightly version of Rust, which is required by [monoio](https://github.com/bytedance/monoio) runtime.

To start the project, at first open 3 distinct terminals and set the following environment variables:

```bash
export IGGY_NODE_CONFIG_PATH=configs/node1.toml
```

```bash
export IGGY_NODE_CONFIG_PATH=configs/node2.toml
```

```bash
export IGGY_NODE_CONFIG_PATH=configs/node3.toml
```

Then, in each terminal, simply run each node with `cargo r --bin iggy-node`.
The cluster should start and the **leader election** process should begin.
Try disconnecting one of the nodes and see how the cluster reacts.
The data will be stored under `local_data` directory unless configured otherwise.

To start the CLI, run `cargo r --bin iggy-cli`.