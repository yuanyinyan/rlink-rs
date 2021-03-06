pub mod memory;
pub mod network;

pub(crate) const DEFAULT_CHANNEL_SIZE: usize = 10240;

#[derive(Copy, Clone, Debug)]
pub(crate) enum ChannelType {
    Memory = 1,
    Network = 2,
}
