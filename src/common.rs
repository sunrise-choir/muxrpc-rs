use packet_stream::{Metadata, PacketType};

pub const JSON_TRUE: [u8; 4] = [116, 114, 117, 101];

pub const META_NON_END: Metadata = Metadata {
    packet_type: PacketType::Json,
    is_end: false,
};

pub const META_END: Metadata = Metadata {
    packet_type: PacketType::Json,
    is_end: true,
};
