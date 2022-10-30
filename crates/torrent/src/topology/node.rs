use std::{
    io::{Result as IOResut, Error, ErrorKind},
    net::{IpAddr, Ipv4Addr, SocketAddr},
};

use serde::{
    de::{self},
    Deserialize, Serialize,
};
use vendor::prelude::{local_dns_lookup, DashMap, DashSet};

#[derive(Debug, Clone)]
pub struct Node<I> {
    pub id: I,
    pub ori_addr: String,
    pub socket_addr: SocketAddr,

    pub labels: DashSet<String>,
    pub ports: DashMap<String, u16>,
}

impl<I: Default> Default for Node<I> {
    fn default() -> Self {
        Self {
            id: Default::default(),
            ori_addr: Default::default(),
            socket_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080),
            labels: Default::default(),
            ports: Default::default(),
        }
    }
}

#[inline]
fn _invalid_address(input: &str, exp: &str) -> Error {
    Error::new(
        ErrorKind::InvalidInput, 
        format!("unexpected format of address: {input}, require {exp}")
    )
}

impl<T> Node<T> {
    pub fn new(id: T, scheme: &str, address: &str) -> IOResut<Self> {
        let socket_addr = local_dns_lookup(address)?;

        let initial = DashMap::new();
        initial.insert(scheme.to_string(), socket_addr.port());
        Ok(Self {
            id,
            ori_addr: address.to_owned(),
            socket_addr,
            labels: DashSet::new(),
            ports: initial,
        })
    }

    #[inline]
    pub fn is_valid(&self) -> bool {
        !self.ori_addr.is_empty() && !self.ports.is_empty()
    }

    /// Parse `Node` from full qualified address, 
    /// correct format of `full_address` should be:
    /// "scheme://address", e.g. "http://127.0.0.1:8080"
    pub fn parse(id: T, full_address: &str) -> IOResut<Self> {
        let split = full_address
            .find("://")
            .ok_or(_invalid_address(full_address, "scheme://host:port"))?;

        let scheme = &full_address[0..split];
        let address = &full_address[split + 3..];
        Self::new(id, scheme, address)
    }

    #[inline]
    pub fn add_port(&self, scheme: &str, port: u16) -> &Self {
        self.ports.insert(scheme.to_owned(), port);
        self
    }

    #[inline]
    pub fn remove_port(&self, scheme: &str) -> &Self {
        self.ports.remove(scheme);
        self
    }

    #[inline]
    pub fn get_port(&self, scheme: &str) -> Option<u16> {
        self.ports.get(scheme).as_ref().map(|p| *p.value())
    }

    #[inline]
    pub fn port_num(&self) -> usize {
        self.ports.len()
    }

    // Clone all ports with scheme from this node.
    pub fn clone_ports(&self) -> Vec<(String, u16)> {
        self.ports.iter()
            .map(|p| (p.key().clone(), *p.value()))
            .collect()
    }

    /// Get socket of scheme.
    pub fn socket(&self, scheme: &str) -> Option<SocketAddr> {
        let mut socket = self.socket_addr;
        let port = *self.ports.get(scheme)?;
        socket.set_port(port);
        Some(socket)
    }

    pub fn merge(&self, other: &Node<T>) {
        let Node { labels, ports, .. } = other;
        for port in ports.iter() {
            self.add_port(port.key().as_str(), *port.value());
        }
        for label in labels.iter() {
            self.labels.insert(label.clone());
        }
    }
}

const ID: &str = "id";
const ORI_ADDR: &str = "ori_addr";
const SOCKET_ADDR: &str = "socket_addr";
const LABELS: &str = "labels";
const PORTS: &str = "ports";

impl<T: Default + PartialEq + Serialize> Serialize for Node<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(5))?;
        map.serialize_entry(ID, &self.id)?;
        map.serialize_entry(ORI_ADDR, &self.ori_addr)?;
        map.serialize_entry(SOCKET_ADDR, &self.socket_addr)?;
        let labels: Vec<String> = self.labels.iter().map(|l| l.to_string()).collect();
        map.serialize_entry(LABELS, &labels)?;
        let ports: Vec<(String, u16)> = self
            .ports
            .iter()
            .map(|port| (port.key().clone(), *port.value()))
            .collect();
        map.serialize_entry(PORTS, &ports)?;
        map.end()
    }
}

impl<'de, T: Clone + Default + PartialEq + Deserialize<'de>> Deserialize<'de> for Node<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(Node::<T>::default())
    }
}

impl<'de, T> de::Visitor<'de> for Node<T>
where
    T: Clone + Default + PartialEq + Deserialize<'de>,
{
    type Value = Node<T>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a Node with id and socket address")
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Node::<T>::default())
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: de::MapAccess<'de>,
    {
        let mut node = Node::<T>::default();

        let id = map.next_entry::<&str, T>()?;
        if let Some((ID, id)) = id {
            node.id = id;
        }

        let ori_addr = map.next_entry::<&str, String>()?;
        if let Some((ORI_ADDR, ori_addr)) = ori_addr {
            node.ori_addr = ori_addr;
        }

        let socket_addr = map.next_entry::<&str, SocketAddr>()?;
        if let Some((SOCKET_ADDR, socket_addr)) = socket_addr {
            node.socket_addr = socket_addr;
        }

        let labels = map.next_entry::<&str, Vec<String>>()?;
        if let Some((LABELS, labels)) = labels {
            let label_set = DashSet::with_capacity(labels.len());
            for label in labels {
                label_set.insert(label);
            }
            node.labels = label_set;
        }

        let ports = map.next_entry::<&str, Vec<(String, u16)>>()?;
        if let Some((PORTS, ports)) = ports {
            let port_set = DashMap::with_capacity(ports.len());
            for (scheme, port) in ports {
                port_set.insert(scheme, port);
            }
            node.ports = port_set;
        }
        Ok(node)
    }
}