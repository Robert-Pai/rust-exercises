//! Binance USD-M Futures implementation of the maker exchange ports.
//!
//! Binance wire types are intentionally private. Public consumers interact
//! only through [`BinanceUsdm`] and exchange-neutral domain/port types.

#![forbid(unsafe_code)]

mod adapter;
mod config;
mod error;
mod mapping;
mod models;
mod network;
mod rate_limit;
mod rest;
mod signing;
mod websocket;
mod ws_api;

pub use adapter::BinanceUsdm;
pub use config::{BinanceCredentials, BinanceUsdmConfig};
