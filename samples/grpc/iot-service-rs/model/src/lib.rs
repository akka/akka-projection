//! The purpose of the model crate is to share types and traits between
//! both the frontend and the backend. In particular, we use it to share
//! the temperature entity behavior so that we can event source temperature
//! observations both at the front and the back.

pub mod temperature;
