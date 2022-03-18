pub mod random;
mod stop_handle;
mod stoppable;
mod uniq_id;

pub use stop_handle::StopHandle;
pub use stoppable::Stoppable;
pub use uniq_id::{GlobalSequence, GlobalUniqName};

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
