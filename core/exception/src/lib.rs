pub mod exception;
mod exception_code;
mod exception_into;

pub use exception::{ErrorCode, Result, ToErrorCode};

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
