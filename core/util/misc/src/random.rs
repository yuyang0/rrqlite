use rand::distributions::Alphanumeric;
use rand::Rng; // 0.8

pub fn thread_rand_string(length: usize) -> String {
    let s: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(length)
        .map(char::from)
        .collect();
    s
}
