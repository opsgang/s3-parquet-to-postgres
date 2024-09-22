* Need kitchen sink example that uses the `test_setup`
    * use integration test - can call the test_setup function
    * split out main.rs boilerplate into separate wrapper.rs
        * should ultimately call one function with path to yaml

* Add db + write example with cols with special chars
    (should be quoted in SELECT and INSERT)


```rust
mod test_setup;

#[cfg(test)]
mod tests {
    use super::test_setup::setup_docker;

    #[test]
    fn test_something_integration() {
        // Run the setup before the test
        setup_docker();

        // Your test logic here
        call the same wrapper func called by main.rs but different config.yml
        to illustrate what is happening.
    }
}
```
