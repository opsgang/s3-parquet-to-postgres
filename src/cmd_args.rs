use anyhow::Result;
use log::error;
use std::error::Error;
use std::fmt;

#[derive(Debug)]
struct IncorrectArgs;
impl fmt::Display for IncorrectArgs {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "You must pass path to the config yaml")
    }
}

impl Error for IncorrectArgs {}

pub fn config_yaml<F>(get_args: F) -> Result<String>
where
    F: Fn() -> Vec<String>,
{
    let args: Vec<String> = get_args();

    // 1st arg is the binary. 2nd arg on is what we want
    if args.len() != 2 {
        error!("Incorrect number of args passed");
        return Err(IncorrectArgs.into());
    }
    Ok(args[1].clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

    #[test]
    fn test_config_yaml_succeeds_with_one_argument() -> Result<()> {
        // Mocked version of std::env::args
        let mock_args = || {
            vec![
                "test_binary".to_string(),
                "/path/to/config.yaml".to_string(),
            ]
        };

        // Call config_yaml with the mocked args
        let result = config_yaml(mock_args);

        // Assert that the result is Ok and the value is correct
        assert!(result.is_ok());
        assert_eq!(result?, "/path/to/config.yaml".to_string());

        Ok(())
    }

    #[test]
    fn test_config_yaml_fails_with_no_arguments() -> Result<()> {
        // Mocked version of std::env::args with no additional arguments
        let mock_args = || vec!["test_binary".to_string()];

        // Call config_yaml with the mocked args
        let result = config_yaml(mock_args);

        // Assert that the result is an error
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_config_yaml_fails_with_too_many_arguments() -> Result<()> {
        // Mocked version of std::env::args with too many arguments
        let mock_args = || {
            vec![
                "test_binary".to_string(),
                "/path/to/config.yaml".to_string(),
                "extra_arg".to_string(),
            ]
        };

        // Call config_yaml with the mocked args
        let result = config_yaml(mock_args);

        // Assert that the result is an error
        assert!(result.is_err());

        Ok(())
    }
}
