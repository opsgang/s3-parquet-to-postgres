// This sets up locally running s3 and db to simplify our testing.
// We need to bring it into the runtime in main.rs but we can minimise
// its impact as its only needed during testing.
//
// Yes, using docker evidences that several of my "unit-tests" have external dependencies
// Still, this approach is simpler than the amount of code changes needed by mocking.
// I have faith in the versimilitude of localstack and docker postgres, both well-supported projects.
// Besides, I'm more interested in whether the thing works end-to-end, than in unit-testing.
use once_cell::sync::Lazy;
use std::env;
use std::path::Path;
use std::process::Command;

fn docker_exists() -> bool {
    Command::new("docker")
        .arg("--version")
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

fn run_docker_compose_down() {
    let status = Command::new("docker")
        .args(["compose", "down", "--remove-orphans", "-v"])
        .status()
        .expect("Failed to run docker compose down");

    if !status.success() {
        panic!("docker compose down failed");
    }
}

fn run_docker_compose_up() {
    let status = Command::new("docker")
        .args(["compose", "up", "-d", "--wait"])
        .status()
        .expect("Failed to run docker compose up");

    if !status.success() {
        panic!("docker compose up failed");
    }
}

// Setup function that runs once before all tests
pub fn setup_docker() {
    static DOCKER_SETUP: Lazy<()> = Lazy::new(|| {
        if !docker_exists() {
            panic!("Docker is not installed or not found in PATH");
        }

        // cd to docker-compose dir
        let cargo_manifest_dir =
            env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
        let local_dir = Path::new(&cargo_manifest_dir).join("local");

        env::set_current_dir(&local_dir).expect("Failed to change directory to local");

        run_docker_compose_down();
        run_docker_compose_up();
    });

    Lazy::force(&DOCKER_SETUP);
}
