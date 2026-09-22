//! uw-daemon: long-running polling/scheduling/delivery process.

fn print_help() {
    println!(
        "{} {}\nlong-running polling/scheduling/delivery process\n\nUsage: uw-daemon\n\nOptions:\n  -h, --help     Print help\n  -V, --version  Print version",
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION")
    );
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    match std::env::args().nth(1).as_deref() {
        Some("-h") | Some("--help") => {
            print_help();
            return Ok(());
        }
        Some("-V") | Some("--version") => {
            println!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));
            return Ok(());
        }
        Some(other) => {
            eprintln!("uw-daemon: unrecognized argument '{other}'");
            print_help();
            std::process::exit(2);
        }
        None => {}
    }
    tracing_subscriber::fmt::init();
    uw_daemon::run_daemon_loop().await
}
