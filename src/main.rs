use std::io::Write;
use std::process::ExitCode;

fn main() -> ExitCode {
    match files_sync::cli::run() {
        // Output was cut short by the reader (e.g. `status | head`): quiet,
        // with the status a shell shows for a process killed by SIGPIPE.
        Ok(()) if files_sync::cli::stdout_closed() => {
            ExitCode::from(files_sync::cli::EXIT_STDOUT_CLOSED)
        }
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            // As `fn main() -> anyhow::Result<()>` reports it, without
            // panicking if stderr is closed too.
            let _ = writeln!(std::io::stderr(), "Error: {e:?}");
            ExitCode::FAILURE
        }
    }
}
