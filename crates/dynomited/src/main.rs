//! `dynomited` is the server binary that drives the `dynomite` engine.

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, Eq, PartialEq)]
enum Action {
    PrintHelp,
    PrintVersion,
}

fn parse_action<I: IntoIterator<Item = String>>(args: I) -> Action {
    for arg in args {
        match arg.as_str() {
            "-V" | "--version" => return Action::PrintVersion,
            "-h" | "--help" => return Action::PrintHelp,
            _ => {}
        }
    }
    Action::PrintHelp
}

fn main() {
    match parse_action(std::env::args().skip(1)) {
        Action::PrintVersion => println!("dynomited {VERSION}"),
        Action::PrintHelp => print_help(),
    }
}

fn print_help() {
    println!(
        "Usage: dynomited [-?hV]\n\n\
         Options:\n  \
         -h, --help     this help\n  \
         -V, --version  show version and exit\n"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn help_is_default() {
        assert_eq!(parse_action(std::iter::empty()), Action::PrintHelp);
    }

    #[test]
    fn version_short_flag() {
        let args = vec!["-V".to_string()];
        assert_eq!(parse_action(args), Action::PrintVersion);
    }

    #[test]
    fn version_long_flag() {
        let args = vec!["--version".to_string()];
        assert_eq!(parse_action(args), Action::PrintVersion);
    }

    #[test]
    fn help_short_flag() {
        let args = vec!["-h".to_string()];
        assert_eq!(parse_action(args), Action::PrintHelp);
    }
}
