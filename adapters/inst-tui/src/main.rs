mod app;
mod data;
mod event;
mod redis_client;
mod ui;

use app::App;
use clap::Parser;
use crossterm::{
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::prelude::*;
use redis_client::SharedState;
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(name = "inst-tui", about = "Instrument TUI for BPM/BLM/BCM monitoring")]
struct Args {
    /// Redis hosts to connect to (host:port or just host for default port 6379)
    #[arg(long, value_delimiter = ',')]
    hosts: Option<Vec<String>>,

    /// Config file with one host[:port] per line
    #[arg(long, short)]
    config: Option<String>,

    /// Data refresh interval in milliseconds
    #[arg(long, default_value = "250")]
    refresh: u64,
}

fn parse_host(s: &str) -> (String, u16) {
    if let Some((host, port_str)) = s.rsplit_once(':') {
        if let Ok(port) = port_str.parse::<u16>() {
            return (host.to_string(), port);
        }
    }
    (s.to_string(), 6379)
}

fn load_hosts(args: &Args) -> Vec<(String, u16)> {
    let mut hosts = Vec::new();

    if let Some(ref host_list) = args.hosts {
        for h in host_list {
            hosts.push(parse_host(h.trim()));
        }
    }

    if let Some(ref path) = args.config {
        if let Ok(contents) = std::fs::read_to_string(path) {
            for line in contents.lines() {
                let trimmed = line.trim();
                if !trimmed.is_empty() && !trimmed.starts_with('#') {
                    hosts.push(parse_host(trimmed));
                }
            }
        } else {
            eprintln!("Warning: could not read config file: {}", path);
        }
    }

    if hosts.is_empty() {
        hosts.push(("127.0.0.1".to_string(), 6379));
    }

    hosts
}

fn main() -> io::Result<()> {
    let args = Args::parse();
    let hosts = load_hosts(&args);

    eprintln!(
        "inst-tui: connecting to {} Redis host(s)...",
        hosts.len()
    );
    for (h, p) in &hosts {
        eprintln!("  {}:{}", h, p);
    }

    let state = Arc::new(Mutex::new(SharedState::default()));

    // Start background data fetchers (one per Redis host)
    let _fetchers = redis_client::start_fetcher(hosts.clone(), state.clone(), args.refresh);

    // Wait briefly for initial discovery
    std::thread::sleep(Duration::from_millis(500));

    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut app = App::new(state, hosts);
    let tick_rate = Duration::from_millis(50);

    // Main render loop
    while app.running {
        terminal.draw(|f| ui::render(f, &mut app))?;

        if let Some(evt) = event::poll_event(tick_rate) {
            match evt {
                event::AppEvent::Key(key) => event::handle_key(&mut app, key),
                event::AppEvent::Tick => {}
            }
        }
    }

    // Restore terminal
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    Ok(())
}
