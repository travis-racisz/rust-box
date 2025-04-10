use axum::extract::{Path, State};
use axum::{
    Json, Router,
    response::{Html, IntoResponse},
    routing::get,
};
use rodio::{Decoder, OutputStream, Sink};
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::spawn_blocking;

enum AudioCommand {
    Play(String),
    Pause,
    Stop,
}

struct AppState {
    audio_tx: mpsc::Sender<AudioCommand>,
}

#[tokio::main]
async fn main() {
    let (audio_tx, audio_rx) = mpsc::channel::<AudioCommand>(32);

    let app_state = Arc::new(AppState { audio_tx });

    tokio::spawn(audio_player(audio_rx));

    let app = Router::new()
        .route("/", get(serve_index))
        .route(
            "/play/{artist}/{song}",
            get({
                let shared_state = Arc::clone(&app_state);
                move |path| play_song1(axum::extract::State(shared_state), path)
            }),
        )
        .route("/stop", get(stop_music))
        .route("/pause", get(pause_music))
        .route("/get_library", get(get_library))
        .route("/{artist}", get(move |path| get_artist_dir(path)))
        .with_state(app_state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();

    axum::serve(listener, app).await.unwrap();
}

async fn serve_index() -> Html<String> {
    Html(include_str!("../assets/index.html").to_string())
}

async fn get_library() -> Json<Vec<String>> {
    let mut library: Vec<String> = vec![];
    for entry in std::fs::read_dir("assets/music").unwrap() {
        if let Ok(entry) = entry {
            if let Some(filename) = entry.file_name().to_str() {
                library.push(filename.to_owned());
            }
        }
    }
    Json(library)
}

async fn get_artist_dir(Path(artist): Path<String>) -> Json<Vec<String>> {
    let mut artist_dir = vec![];
    for entry in std::fs::read_dir(format!("assets/music/{}", artist)).unwrap() {
        if let Ok(entry) = entry {
            if let Some(filename) = entry.file_name().to_str() {
                artist_dir.push(filename.to_owned())
            }
        }
    }
    Json(artist_dir)
}

async fn stop_music(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    if let Err(e) = state.audio_tx.send(AudioCommand::Stop).await {
        eprintln!("Failed to send stop command: {}", e);
        return Html::<String>("Error stopping music".into());
    }

    Html("Stopping Music".into())
}

async fn audio_player(mut rx: mpsc::Receiver<AudioCommand>) {
    spawn_blocking(move || {
        let (_stream, stream_handle) = match OutputStream::try_default() {
            Ok(output) => output,
            Err(e) => {
                eprintln!("Failed to create audio output stream: {}", e);
                return;
            }
        };

        let mut current_sink: Option<Sink> = None;

        while let Some(cmd) = rx.blocking_recv() {
            match cmd {
                AudioCommand::Play(path) => {
                    println!("{}", path);

                    if let Some(sink) = current_sink.take() {
                        sink.stop();
                    }

                    match Sink::try_new(&stream_handle) {
                        Ok(sink) => match File::open(&path) {
                            Ok(file) => {
                                let buf_reader = BufReader::new(file);
                                match Decoder::new(buf_reader) {
                                    Ok(source) => {
                                        sink.append(source);
                                        current_sink = Some(sink);
                                        println!("Playing: {}", path);
                                    }
                                    Err(e) => eprintln!("Error decoding audio: {}", e),
                                }
                            }
                            Err(e) => eprintln!("Error opening audio file: {}", e),
                        },
                        Err(e) => eprintln!("Error creating audio sink: {}", e),
                    }
                }
                AudioCommand::Stop => {
                    if let Some(sink) = current_sink.take() {
                        println!("Stopping audio");
                        sink.stop();
                    }
                }
                AudioCommand::Pause => {
                    if let Some(sink) = current_sink.take() {
                        println!("Pausing Music");
                        sink.pause();
                    }
                }
            }
        }
    })
    .await
    .unwrap();
}

async fn pause_music(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    if let Err(e) = state.audio_tx.send(AudioCommand::Pause).await {
        eprintln!("Failed to send pause command: {}", e);
        return Html::<String>("Error pausing music".into());
    }

    Html("Pausing music".into())
}

async fn play_song1(
    State(state): State<Arc<AppState>>,
    Path((artist, song)): Path<(String, String)>,
) -> impl IntoResponse {
    if let Err(e) = state
        .audio_tx
        .send(AudioCommand::Play(format!("assets/music/{artist}/{song}")))
        .await
    {
        eprintln!("Failed to send play command: {}", e);
        return Html::<String>("Error playing music".into());
    }

    Html("Playing song 1".into())
}
