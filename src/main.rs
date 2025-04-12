use axum::extract::{Path, State};
use axum::{
    Json, Router,
    response::{Html, IntoResponse},
    routing::{get, post},
};
use axum_macros::debug_handler;
use rodio::{Decoder, OutputStream, Sink};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::BufReader;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio::task::spawn_blocking;
use tower_http::services::ServeDir;

enum AudioCommand {
    Play(String),
    PlayIndex(usize),
    Pause,
    Stop,
    Next,
    Previous,
    Increment,
    GetCurrentTrack,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Song {
    artist: String,
    album: String,
    title: String,
    path: String,
}

#[derive(Serialize, Deserialize)]
struct Album {
    artist: String,
    title: String,
    cover_art_path: String,
}

// Queue status response
#[derive(Debug, Serialize, Deserialize)]
struct QueueStatus {
    current_index: u32,
    queue: Vec<Song>,
    is_playing: bool,
}

// Application state with thread-safe queue
struct AppState {
    audio_tx: mpsc::Sender<AudioCommand>,
    song_queue: Arc<Mutex<Vec<Song>>>,
    current_index: Arc<AtomicU32>,
    is_playing: Arc<Mutex<bool>>,
}

#[tokio::main]
async fn main() {
    let (audio_tx, audio_rx) = mpsc::channel::<AudioCommand>(32);
    let song_queue = Arc::new(Mutex::new(Vec::new()));
    let current_index = Arc::new(AtomicU32::new(0));
    let is_playing = Arc::new(Mutex::new(false));

    let app_state = Arc::new(AppState {
        audio_tx,
        song_queue,
        current_index,
        is_playing,
    });

    tokio::spawn(audio_player(
        audio_rx,
        Arc::clone(&app_state),
        Arc::clone(&app_state.song_queue),
        Arc::clone(&app_state.current_index),
        Arc::clone(&app_state.is_playing),
    ));

    let app = Router::new()
        .route("/", get(serve_index))
        //.route_service("/music", ServeDir::new("assets/music"))
        .route(
            "/play/{artist}/{album}/{song}",
            get({
                let shared_state = Arc::clone(&app_state);
                move |path| play_song(State(shared_state), path)
            }),
        )
        .route(
            "/add_to_queue/{artist}/{album}/{song}",
            post({
                let shared_state = Arc::clone(&app_state);
                move |path| add_to_queue(State(shared_state), path)
            }),
        )
        .route(
            "/play_queue_index/{index}",
            get({
                let shared_state = Arc::clone(&app_state);
                move |path| play_queue_index(State(shared_state), path)
            }),
        )
        .route(
            "/next",
            get({
                let shared_state = Arc::clone(&app_state);
                move || next_song(State(shared_state))
            }),
        )
        .route(
            "/previous",
            get({
                let shared_state = Arc::clone(&app_state);
                move || previous_song(State(shared_state))
            }),
        )
        .route(
            "/queue",
            get({
                let shared_state = Arc::clone(&app_state);
                move || get_queue(State(shared_state))
            }),
        )
        .route(
            "/stop",
            get({
                let shared_state = Arc::clone(&app_state);
                move || stop_music(State(shared_state))
            }),
        )
        .route(
            "/pause",
            get({
                let shared_state = Arc::clone(&app_state);
                move || pause_music(State(shared_state))
            }),
        )
        .route("/get_library", get(get_library))
        .route("/{artist}", get(move |path| get_artist_dir(path)))
        .route(
            "/{artist}/{album}",
            get(move |path| get_artist_albums(path)),
        )
        .nest_service("/assets", ServeDir::new("assets"))
        .with_state(app_state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    println!("Listening on http://localhost:3000");
    axum::serve(listener, app).await.unwrap();
}

async fn serve_index() -> Html<String> {
    Html(include_str!("../assets/index.html").to_string())
}

async fn get_library() -> Json<Vec<String>> {
    let mut library: Vec<String> = vec![];
    match std::fs::read_dir("assets/music") {
        Ok(entries) => {
            for entry in entries {
                if let Ok(entry) = entry {
                    if let Some(filename) = entry.file_name().to_str() {
                        library.push(filename.to_owned());
                    }
                }
            }
        }
        Err(e) => {
            eprintln!("Error reading music library: {}", e);
        }
    }
    Json(library)
}

async fn get_artist_dir(Path(artist): Path<String>) -> Json<Vec<Album>> {
    let mut artist_albums = vec![];
    match std::fs::read_dir(format!("assets/music/{}", artist)) {
        Ok(entries) => {
            for entry in entries {
                if let Ok(entry) = entry {
                    if let Some(album_name) = entry.file_name().to_str() {
                        // Find cover art in the album directory
                        let mut cover_art_path = String::new();
                        if let Ok(album_entries) =
                            std::fs::read_dir(format!("assets/music/{}/{}", artist, album_name))
                        {
                            for album_entry in album_entries {
                                if let Ok(album_entry) = album_entry {
                                    if let Some(filename) = album_entry.file_name().to_str() {
                                        if filename.ends_with(".jpg") || filename.ends_with(".png")
                                        {
                                            cover_art_path = filename.to_owned();
                                            break; // Use the first image file found
                                        }
                                    }
                                }
                            }
                        }

                        // Add album to list
                        artist_albums.push(Album {
                            artist: artist.clone(),
                            title: album_name.to_owned(),
                            cover_art_path,
                        });
                    }
                }
            }
        }
        Err(e) => {
            eprintln!("Error reading artist directory: {}", e);
        }
    }
    Json(artist_albums)
}

#[debug_handler]
async fn get_artist_albums(Path((artist, album)): Path<(String, String)>) -> Json<Vec<Album>> {
    let mut artist_albums = vec![];
    let mut cover = "".to_owned();

    // First, try to find any cover art in the album directory
    match std::fs::read_dir(format!("assets/music/{}/{}", artist, album)) {
        Ok(entries) => {
            // First pass - look for cover art
            for entry in entries {
                if let Ok(entry) = entry {
                    if let Some(filename) = entry.file_name().to_str() {
                        if filename.ends_with(".jpg") || filename.ends_with(".png") {
                            cover = filename.to_owned();
                            break; // Use the first image file found
                        }
                    }
                }
            }

            // Second pass - process song files
            if let Ok(entries) = std::fs::read_dir(format!("assets/music/{}/{}", artist, album)) {
                for entry in entries {
                    if let Ok(entry) = entry {
                        if let Some(filename) = entry.file_name().to_str() {
                            if filename.ends_with(".flac")
                                || filename.ends_with(".mp3")
                                || filename.ends_with(".wav")
                            {
                                artist_albums.push(Album {
                                    artist: artist.clone(),
                                    title: filename.to_owned(),
                                    cover_art_path: cover.clone(),
                                });
                            }
                        }
                    }
                }
            }
        }
        Err(e) => {
            eprintln!("Error reading album directory: {}", e);
        }
    }

    Json(artist_albums)
}

async fn stop_music(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    if let Err(e) = state.audio_tx.send(AudioCommand::Stop).await {
        eprintln!("Failed to send stop command: {}", e);
        return Html::<String>("Error stopping music".into());
    }

    // Update playing state
    {
        let mut is_playing = state.is_playing.lock().unwrap();
        *is_playing = false;
    }

    Html("Stopping Music".into())
}

async fn play_song(
    State(state): State<Arc<AppState>>,
    Path((artist, album, song)): Path<(String, String, String)>,
) -> impl IntoResponse {
    let path = format!("assets/music/{artist}/{album}/{song}");

    {
        let mut queue = state.song_queue.lock().unwrap();
        queue.push(Song {
            artist: artist.clone(),
            album: album.clone(),
            title: song.clone(),
            path: path.clone(),
        });

        // Reset current index
        state.current_index.store(0, Ordering::Relaxed);

        // Set playing state to true
        let mut is_playing = state.is_playing.lock().unwrap();
        *is_playing = true;
    }

    if let Err(e) = state.audio_tx.send(AudioCommand::Play(path)).await {
        eprintln!("Failed to send play command: {}", e);
        return Html::<String>("Error playing music".into());
    }

    Html(format!("Playing {song} by {artist}").into())
}

async fn add_to_queue(
    State(state): State<Arc<AppState>>,
    Path((artist, album, song)): Path<(String, String, String)>,
) -> impl IntoResponse {
    let path = format!("assets/music/{artist}/{album}/{song}");
    let queue_was_empty;

    // Add to queue
    {
        let mut queue = state.song_queue.lock().unwrap();
        queue_was_empty = queue.is_empty();

        queue.push(Song {
            artist,
            album,
            title: song.clone(),
            path: path.clone(),
        });
    }

    // If the queue was empty, we should start playing this song immediately
    if queue_was_empty {
        // Start playing the first song
        if let Err(e) = state.audio_tx.send(AudioCommand::Play(path)).await {
            eprintln!("Failed to start playing first queued song: {}", e);
            return Html::<String>(
                format!("Added {song} to queue, but failed to start playback").into(),
            );
        }
    }

    // Set playing state to true
    {
        let mut is_playing = state.is_playing.lock().unwrap();
        *is_playing = true;
    }

    return Html(format!("Added {song} to queue and started playback").into());
}

async fn play_queue_index(
    State(state): State<Arc<AppState>>,
    Path(index): Path<usize>,
) -> impl IntoResponse {
    let song_title;

    {
        let queue = state.song_queue.lock().unwrap();
        if index >= queue.len() {
            return Html("Invalid queue index".into());
        }

        song_title = queue[index].title.clone();

        // Update current index
        // state.current_index.store(index, Ordering::Relaxed);

        // Set playing state to true
        let mut is_playing = state.is_playing.lock().unwrap();
        *is_playing = true;
    }

    if let Err(e) = state.audio_tx.send(AudioCommand::PlayIndex(index)).await {
        eprintln!("Failed to send play index command: {}", e);
        return Html::<String>("Error playing music from queue".into());
    }

    Html(format!("Playing '{}' from queue", song_title).into())
}

async fn next_song(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    if let Err(e) = state.audio_tx.send(AudioCommand::Next).await {
        eprintln!("Failed to send next command: {}", e);
        return Html::<String>("Error skipping to next song".into());
    }

    // Set playing state to true
    {
        let mut is_playing = state.is_playing.lock().unwrap();
        *is_playing = true;
    }

    Html("Playing next song".into())
}

async fn previous_song(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    if let Err(e) = state.audio_tx.send(AudioCommand::Previous).await {
        eprintln!("Failed to send previous command: {}", e);
        return Html::<String>("Error going to previous song".into());
    }

    // Set playing state to true
    {
        let mut is_playing = state.is_playing.lock().unwrap();
        *is_playing = true;
    }

    Html("Playing previous song".into())
}

#[debug_handler]
async fn get_queue(State(state): State<Arc<AppState>>) -> Json<QueueStatus> {
    let queue = state.song_queue.try_lock().unwrap().clone();
    let current_index = state.current_index.load(Ordering::Relaxed);
    let is_playing = *state.is_playing.lock().unwrap();

    Json(QueueStatus {
        current_index,
        queue,
        is_playing,
    })
}

async fn pause_music(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    if let Err(e) = state.audio_tx.send(AudioCommand::Pause).await {
        eprintln!("Failed to send pause command: {}", e);
        return Html::<String>("Error pausing music".into());
    }

    // Update playing state
    {
        let mut is_playing = state.is_playing.lock().unwrap();
        *is_playing = false;
    }

    Html("Pausing music".into())
}

async fn audio_player(
    mut rx: mpsc::Receiver<AudioCommand>,
    state: Arc<AppState>,
    queue: Arc<Mutex<Vec<Song>>>,
    current_index: Arc<AtomicU32>,
    is_playing: Arc<Mutex<bool>>,
) {
    spawn_blocking(move || {
        let (_stream, stream_handle) = OutputStream::try_default().unwrap();
        let sink = Sink::try_new(&stream_handle).unwrap();
        loop {
            if let Ok(cmd) = rx.try_recv() {
                match cmd {
                    AudioCommand::Play(path) => {
                        let file = BufReader::new(File::open(path).unwrap());
                        let source = Decoder::new(file).unwrap();
                        sink.append(source);

                        let idx_clone = current_index.clone();
                        sink.append(rodio::source::EmptyCallback::<f32>::new(Box::new(
                            move || {
                                idx_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            },
                        )));
                    }
                    AudioCommand::Pause => {
                        if sink.is_paused() {
                            sink.play();
                        } else {
                            sink.pause();
                        }
                    }
                    AudioCommand::Next => {
                        sink.skip_one();
                        queue.lock().unwrap().remove(0);
                    }
                    AudioCommand::Stop => {
                        sink.stop();
                        queue.lock().unwrap().clear();
                        current_index.store(0, Ordering::Relaxed);
                    }
                    AudioCommand::PlayIndex(index) => {
                        let mut i = 0;
                        while i <= index {
                            sink.skip_one();
                            queue.lock().unwrap().remove(0);
                            i += 1;
                        }
                        current_index.store(0, Ordering::Relaxed);
                    }
                    AudioCommand::Increment => {
                        current_index.fetch_add(1, Ordering::Relaxed);
                    }
                    _ => {}
                }
            }
        }
    });
}
