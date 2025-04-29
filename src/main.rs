use axum::extract::{Path, State, Query};
use audiotags::Tag;
use axum::{
    Json, Router,
    response::{Html, IntoResponse, Sse},
    routing::{get, post},
};
use axum_macros::debug_handler;
use futures::stream::{self, Stream};
use rodio::{Decoder, OutputStream, Sink};
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::fs::File;
use std::io::BufReader;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{broadcast, mpsc};
use tokio::task::spawn_blocking;
use tower_http::services::ServeDir;
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SseEvent {
    event_type: String,
    data: String,
}

enum AudioCommand {
    Play(String),
    PlayIndex(usize),
    Pause,
    Stop,
    Next,
    Previous,
    SetVolume(f32),
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
    filename: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct QueueStatus {
    current_index: u32,
    queue: Vec<Song>,
    is_playing: bool,
}

#[derive(Debug, Serialize)]
struct SearchResult {
    title: String,
    artist: String,
    album: String,
    cover_art_path: String,
    file_extension: String,
    filename: String,
}

struct AppState {
    audio_tx: mpsc::Sender<AudioCommand>,
    song_queue: Arc<Mutex<Vec<Song>>>,
    current_index: Arc<AtomicU32>,
    is_playing: Arc<Mutex<bool>>,
    sse_tx: broadcast::Sender<SseEvent>,
}

#[tokio::main]
async fn main() {
    let (audio_tx, audio_rx) = mpsc::channel::<AudioCommand>(32);
    let song_queue = Arc::new(Mutex::new(Vec::new()));
    let current_index = Arc::new(AtomicU32::new(0));
    let is_playing = Arc::new(Mutex::new(false));

    let (sse_tx, _) = broadcast::channel::<SseEvent>(100);
    let app_state = Arc::new(AppState {
        audio_tx,
        song_queue,
        current_index,
        is_playing,
        sse_tx: sse_tx.clone(),
    });

    tokio::spawn(audio_player(
        audio_rx,
        Arc::clone(&app_state),
        Arc::clone(&app_state.song_queue),
        Arc::clone(&app_state.current_index),
        Arc::clone(&app_state.is_playing),
        sse_tx.clone(),
    ));

    let app = Router::new()
        .route("/", get(serve_index))
        //.route_service("/music", ServeDir::new("assets/music"))
        .route(
            "/events",
            get({
                let shared_state = Arc::clone(&app_state);
                move || sse_handler(State(shared_state))
            }),
        )
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
        .route("/search", get(search_songs))
        .route("/set_volume", get(set_volume))
        .nest_service("/assets", ServeDir::new("assets"))
        .with_state(app_state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    println!("Listening on http://localhost:3000");
    axum::serve(listener, app).await.unwrap();
}

async fn serve_index() -> Html<String> {
    Html(include_str!("../assets/index.html").to_string())
}

async fn sse_handler(
    State(state): State<Arc<AppState>>,
) -> Sse<impl Stream<Item = Result<axum::response::sse::Event, Infallible>>> {
    let rx = state.sse_tx.subscribe();

    let stream = stream::unfold(rx, move |mut rx| async move {
        match rx.recv().await {
            Ok(msg) => {
                let event = axum::response::sse::Event::default()
                    .event(msg.event_type)
                    .data(msg.data);
                Some((Ok(event), rx))
            }
            Err(_) => None,
        }
    });

    if let Ok(queue) = state.song_queue.lock() {
        let current_index = state.current_index.load(Ordering::Relaxed);
        let is_playing = *state.is_playing.lock().unwrap();

        let queue_status = QueueStatus {
            current_index,
            queue: queue.clone(),
            is_playing,
        };

        if let Ok(json) = serde_json::to_string(&queue_status) {
            let _ = state.sse_tx.send(SseEvent {
                event_type: "queue_update".to_string(),
                data: json,
            });
        }
    }

    Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(std::time::Duration::from_secs(15))
            .text("keep-alive-text"),
    )
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
                            filename: cover_art_path.clone(),
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
            let path = format!("assets/music/{}/{}", artist, album);
            if let Ok(entries) = std::fs::read_dir(path) {
                for entry in entries {
                    if let Ok(entry) = entry {
                        if let Some(filename) = entry.file_name().to_str() {
                            let song_path = format!("assets/music/{}/{}/{}", artist, album, filename);
                            
                            // Check if file has a valid audio extension
                            if let Some(ext) = std::path::Path::new(filename).extension() {
                                let ext = ext.to_string_lossy().to_lowercase();
                                if ext == "flac" || ext == "wav" || ext == "mp3" || ext == "mp4" {
                                    // Try to read metadata from the audio file
                                    let title = if let Ok(tags) = Tag::default().read_from_path(&song_path) {
                                        // Use the title from metadata if available
                                        tags.title().map(|t| t.to_string()).unwrap_or_else(|| {
                                            // Fallback to filename without extension
                                            filename.rsplit_once('.').map(|(name, _)| name.to_string()).unwrap_or_else(|| filename.to_string())
                                        })
                                    } else {
                                        // Fallback to filename without extension if metadata reading fails
                                        filename.rsplit_once('.').map(|(name, _)| name.to_string()).unwrap_or_else(|| filename.to_string())
                                    };
                                    
                                    artist_albums.push(Album {
                                        artist: artist.clone(),
                                        title: title,
                                        filename: filename.to_string(),
                                        cover_art_path: cover.clone(),
                                    });
                                }
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

    // broadcast_queue_update(&state);
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

        state.current_index.store(0, Ordering::Relaxed);

        let mut is_playing = state.is_playing.lock().unwrap();
        *is_playing = true;
    }

    if let Err(e) = state.audio_tx.send(AudioCommand::Play(path)).await {
        eprintln!("Failed to send play command: {}", e);
        return Html::<String>("Error playing music".into());
    }

    // broadcast_queue_update(&state);

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

        let mut is_playing = state.is_playing.lock().unwrap();
        *is_playing = true;
    }

    if let Err(e) = state.audio_tx.send(AudioCommand::PlayIndex(index)).await {
        eprintln!("Failed to send play index command: {}", e);
        return Html::<String>("Error playing music from queue".into());
    }

    // broadcast_queue_update(&state);

    Html(format!("Playing '{}' from queue", song_title).into())
}

async fn next_song(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    if let Err(e) = state.audio_tx.send(AudioCommand::Next).await {
        eprintln!("Failed to send next command: {}", e);
        return Html::<String>("Error skipping to next song".into());
    }
    // state.current_index.store(0, Ordering::Relaxed);

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

    // broadcast_queue_update(&state);

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

    {
        let mut is_playing = state.is_playing.lock().unwrap();
        *is_playing = false;
    }

    Html("Pausing music".into())
}

async fn audio_player(
    mut rx: mpsc::Receiver<AudioCommand>,
    _state: Arc<AppState>,
    queue: Arc<Mutex<Vec<Song>>>,
    current_index: Arc<AtomicU32>,
    is_playing: Arc<Mutex<bool>>,
    sse_tx: broadcast::Sender<SseEvent>,
) {
    spawn_blocking(move || {
        let (_stream, stream_handle) = OutputStream::try_default().unwrap();
        let sink = Sink::try_new(&stream_handle).unwrap();
        let mut current_volume = 0.5;
        
        loop {
            if let Ok(cmd) = rx.try_recv() {
                match cmd {
                    AudioCommand::Play(path) => {
                        println!("{:?}", path);
                        let file = BufReader::new(File::open(path).unwrap());
                        let source = Decoder::new(file).unwrap();
                        sink.set_volume(current_volume);
                        sink.append(source);

                        let idx_clone = current_index.clone();
                        // Create clones for the closure
                        let sse_tx_clone = sse_tx.clone();
                        let queue_clone = queue.clone();
                        let is_playing_clone = is_playing.clone();

                        sink.append(rodio::source::EmptyCallback::<f32>::new(Box::new(
                            move || {
                                idx_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                                // Send an SSE event when a track completes
                                // This ensures clients know when tracks auto-advance
                                if let Ok(queue_data) = queue_clone.lock() {
                                    let current_idx = idx_clone.load(Ordering::Relaxed);
                                    let is_playing_val = if let Ok(play) = is_playing_clone.lock() {
                                        *play
                                    } else {
                                        false
                                    };

                                    let queue_status = QueueStatus {
                                        current_index: current_idx,
                                        queue: queue_data.clone(),
                                        is_playing: is_playing_val,
                                    };

                                    if let Ok(json) = serde_json::to_string(&queue_status) {
                                        let _ = sse_tx_clone.send(SseEvent {
                                            event_type: "queue_update".to_string(),
                                            data: json,
                                        });
                                    }
                                }
                            },
                        )));

                        // Broadcast update immediately after starting playback
                        broadcast_queue_update_from_state(
                            &queue,
                            &current_index,
                            &is_playing,
                            &sse_tx,
                        );
                    }
                    AudioCommand::Pause => {
                        if sink.is_paused() {
                            sink.play();
                        } else {
                            sink.pause();
                        }

                        // Broadcast update immediately after pausing/resuming playback
                        broadcast_queue_update_from_state(
                            &queue,
                            &current_index,
                            &is_playing,
                            &sse_tx,
                        );
                    }
                    AudioCommand::Next => {
                        sink.stop();

                        let next_song_path: Option<String> = {
                            let mut queue = queue.lock().unwrap();

                            if queue.len() < 1 {
                                None
                            } else {
                                queue.remove(0);

                                queue.get(0).map(|song| song.path.clone())
                            }
                        };

                        if let Some(path) = next_song_path {
                            let file = BufReader::new(File::open(path).unwrap());
                            let source = Decoder::new(file).unwrap();
                            sink.append(source);

                            let idx_clone = current_index.clone();
                            let sse_tx_clone = sse_tx.clone();
                            let queue_clone = queue.clone();
                            let is_playing_clone = is_playing.clone();

                            sink.append(rodio::source::EmptyCallback::<f32>::new(Box::new(
                                move || {
                                    idx_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                                    if let Ok(queue_data) = queue_clone.lock() {
                                        let current_idx = idx_clone.load(Ordering::Relaxed);
                                        let is_playing_val =
                                            if let Ok(play) = is_playing_clone.lock() {
                                                *play
                                            } else {
                                                false
                                            };

                                        let queue_status = QueueStatus {
                                            current_index: current_idx,
                                            queue: queue_data.clone(),
                                            is_playing: is_playing_val,
                                        };

                                        if let Ok(json) = serde_json::to_string(&queue_status) {
                                            let _ = sse_tx_clone.send(SseEvent {
                                                event_type: "queue_update".to_string(),
                                                data: json,
                                            });
                                        }
                                    }
                                },
                            )));

                            current_index.store(0, Ordering::Relaxed);
                        }

                        broadcast_queue_update_from_state(
                            &queue,
                            &current_index,
                            &is_playing,
                            &sse_tx,
                        );
                    }
                    AudioCommand::Stop => {
                        sink.stop();
                        queue.lock().unwrap().clear();
                        current_index.store(0, Ordering::Relaxed);

                        // Broadcast update immediately after stopping playback
                        broadcast_queue_update_from_state(
                            &queue,
                            &current_index,
                            &is_playing,
                            &sse_tx,
                        );
                    }
                    AudioCommand::PlayIndex(index) => {
                        sink.stop();

                        let song_path: Option<String> = {
                            let mut queue = queue.lock().unwrap();

                            if index >= queue.len() {
                                None
                            } else {
                                if index > 0 {
                                    queue.drain(0..index);
                                }

                                queue.get(0).map(|song| song.path.clone())
                            }
                        };

                        if let Some(path) = song_path {
                            let file = BufReader::new(File::open(path).unwrap());
                            let source = Decoder::new(file).unwrap();
                            sink.append(source);

                            let idx_clone = current_index.clone();
                            let sse_tx_clone = sse_tx.clone();
                            let queue_clone = queue.clone();
                            let is_playing_clone = is_playing.clone();

                            sink.append(rodio::source::EmptyCallback::<f32>::new(Box::new(
                                move || {
                                    idx_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                                    if let Ok(queue_data) = queue_clone.lock() {
                                        let current_idx = idx_clone.load(Ordering::Relaxed);
                                        let is_playing_val =
                                            if let Ok(play) = is_playing_clone.lock() {
                                                *play
                                            } else {
                                                false
                                            };

                                        let queue_status = QueueStatus {
                                            current_index: current_idx,
                                            queue: queue_data.clone(),
                                            is_playing: is_playing_val,
                                        };

                                        if let Ok(json) = serde_json::to_string(&queue_status) {
                                            let _ = sse_tx_clone.send(SseEvent {
                                                event_type: "queue_update".to_string(),
                                                data: json,
                                            });
                                        }
                                    }
                                },
                            )));
                            broadcast_queue_update_from_state(
                                &queue,
                                &current_index,
                                &is_playing,
                                &sse_tx,
                            );
                            current_index.store(0, Ordering::Relaxed);
                        }
                    }
                    AudioCommand::SetVolume(volume) => {
                        current_volume = volume.clamp(0.0, 1.0);
                        sink.set_volume(current_volume);
                    }
                    _ => {}
                }
            }
        }
    });
}

fn broadcast_queue_update_from_state(
    queue: &Arc<Mutex<Vec<Song>>>,
    current_index: &Arc<AtomicU32>,
    is_playing: &Arc<Mutex<bool>>,
    sse_tx: &broadcast::Sender<SseEvent>,
) {
    // Safely get the queue status
    if let Ok(queue_data) = queue.lock() {
        let current_idx = current_index.load(Ordering::Relaxed);
        let is_playing_val = if let Ok(playing) = is_playing.lock() {
            *playing
        } else {
            false // Default if we can't get the lock
        };

        let queue_status = QueueStatus {
            current_index: current_idx,
            queue: queue_data.clone(),
            is_playing: is_playing_val,
        };

        if let Ok(json) = serde_json::to_string(&queue_status) {
            let _ = sse_tx.send(SseEvent {
                event_type: "queue_update".to_string(),
                data: json,
            });
        }
    }
}

async fn search_songs(Query(params): Query<HashMap<String, String>>) -> Json<Vec<SearchResult>> {
    let query = params.get("q").unwrap_or(&String::new()).to_lowercase();
    let filter = params.get("filter").unwrap_or(&String::from("all")).to_lowercase();
    let mut results = Vec::new();

    // If query is empty, return empty results
    if query.is_empty() {
        return Json(results);
    }

    // Read the music directory
    if let Ok(artists) = std::fs::read_dir("assets/music") {
        for artist_entry in artists.flatten() {
            let artist_name = artist_entry.file_name().to_string_lossy().into_owned();
            
            // Search in artist name if filter is "all" or "artists"
            if (filter == "all" || filter == "artists") && artist_name.to_lowercase().contains(&query) {
                // Add all albums by this artist
                if let Ok(albums) = std::fs::read_dir(artist_entry.path()) {
                    for album_entry in albums.flatten() {
                        let album_name = album_entry.file_name().to_string_lossy().into_owned();
                        let mut cover_art = String::new();
                        
                        // Find cover art
                        if let Ok(files) = std::fs::read_dir(album_entry.path()) {
                            for file in files.flatten() {
                                if let Some(ext) = file.path().extension() {
                                    if ext == "jpg" || ext == "png" {
                                        cover_art = file.file_name().to_string_lossy().into_owned();
                                        break;
                                    }
                                }
                            }
                        }
                        
                        // Add all songs in this album if filter is "all" or "songs"
                        if filter == "all" || filter == "songs" {
                            if let Ok(songs) = std::fs::read_dir(album_entry.path()) {
                                for song_entry in songs.flatten() {
                                    if let Some(ext) = song_entry.path().extension() {
                                        if ext == "mp3" || ext == "flac" || ext == "wav" {
                                            let song_path = song_entry.path();
                                            let song_name = if let Ok(tags) = Tag::default().read_from_path(&song_path) {
                                                // Use the title from metadata if available
                                                tags.title().map(|t| t.to_string()).unwrap_or_else(|| {
                                                    // Fallback to filename without extension
                                                    song_entry.file_name()
                                                        .to_string_lossy()
                                                        .replace(&format!(".{}", ext.to_string_lossy()), "")
                                                        .to_owned()
                                                })
                                            } else {
                                                // Fallback to filename without extension if metadata reading fails
                                                song_entry.file_name()
                                                    .to_string_lossy()
                                                    .replace(&format!(".{}", ext.to_string_lossy()), "")
                                                    .to_owned()
                                            };
                                            
                                            results.push(SearchResult {
                                                title: song_name,
                                                artist: artist_name.clone(),
                                                album: album_name.clone(),
                                                cover_art_path: cover_art.clone(),
                                                file_extension: ext.to_string_lossy().into_owned(),
                                                filename: song_entry.file_name().to_string_lossy().into_owned(),
                                            });
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                // Search in albums and songs
                if let Ok(albums) = std::fs::read_dir(artist_entry.path()) {
                    for album_entry in albums.flatten() {
                        let album_name = album_entry.file_name().to_string_lossy().into_owned();
                        let mut cover_art = String::new();
                        
                        // Find cover art
                        if let Ok(files) = std::fs::read_dir(album_entry.path()) {
                            for file in files.flatten() {
                                if let Some(ext) = file.path().extension() {
                                    if ext == "jpg" || ext == "png" {
                                        cover_art = file.file_name().to_string_lossy().into_owned();
                                        break;
                                    }
                                }
                            }
                        }
                        
                        // Search in album name if filter is "all" or "albums"
                        if (filter == "all" || filter == "albums") && album_name.to_lowercase().contains(&query) {
                            // Add all songs in this album if filter is "all" or "songs"
                            if filter == "all" || filter == "songs" {
                                if let Ok(songs) = std::fs::read_dir(album_entry.path()) {
                                    for song_entry in songs.flatten() {
                                        if let Some(ext) = song_entry.path().extension() {
                                            if ext == "mp3" || ext == "flac" || ext == "wav" {
                                                let song_path = song_entry.path();
                                                let song_name = if let Ok(tags) = Tag::default().read_from_path(&song_path) {
                                                    // Use the title from metadata if available
                                                    tags.title().map(|t| t.to_string()).unwrap_or_else(|| {
                                                        // Fallback to filename without extension
                                                        song_entry.file_name()
                                                            .to_string_lossy()
                                                            .replace(&format!(".{}", ext.to_string_lossy()), "")
                                                            .to_owned()
                                                    })
                                                } else {
                                                    // Fallback to filename without extension if metadata reading fails
                                                    song_entry.file_name()
                                                        .to_string_lossy()
                                                        .replace(&format!(".{}", ext.to_string_lossy()), "")
                                                        .to_owned()
                                                };
                                                
                                                results.push(SearchResult {
                                                    title: song_name,
                                                    artist: artist_name.clone(),
                                                    album: album_name.clone(),
                                                    cover_art_path: cover_art.clone(),
                                                    file_extension: ext.to_string_lossy().into_owned(),
                                                    filename: song_entry.file_name().to_string_lossy().into_owned(),
                                                });
                                            }
                                        }
                                    }
                                }
                            }
                        } else if filter == "all" || filter == "songs" {
                            // Search in song names
                            if let Ok(songs) = std::fs::read_dir(album_entry.path()) {
                                for song_entry in songs.flatten() {
                                    if let Some(ext) = song_entry.path().extension() {
                                        if ext == "mp3" || ext == "flac" || ext == "wav" {
                                            let song_path = song_entry.path();
                                            let song_name = if let Ok(tags) = Tag::default().read_from_path(&song_path) {
                                                // Use the title from metadata if available
                                                tags.title().map(|t| t.to_string()).unwrap_or_else(|| {
                                                    // Fallback to filename without extension
                                                    song_entry.file_name()
                                                        .to_string_lossy()
                                                        .replace(&format!(".{}", ext.to_string_lossy()), "")
                                                        .to_owned()
                                                })
                                            } else {
                                                // Fallback to filename without extension if metadata reading fails
                                                song_entry.file_name()
                                                    .to_string_lossy()
                                                    .replace(&format!(".{}", ext.to_string_lossy()), "")
                                                    .to_owned()
                                            };
                                            
                                            if song_name.to_lowercase().contains(&query) {
                                                results.push(SearchResult {
                                                    title: song_name,
                                                    artist: artist_name.clone(),
                                                    album: album_name.clone(),
                                                    cover_art_path: cover_art.clone(),
                                                    file_extension: ext.to_string_lossy().into_owned(),
                                                    filename: song_entry.file_name().to_string_lossy().into_owned(),
                                                });
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // Limit results to 20 to prevent overwhelming the client
    results.truncate(20);
    Json(results)
}

async fn set_volume(
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    if let Some(volume_str) = params.get("volume") {
        if let Ok(volume) = volume_str.parse::<f32>() {
            if let Err(e) = state.audio_tx.send(AudioCommand::SetVolume(volume)).await {
                eprintln!("Failed to set volume: {}", e);
                return Html::<String>("Error setting volume".into());
            }
            return Html(format!("Volume set to {}", volume).into());
        }
    }
    Html::<String>("Invalid volume value".into())
}
