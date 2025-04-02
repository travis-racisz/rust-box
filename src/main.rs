use axum::{
    Router,
    extract::{Path, State},
    http::StatusCode,
    response::Html,
    routing::get,
    Json,
};
use rodio::{Decoder, OutputStream, Sink, source::Source};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::{
    fs::{self, File},
    io::BufReader,
    path::{PathBuf, Path as FsPath},
    sync::{Arc, Mutex},
};
use tokio::sync::mpsc;

// New structs to represent the music library hierarchy
struct Song {
    title: String,
    path: String,
}

struct Album {
    title: String,
    songs: Vec<Song>,
    cover_path: Option<String>, // Path to album cover image if exists
}

struct Artist {
    name: String,
    albums: Vec<Album>,
}

struct AudioState {
    currently_playing: Mutex<Option<String>>,
    is_playing: AtomicBool,
    is_paused: AtomicBool,
    queue: Mutex<VecDeque<String>>, // Stores paths to songs
    library: Mutex<Vec<Artist>>,    // The music library
}

#[derive(Clone)]
struct AppState {
    tx: mpsc::Sender<AudioCommand>,
    audio_state: Arc<AudioState>,
}

#[tokio::main]
async fn main() {
    let audio_state = Arc::new(AudioState {
        currently_playing: Mutex::new(None),
        is_playing: AtomicBool::new(false),
        is_paused: AtomicBool::new(false),
        queue: Mutex::new(VecDeque::new()),
        library: Mutex::new(Vec::new()),
    });

    // Initialize the music library
    {
        let mut library = audio_state.library.lock().unwrap();
        *library = scan_music_library();
    }

    let (tx, rx) = mpsc::channel(32);
    let audio_state_clone = audio_state.clone();
    let app_state = AppState {
        tx,
        audio_state: audio_state.clone(),
    };
    
    // Set up the audio controller
    let (auto_tx, mut auto_rx) = mpsc::channel::<()>(1);
    let audio_state_for_auto = audio_state_clone.clone();
    let auto_tx_clone = auto_tx.clone(); // Clone before moving
    
    // Spawn a task to handle auto-playing from queue when a song finishes
    tokio::spawn(async move {
        while let Some(_) = auto_rx.recv().await {
            // Check if there's another song in the queue
            if let Some(next_song) = {
                let mut queue = audio_state_for_auto.queue.lock().unwrap();
                queue.pop_front()
            } {
                println!("Auto-playing next song from queue: {}", next_song);
                
                // Set the current playing song
                {
                    let mut current = audio_state_for_auto.currently_playing.lock().unwrap();
                    *current = Some(next_song.clone());
                }
                
                // Start playback of the next song
                let audio_state_clone = audio_state_for_auto.clone();
                let signal_tx = auto_tx_clone.clone(); // Use the clone instead of original
                let handle_thread = thread::spawn(move || {
                    play_song_sync(next_song, audio_state_clone);
                    // Signal that we're done with this song
                    let _ = tokio::runtime::Handle::current().block_on(signal_tx.send(()));
                });
                
                // Store the thread handle somewhere safe
                unsafe {
                    static mut CURRENT_HANDLE: Option<thread::JoinHandle<()>> = None;
                    CURRENT_HANDLE = Some(handle_thread);
                }
            }
        }
    });

    // Spawn main audio controller
    let mut handle: Option<thread::JoinHandle<()>> = None;
    tokio::spawn(async move {
        audio_controller(rx, audio_state_clone, handle, auto_tx).await;
    });

    let app = Router::new()
        .route("/", get(serve_index))
        .route("/play/:artist/:album/:song", get(play_song_handler))
        .route("/queue/:artist/:album/:song", get(queue_song_handler))
        .route("/queue_album/:artist/:album", get(queue_album_handler))
        .route("/stop", get(stop_song))
        .route("/pause", get(pause_song))
        .route("/resume", get(resume_song))
        .route("/skip", get(skip_song))
        .route("/status", get(get_status))
        .route("/library", get(get_library))
        .route("/artists", get(get_artists))
        .route("/albums/:artist", get(get_albums))
        .route("/songs/:artist/:album", get(get_songs))
        .route("/queue", get(get_queue))
        .route("/assets/*path", get(serve_static_files))  // Add route for serving static files like album covers
        .with_state(app_state);

    println!("Server starting at http://0.0.0.0:3000");
    println!("Access http://0.0.0.0:3000/ to view and control the music player");

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

// Sets up the audio controller and returns a clone of the auto_tx channel
async fn spawn_audio_controller(
    rx: mpsc::Receiver<AudioCommand>,
    audio_state: Arc<AudioState>,
) -> mpsc::Sender<()> {
    let mut handle: Option<thread::JoinHandle<()>> = None;
    let (auto_tx, mut auto_rx) = mpsc::channel::<()>(1);
    let audio_state_for_auto = audio_state.clone();
    let auto_tx_clone = auto_tx.clone(); // Clone before moving
    
    // Spawn a task to handle auto-playing from queue when a song finishes
    tokio::spawn(async move {
        while let Some(_) = auto_rx.recv().await {
            // Check if there's another song in the queue
            if let Some(next_song) = {
                let mut queue = audio_state_for_auto.queue.lock().unwrap();
                queue.pop_front()
            } {
                println!("Auto-playing next song from queue: {}", next_song);
                
                // Set the current playing song
                {
                    let mut current = audio_state_for_auto.currently_playing.lock().unwrap();
                    *current = Some(next_song.clone());
                }
                
                // Start playback of the next song
                let audio_state_clone = audio_state_for_auto.clone();
                let signal_tx = auto_tx_clone.clone();
                let handle_thread = thread::spawn(move || {
                    play_song_sync(next_song, audio_state_clone);
                    // Signal that we're done with this song
                    let _ = tokio::runtime::Handle::current().block_on(signal_tx.send(()));
                });
                
                // Store the thread handle somewhere safe
                unsafe {
                    static mut CURRENT_HANDLE: Option<thread::JoinHandle<()>> = None;
                    CURRENT_HANDLE = Some(handle_thread);
                }
            }
        }
    });

    // Spawn a task to handle audio commands
    tokio::spawn(async move {
        audio_controller(rx, audio_state, handle, auto_tx).await;
    });

    auto_tx_clone
}

// Scan the music library structured as artist/album/song
fn scan_music_library() -> Vec<Artist> {
    let mut artists = Vec::new();
    
    // Get the assets directory
    let mut assets_dir = std::env::current_dir().unwrap();
    assets_dir.push("assets");
    
    // Check if the assets directory exists
    if !assets_dir.exists() {
        println!("Warning: Assets directory not found. Creating it...");
        fs::create_dir_all(&assets_dir).unwrap_or_else(|e| {
            println!("Error creating assets directory: {}", e);
        });
        return artists;
    }
    
    // Read artist directories
    if let Ok(artist_entries) = fs::read_dir(&assets_dir) {
        for artist_entry in artist_entries.flatten() {
            let artist_path = artist_entry.path();
            
            // Skip non-directory entries in the artists folder
            if !artist_path.is_dir() {
                continue;
            }
            
            let artist_name = artist_path.file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("Unknown Artist")
                .to_string();
                
            println!("Found artist: {}", artist_name);
            
            let mut albums = Vec::new();
            
            // Read album directories for this artist
            if let Ok(album_entries) = fs::read_dir(&artist_path) {
                for album_entry in album_entries.flatten() {
                    let album_path = album_entry.path();
                    
                    // Skip non-directory entries in the album folder
                    if !album_path.is_dir() {
                        continue;
                    }
                    
                    let album_name = album_path.file_name()
                        .and_then(|name| name.to_str())
                        .unwrap_or("Unknown Album")
                        .to_string();
                        
                    println!("  Found album: {}", album_name);
                    
                    let mut songs = Vec::new();
                    let mut cover_path = None;
                    
                    // Read songs and cover art in this album
                    if let Ok(song_entries) = fs::read_dir(&album_path) {
                        for song_entry in song_entries.flatten() {
                            let song_path = song_entry.path();
                            
                            if song_path.is_file() {
                                // Check if this is a cover image
                                if let Some(extension) = song_path.extension().and_then(|ext| ext.to_str()) {
                                    match extension.to_lowercase().as_str() {
                                        "jpg" | "jpeg" | "png" if song_path.file_stem().and_then(|s| s.to_str()).map_or(false, |s| s.to_lowercase() == "cover") => {
                                            // Found a cover image
                                            let rel_path = song_path.strip_prefix(&assets_dir).unwrap_or(&song_path);
                                            cover_path = Some(rel_path.to_string_lossy().to_string());
                                            println!("    Found cover art: {}", rel_path.display());
                                        },
                                        "flac" | "mp3" | "wav" | "ogg" => {
                                            // Found a song
                                            let song_title = song_path.file_stem()
                                                .and_then(|name| name.to_str())
                                                .unwrap_or("Unknown Song")
                                                .to_string();
                                                
                                            // Create a relative path for the song
                                            let rel_path = song_path.strip_prefix(&assets_dir).unwrap_or(&song_path);
                                            
                                            println!("    Found song: {} at {}", song_title, rel_path.display());
                                            
                                            songs.push(Song {
                                                title: song_title,
                                                path: rel_path.to_string_lossy().to_string(),
                                            });
                                        },
                                        _ => {} // Ignore other file types
                                    }
                                }
                            }
                        }
                    }
                    
                    albums.push(Album {
                        title: album_name,
                        songs,
                        cover_path,
                    });
                }
            }
            
            artists.push(Artist {
                name: artist_name,
                albums,
            });
        }
    }
    
    artists
}

// API handlers for the music browser
async fn get_library(State(state): State<AppState>) -> Json<Vec<Artist>> {
    let library = state.audio_state.library.lock().unwrap().clone();
    Json(library)
}

async fn get_artists(State(state): State<AppState>) -> Json<Vec<String>> {
    let library = state.audio_state.library.lock().unwrap();
    let artist_names: Vec<String> = library.iter()
        .map(|artist| artist.name.clone())
        .collect();
    
    Json(artist_names)
}

async fn get_albums(
    Path(artist_name): Path<String>,
    State(state): State<AppState>,
) -> Json<Vec<Album>> {
    let library = state.audio_state.library.lock().unwrap();
    
    // Find the specified artist
    if let Some(artist) = library.iter().find(|a| a.name == artist_name) {
        Json(artist.albums.clone())
    } else {
        Json(Vec::new()) // Artist not found
    }
}

async fn get_songs(
    Path((artist_name, album_name)): Path<(String, String)>,
    State(state): State<AppState>,
) -> Json<Vec<Song>> {
    let library = state.audio_state.library.lock().unwrap();
    
    // Find the specified artist and album
    for artist in library.iter() {
        if artist.name == artist_name {
            if let Some(album) = artist.albums.iter().find(|a| a.title == album_name) {
                return Json(album.songs.clone());
            }
        }
    }
    
    Json(Vec::new()) // Artist or album not found
}

// Queue an entire album
async fn queue_album_handler(
    Path((artist_name, album_name)): Path<(String, String)>,
    State(state): State<AppState>,
) -> StatusCode {
    // Get all songs from the album
    let songs_to_queue = {
        let library = state.audio_state.library.lock().unwrap();
        
        let mut songs = Vec::new();
        
        // Find the artist and album
        for artist in library.iter() {
            if artist.name == artist_name {
                if let Some(album) = artist.albums.iter().find(|a| a.title == album_name) {
                    songs = album.songs.clone();
                    break;
                }
            }
        }
        
        songs
    };
    
    // Queue each song
    for song in songs_to_queue {
        if let Err(_) = state.tx.send(AudioCommand::Queue(song.path)).await {
            return StatusCode::INTERNAL_SERVER_ERROR;
        }
    }
    
    StatusCode::OK
}

// New handler to queue a song
async fn queue_song_handler(
    Path((artist, album, song)): Path<(String, String, String)>,
    State(state): State<AppState>,
) -> StatusCode {
    // Construct the path to the song
    let song_path = format!("{}/{}/{}", artist, album, song);
    
    if let Err(_) = state.tx.send(AudioCommand::Queue(song_path)).await {
        return StatusCode::INTERNAL_SERVER_ERROR;
    }
    StatusCode::OK
}

// New handler to get the current queue
async fn get_queue(State(state): State<AppState>) -> Json<Vec<String>> {
    let queue = {
        let guard = state.audio_state.queue.lock().unwrap();
        guard.iter().cloned().collect::<Vec<String>>()
    };
    Json(queue)
}

async fn get_status(
    State(state): State<AppState>,
) -> Json<HashMap<String, serde_json::Value>> {
    let mut status = HashMap::new();

    let is_playing = state.audio_state.is_playing.load(Ordering::SeqCst);
    let is_paused = state.audio_state.is_paused.load(Ordering::SeqCst);
    let currently_playing = {
        let guard = state.audio_state.currently_playing.lock().unwrap();
        guard.clone()
    };
    
    // Include queue length in status
    let queue_length = {
        let guard = state.audio_state.queue.lock().unwrap();
        guard.len()
    };

    status.insert("is_playing".to_string(), serde_json::Value::Bool(is_playing));
    status.insert("is_paused".to_string(), serde_json::Value::Bool(is_paused));
    status.insert(
        "currently_playing".to_string(),
        serde_json::Value::String(currently_playing.unwrap_or_else(|| "".to_string())),
    );
    status.insert(
        "queue_length".to_string(), 
        serde_json::Value::Number(serde_json::Number::from(queue_length)),
    );

    Json(status)
}

async fn pause_song(State(state): State<AppState>) -> StatusCode {
    if let Err(_) = state.tx.send(AudioCommand::Pause).await {
        return StatusCode::INTERNAL_SERVER_ERROR;
    }
    StatusCode::OK
}

async fn resume_song(State(state): State<AppState>) -> StatusCode {
    if let Err(_) = state.tx.send(AudioCommand::Resume).await {
        return StatusCode::INTERNAL_SERVER_ERROR;
    }
    StatusCode::OK
}

// New handler to skip the current song
async fn skip_song(State(state): State<AppState>) -> StatusCode {
    if let Err(_) = state.tx.send(AudioCommand::Skip).await {
        return StatusCode::INTERNAL_SERVER_ERROR;
    }
    StatusCode::OK
}

async fn stop_song(State(state): State<AppState>) -> StatusCode {
    if let Err(_) = state.tx.send(AudioCommand::Stop).await {
        return StatusCode::INTERNAL_SERVER_ERROR;
    }
    StatusCode::OK
}

async fn play_song_handler(
    Path((artist, album, song)): Path<(String, String, String)>,
    State(state): State<AppState>,
) -> StatusCode {
    // Construct the path to the song
    let song_path = format!("{}/{}/{}", artist, album, song);
    
    if let Err(_) = state.tx.send(AudioCommand::Play(song_path)).await {
        return StatusCode::INTERNAL_SERVER_ERROR;
    }
    StatusCode::OK
}

// Serve static files (for album covers)
async fn serve_static_files(Path(path): Path<String>) -> impl axum::response::IntoResponse {
    let mut file_path = PathBuf::from("assets");
    file_path.push(path);
    
    match tokio::fs::read(&file_path).await {
        Ok(data) => {
            let mime_type = match file_path.extension().and_then(|ext| ext.to_str()) {
                Some("jpg") | Some("jpeg") => "image/jpeg",
                Some("png") => "image/png",
                Some("gif") => "image/gif",
                _ => "application/octet-stream",
            };
            
            (
                [(axum::http::header::CONTENT_TYPE, mime_type)],
                data
            ).into_response()
        },
        Err(_) => {
            (
                axum::http::StatusCode::NOT_FOUND,
                "File not found".to_string()
            ).into_response()
        }
    }
}

async fn serve_index() -> Html<String> {
    Html(r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Rust Music Player</title>
    <style>
        :root {
            --primary-color: #2c3e50;
            --secondary-color: #3498db;
            --background-color: #f5f5f5;
            --card-color: white;
            --text-color: #333;
            --accent-color: #1abc9c;
            --hover-color: #e9e9e9;
            --playing-color: #d4edda;
            --playing-border: #28a745;
        }

        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
            background-color: var(--background-color);
            color: var(--text-color);
        }

        h1, h2, h3 {
            color: var(--primary-color);
        }

        h1 {
            text-align: center;
            margin-bottom: 30px;
        }

        .player-container {
            background-color: var(--card-color);
            border-radius: 8px;
            padding: 20px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            margin-bottom: 20px;
        }

        .library-container {
            display: flex;
            gap: 20px;
        }

        .artist-list, .album-list, .song-list {
            background-color: var(--card-color);
            border-radius: 8px;
            padding: 15px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            flex: 1;
            min-height: 300px;
            overflow-y: auto;
        }

        .list-title {
            margin-top: 0;
            padding-bottom: 10px;
            border-bottom: 1px solid #eee;
        }

        .artist-item, .album-item, .song-item {
            padding: 10px 15px;
            margin-bottom: 5px;
            background-color: #f9f9f9;
            border-radius: 4px;
            cursor: pointer;
            transition: all 0.2s ease;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }

        .artist-item:hover, .album-item:hover, .song-item:hover {
            background-color: var(--hover-color);
        }

        .artist-item.active, .album-item.active {
            background-color: var(--secondary-color);
            color: white;
        }

        .song-item.playing {
            background-color: var(--playing-color);
            border-left: 4px solid var(--playing-border);
        }

        .controls {
            display: flex;
            justify-content: center;
            margin: 20px 0;
        }

        .song-actions {
            display: flex;
            gap: 5px;
        }

        button {
            background-color: var(--primary-color);
            color: white;
            border: none;
            padding: 8px 16px;
            margin: 0 5px;
            border-radius: 4px;
            cursor: pointer;
            transition: background-color 0.2s;
        }

        button:hover {
            background-color: #1a252f;
        }

        button:disabled {
            background-color: #95a5a6;
            cursor: not-allowed;
        }

        .action-btn {
            background-color: #6c757d;
            font-size: 0.8em;
            padding: 5px 10px;
        }

        .queue-btn {
            background-color: var(--secondary-color);
        }

        .play-all-btn {
            background-color: var(--accent-color);
            display: block;
            margin: 10px auto;
            width: 100%;
            max-width: 200px;
        }

        .album-cover {
            width: 100%;
            max-width: 200px;
            height: auto;
            border-radius: 4px;
            display: block;
            margin: 0 auto 15px auto;
        }

        .status {
            text-align: center;
            font-style: italic;
            color: #6c757d;
            margin: 15px 0;
        }

        .queue-container {
            background-color: var(--card-color);
            border-radius: 8px;
            padding: 20px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            margin-top: 20px;
        }

        .queue-list {
            list-style: none;
            padding: 0;
        }

        .queue-item {
            padding: 8px 12px;
            margin-bottom: 5px;
            background-color: #e9ecef;
            border-radius: 4px;
        }

        .error {
            color: #dc3545;
            text-align: center;
            margin-top: 10px;
            display: none;
        }

        .song-title {
            flex: 1;
        }

        .breadcrumb {
            display: flex;
            margin-bottom: 15px;
            align-items: center;
        }

        .breadcrumb span {
            margin: 0 5px;
            color: #6c757d;
        }

        .back-btn {
            background-color: #6c757d;
            margin-right: 10px;
        }

        .album-info {
            display: flex;
            flex-direction: column;
            align-items: center;
            margin-bottom: 20px;
        }

        /* Mobile responsive design */
        @media (max-width: 768px) {
            .library-container {
                flex-direction: column;
            }
            
            .controls button {
                padding: 6px 12px;
                font-size: 0.9em;
            }
        }
    </style>
</head>
<body>
    <h1>Rust Music Player</h1>
    
    <div class="player-container">
        <div class="controls">
            <button id="stopButton">Stop</button>
            <button id="pauseResumeButton">Pause</button>
            <button id="skipButton">Skip</button>
        </div>
        
        <div class="status" id="status">Ready to play</div>
    </div>
    
    <div class="breadcrumb" id="breadcrumb">
        <h2>Music Library</h2>
    </div>
    
    <div class="library-container">
        <div class="artist-list" id="artistList">
            <h3 class="list-title">Artists</h3>
            <div class="list-content">Loading artists...</div>
        </div>
        
        <div class="album-list" id="albumList" style="display: none;">
            <h3 class="list-title">Albums</h3>
            <div class="list-content">Select an artist to view albums</div>
        </div>
        
        <div class="song-list" id="songList" style="display: none;">
            <h3 class="list-title">Songs</h3>
            <div class="album-info" id="albumInfo"></div>
            <button id="playAllButton" class="play-all-btn">Queue All Songs</button>
            <div class="list-content">Select an album to view songs</div>
        </div>
    </div>
    
    <div class="queue-container">
        <h2>Queue <span id="queueCount">(0)</span></h2>
        <ul class="queue-list" id="queueList">
            <li>No songs in queue</li>
        </ul>
    </div>
    
    <div class="error" id="errorMessage"></div>
    
    <script>
        document.addEventListener('DOMContentLoaded', function() {
            // DOM elements
            const artistList = document.getElementById('artistList');
            const albumList = document.getElementById('albumList');
            const songList = document.getElementById('songList');
            const albumInfo = document.getElementById('albumInfo');
            const queueList = document.getElementById('queueList');
            const queueCount = document.getElementById('queueCount');
            const statusElement = document.getElementById('status');
            const stopButton = document.getElementById('stopButton');
            const skipButton = document.getElementById('skipButton');
            const pauseResumeButton = document.getElementById('pauseResumeButton');
            const playAllButton = document.getElementById('playAllButton');
            const errorMessage = document.getElementById('errorMessage');
            const breadcrumb = document.getElementById('breadcrumb');
            
            // State
            let currentArtist = '';
            let currentAlbum = '';
            
            // Load artists when the page loads
            loadArtists();
            
            // Event listeners for controls
            pauseResumeButton.addEventListener('click', function() {
                const isPause = pauseResumeButton.textContent === 'Pause';
                const endpoint = isPause ? '/pause' : '/resume';
                
                fetch(endpoint)
                    .then(response => {
                        if (!response.ok) {
                            throw new Error(`Failed to ${isPause ? 'pause' : 'resume'} playback`);
                        }
                        updatePlayerStatus();
                    })
                    .catch(error => {
                        console.error(`Error ${isPause ? 'pausing' : 'resuming'} playback:`, error);
                        showError(`Failed to ${isPause ? 'pause' : 'resume'} playback. Please try again.`);
                    });
            });
            
            // Stop current playback
            stopButton.addEventListener('click', function() {
                fetch('/stop')
                    .then(response => {
                        if (!response.ok) {
                            throw new Error('Failed to stop playback');
                        }
                        updatePlayerStatus();
                    })
                    .catch(error => {
                        console.error('Error stopping playback:', error);
                        showError('Failed to stop playback. Please try again.');
                    });
            });
            
            // Skip current song
            skipButton.addEventListener('click', function() {
                fetch('/skip')
                    .then(response => {
                        if (!response.ok) {
                            throw new Error('Failed to skip song');
                        }
                        updatePlayerStatus();
                        updateQueueList();
                    })
                    .catch(error => {
                        console.error('Error skipping song:', error);
                        showError('Failed to skip song. Please try again.');
                    });
            });
            
            // Play all songs in the current album
            playAllButton.addEventListener('click', function() {
                if (currentArtist && currentAlbum) {
                    queueAlbum(currentArtist, currentAlbum);
                }
            });
            
            // Functions to load data
            function loadArtists() {
                fetch('/artists')
                    .then(response => response.json())
                    .then(artists => {
                        if (artists.length === 0) {
                            artistList.querySelector('.list-content').innerHTML = 'No artists found. Add some music to the assets folder.';
                            return;
                        }
                        
                        const listContent = artistList.querySelector('.list-content');
                        listContent.innerHTML = '';
                        
                        artists.forEach(artist => {
                            const artistElement = document.createElement('div');
                            artistElement.className = 'artist-item';
                            artistElement.textContent = artist;
                            artistElement.addEventListener('click', () => selectArtist(artist));
                            listContent.appendChild(artistElement);
                        });
                    })
                    .catch(error => {
                        console.error('Error loading artists:', error);
                        showError('Failed to load artists. Please refresh the page.');
                    });
            }
            
            function loadAlbums(artist) {
                albumList.style.display = 'block';
                songList.style.display = 'none';
                
                fetch(`/albums/${encodeURIComponent(artist)}`)
                    .then(response => response.json())
                    .then(albums => {
                        if (albums.length === 0) {
                            albumList.querySelector('.list-content').innerHTML = 'No albums found for this artist.';
                            return;
                        }
                        
                        const listContent = albumList.querySelector('.list-content');
                        listContent.innerHTML = '';
                        
                        albums.forEach(album => {
                            const albumElement = document.createElement('div');
                            albumElement.className = 'album-item';
                            albumElement.textContent = album.title;
                            albumElement.addEventListener('click', () => selectAlbum(artist, album));
                            listContent.appendChild(albumElement);
                        });
                    })
                    .catch(error => {
                        console.error('Error loading albums:', error);
                        showError('Failed to load albums. Please try again.');
                    });
            }
            
            function loadSongs(artist, album) {
                songList.style.display = 'block';
                
                fetch(`/songs/${encodeURIComponent(artist)}/${encodeURIComponent(album.title)}`)
                    .then(response => response.json())
                    .then(songs => {
                        // Update album info section
                        albumInfo.innerHTML = '';
                        
                        if (album.cover_path) {
                            const coverImg = document.createElement('img');
                            coverImg.className = 'album-cover';
                            coverImg.src = `/assets/${album.cover_path}`;
                            coverImg.alt = `${album.title} cover`;
                            albumInfo.appendChild(coverImg);
                        }
                        
                        const albumTitle = document.createElement('h3');
                        albumTitle.textContent = album.title;
                        albumInfo.appendChild(albumTitle);
                        
                        // Update song list
                        if (songs.length === 0) {
                            songList.querySelector('.list-content').innerHTML = 'No songs found in this album.';
                            playAllButton.style.display = 'none';
                            return;
                        }
                        
                        playAllButton.style.display = 'block';
                        
                        const listContent = songList.querySelector('.list-content');
                        listContent.innerHTML = '';
                        
                        songs.forEach(song => {
                            const songItem = document.createElement('div');
                            songItem.className = 'song-item';
                            
                            const songTitle = document.createElement('div');
                            songTitle.className = 'song-title';
                            songTitle.textContent = song.title;
                            songItem.appendChild(songTitle);
                            
                            const actions = document.createElement('div');
                            actions.className = 'song-actions';
                            
                            const playBtn = document.createElement('button');
                            playBtn.className = 'action-btn play-btn';
                            playBtn.textContent = 'Play';
                            playBtn.addEventListener('click', (e) => {
                                e.stopPropagation();
                                playSong(artist, album.title, song.path.split('/').pop());
                            });
                            
                            const queueBtn = document.createElement('button');
                            queueBtn.className = 'action-btn queue-btn';
                            queueBtn.textContent = 'Queue';
                            queueBtn.addEventListener('click', (e) => {
                                e.stopPropagation();
                                queueSong(artist, album.title, song.path.split('/').pop());
                            });
                            
                            actions.appendChild(playBtn);
                            actions.appendChild(queueBtn);
                            songItem.appendChild(actions);
                            
                            listContent.appendChild(songItem);
                        });
                    })
                    .catch(error => {
                        console.error('Error loading songs:', error);
                        showError('Failed to load songs. Please try again.');
                    });
            }
            
            // Navigation functions
            function selectArtist(artist) {
                // Highlight selected artist
                const artistItems = artistList.querySelectorAll('.artist-item');
                artistItems.forEach(item => {
                    if (item.textContent === artist) {
                        item.classList.add('active');
                    } else {
                        item.classList.remove('active');
                    }
                });
                
                currentArtist = artist;
                currentAlbum = '';
                
                // Update breadcrumb
                updateBreadcrumb();
                
                // Load albums for this artist
                loadAlbums(artist);
            }
            
            function selectAlbum(artist, album) {
                // Highlight selected album
                const albumItems = albumList.querySelectorAll('.album-item');
                albumItems.forEach(item => {
                    if (item.textContent === album.title) {
                        item.classList.add('active');
                    } else {
                        item.classList.remove('active');
                    }
                });
                
                currentAlbum = album.title;
                
                // Update breadcrumb
                updateBreadcrumb();
                
                // Load songs for this album
                loadSongs(artist, album);
            }
            
            function updateBreadcrumb() {
                let html = '<h2>Music Library</h2>';
                
                if (currentArtist) {
                    html += `<span>›</span><a href="#" id="artistBreadcrumb">${currentArtist}</a>`;
                    
                    if (currentAlbum) {
                        html += `<span>›</span><a href="#" id="albumBreadcrumb">${currentAlbum}</a>`;
                    }
                }
                
                breadcrumb.innerHTML = html;
                
                // Add event listeners to breadcrumb links
                if (currentArtist) {
                    document.getElementById('artistBreadcrumb').addEventListener('click', (e) => {
                        e.preventDefault();
                        if (currentAlbum) {
                            // If we're viewing songs, go back to album list
                            loadAlbums(currentArtist);
                            currentAlbum = '';
                            updateBreadcrumb();
                        } else {
                            // If we're viewing albums, go back to artist list
                            albumList.style.display = 'none';
                            currentArtist = '';
                            updateBreadcrumb();
                        }
                    });
                }
                
                if (currentAlbum) {
                    document.getElementById('albumBreadcrumb').addEventListener('click', (e) => {
                        e.preventDefault();
                        // No action needed, we're already viewing the album
                    });
                }
            }
            
            // Playback functions
            function playSong(artist, album, song) {
                fetch(`/play/${encodeURIComponent(artist)}/${encodeURIComponent(album)}/${encodeURIComponent(song)}`)
                    .then(response => {
                        if (!response.ok) {
                            throw new Error('Failed to play song');
                        }
                        updatePlayerStatus();
                    })
                    .catch(error => {
                        console.error('Error playing song:', error);
                        showError('Failed to play the song. Please try again.');
                    });
            }
            
            function queueSong(artist, album, song) {
                fetch(`/queue/${encodeURIComponent(artist)}/${encodeURIComponent(album)}/${encodeURIComponent(song)}`)
                    .then(response => {
                        if (!response.ok) {
                            throw new Error('Failed to queue song');
                        }
                        updateQueueList();
                        updatePlayerStatus();
                    })
                    .catch(error => {
                        console.error('Error queuing song:', error);
                        showError('Failed to add song to queue. Please try again.');
                    });
            }
            
            function queueAlbum(artist, album) {
                fetch(`/queue_album/${encodeURIComponent(artist)}/${encodeURIComponent(album)}`)
                    .then(response => {
                        if (!response.ok) {
                            throw new Error('Failed to queue album');
                        }
                        updateQueueList();
                        updatePlayerStatus();
                    })
                    .catch(error => {
                        console.error('Error queuing album:', error);
                        showError('Failed to add album to queue. Please try again.');
                    });
            }
            
            // Update queue list
            function updateQueueList() {
                fetch('/queue')
                    .then(response => response.json())
                    .then(queue => {
                        queueCount.textContent = `(${queue.length})`;
                        
                        if (queue.length === 0) {
                            queueList.innerHTML = '<li>No songs in queue</li>';
                            return;
                        }
                        
                        queueList.innerHTML = '';
                        queue.forEach((song, index) => {
                            const li = document.createElement('li');
                            li.className = 'queue-item';
                            
                            // Extract just the song name from the path
                            const songParts = song.split('/');
                            const songName = songParts[songParts.length - 1];
                            const albumName = songParts.length > 1 ? songParts[songParts.length - 2] : '';
                            const artistName = songParts.length > 2 ? songParts[songParts.length - 3] : '';
                            
                            li.textContent = `${index + 1}. ${artistName} - ${albumName} - ${songName}`;
                            queueList.appendChild(li);
                        });
                    })
                    .catch(error => {
                        console.error('Error updating queue:', error);
                    });
            }
            
            // Update player status
            function updatePlayerStatus() {
                fetch('/status')
                    .then(response => response.json())
                    .then(data => {
                        if (data.is_playing) {
                            // Update pause/resume button state
                            if (data.is_paused) {
                                statusElement.textContent = `Paused: ${formatSongPath(data.currently_playing)}`;
                                pauseResumeButton.textContent = 'Resume';
                            } else {
                                statusElement.textContent = `Currently playing: ${formatSongPath(data.currently_playing)}`;
                                pauseResumeButton.textContent = 'Pause';
                            }
                            
                            // Show queue length if available
                            if (data.queue_length !== undefined) {
                                statusElement.textContent += ` | Queue: ${data.queue_length} song(s)`;
                            }
                            
                            // Enable buttons
                            pauseResumeButton.disabled = false;
                            stopButton.disabled = false;
                            skipButton.disabled = false;
                            
                            // Highlight currently playing song if visible
                            highlightCurrentlyPlaying(data.currently_playing);
                        } else {
                            // Nothing playing
                            let statusText = 'Ready to play';
                            
                            // Show queue length if available
                            if (data.queue_length > 0) {
                                statusText += ` | Queue: ${data.queue_length} song(s)`;
                            }
                            
                            statusElement.textContent = statusText;
                            pauseResumeButton.disabled = true;
                            stopButton.disabled = true;
                            skipButton.disabled = true;
                            pauseResumeButton.textContent = 'Pause';
                            
                            // Remove highlighting from all songs
                            document.querySelectorAll('.song-item').forEach(item => {
                                item.classList.remove('playing');
                            });
                        }
                    })
                    .catch(error => console.error('Error updating status:', error));
            }
            
            function formatSongPath(path) {
                if (!path) return '';
                
                const parts = path.split('/');
                if (parts.length >= 3) {
                    return `${parts[0]} - ${parts[1]} - ${parts[2]}`;
                }
                return path;
            }
            
            function highlightCurrentlyPlaying(currentPath) {
                if (!currentPath) return;
                
                const parts = currentPath.split('/');
                if (parts.length < 3) return;
                
                const artist = parts[0];
                const album = parts[1];
                const song = parts[2];
                
                // Only highlight if we're viewing the correct album
                if (currentArtist === artist && currentAlbum === album) {
                    document.querySelectorAll('.song-item').forEach(item => {
                        const songTitle = item.querySelector('.song-title').textContent;
                        const songFilename = song.split('.')[0]; // Remove extension
                        
                        if (songTitle === songFilename) {
                            item.classList.add('playing');
                        } else {
                            item.classList.remove('playing');
                        }
                    });
                }
            }
            
            function showError(message) {
                errorMessage.textContent = message;
                errorMessage.style.display = 'block';
                
                // Hide after 5 seconds
                setTimeout(() => {
                    errorMessage.style.display = 'none';
                }, 5000);
            }
            
            // Initial status update and queue list
            updatePlayerStatus();
            updateQueueList();
            
            // Poll for status updates and queue updates
            setInterval(() => {
                updatePlayerStatus();
                updateQueueList();
            }, 2000);
        });
    </script>
</body>
</html>
"#.to_string())
}

enum AudioCommand {
    Play(String),
    Queue(String),
    Pause,
    Resume,
    Stop,
    Skip,
}

async fn audio_controller(
    mut rx: mpsc::Receiver<AudioCommand>,
    audio_state: Arc<AudioState>,
    mut handle: Option<thread::JoinHandle<()>>,
    auto_tx: mpsc::Sender<()>,
) {
    while let Some(cmd) = rx.recv().await {
        match cmd {
            AudioCommand::Play(song) => {
                // If there's already a thread playing, stop it first
                if let Some(h) = handle.take() {
                    audio_state.is_playing.store(false, Ordering::SeqCst);
                    let _ = h.join();
                }

                // Start a new playback thread
                {
                    let mut current = audio_state.currently_playing.lock().unwrap();
                    *current = Some(song.clone());
                }

                let song_clone = song.clone();
                let audio_state_clone = audio_state.clone();
                let auto_tx_clone = auto_tx.clone();
                handle = Some(thread::spawn(move || {
                    play_song_sync(song_clone, audio_state_clone);
                    // Signal that we're done with this song
                    let _ = tokio::runtime::Handle::current().block_on(auto_tx_clone.send(()));
                }));
            }
            AudioCommand::Queue(song) => {
                // Add song to the queue
                {
                    let mut queue = audio_state.queue.lock().unwrap();
                    queue.push_back(song.clone());
                    println!("Added {} to queue. Queue size: {}", song, queue.len());
                }

                // If nothing is playing, start playing from the queue
                if !audio_state.is_playing.load(Ordering::SeqCst) {
                    if let Some(next_song) = {
                        let mut queue = audio_state.queue.lock().unwrap();
                        queue.pop_front()
                    } {
                        // Set the current playing song
                        {
                            let mut current = audio_state.currently_playing.lock().unwrap();
                            *current = Some(next_song.clone());
                        }

                        // Start playback
                        let audio_state_clone = audio_state.clone();
                        let auto_tx_clone = auto_tx.clone();
                        handle = Some(thread::spawn(move || {
                            play_song_sync(next_song, audio_state_clone);
                            // Signal that we're done with this song
                            let _ = tokio::runtime::Handle::current().block_on(auto_tx_clone.send(()));
                        }));
                    }
                }
            }
            AudioCommand::Stop => {
                // Stop the current playback
                if let Some(h) = handle.take() {
                    audio_state.is_playing.store(false, Ordering::SeqCst);

                    let mut current = audio_state.currently_playing.lock().unwrap();
                    *current = None;
                    let _ = h.join();
                }
            }
            AudioCommand::Skip => {
                // Skip current song and play the next one from queue
                println!("Skipping current song");
                
                // Stop the current playback
                if let Some(h) = handle.take() {
                    audio_state.is_playing.store(false, Ordering::SeqCst);
                    let _ = h.join();
                }
                
                // Check if there's another song in the queue
                if let Some(next_song) = {
                    let mut queue = audio_state.queue.lock().unwrap();
                    queue.pop_front()
                } {
                    println!("Playing next song from queue: {}", next_song);
                    
                    // Set the current playing song
                    {
                        let mut current = audio_state.currently_playing.lock().unwrap();
                        *current = Some(next_song.clone());
                    }
                    
                    // Start playback of the next song
                    let audio_state_clone = audio_state.clone();
                    let auto_tx_clone = auto_tx.clone();
                    handle = Some(thread::spawn(move || {
                        play_song_sync(next_song, audio_state_clone);
                        // Signal that we're done with this song
                        let _ = tokio::runtime::Handle::current().block_on(auto_tx_clone.send(()));
                    }));
                } else {
                    // No more songs in queue
                    println!("No more songs in queue");
                    let mut current = audio_state.currently_playing.lock().unwrap();
                    *current = None;
                }
            }
            AudioCommand::Pause => {
                if audio_state.is_playing.load(Ordering::SeqCst)
                    && !audio_state.is_paused.load(Ordering::SeqCst)
                {
                    audio_state.is_paused.store(true, Ordering::SeqCst);
                    println!("Playback Paused")
                }
            }
            AudioCommand::Resume => {
                if audio_state.is_paused.load(Ordering::SeqCst) {
                    audio_state.is_paused.store(false, Ordering::SeqCst);
                    println!("Playback Resumed")
                }
            }
        }
    }
}

fn play_song_sync(song: String, audio_state: Arc<AudioState>) {
    // Set playing state
    audio_state.is_playing.store(true, Ordering::SeqCst);
    audio_state.is_paused.store(false, Ordering::SeqCst);

    // Build the path to the song file
    let mut path = PathBuf::from(std::env::current_dir().unwrap());
    path.push("assets");
    path.push(&song);

    println!("Attempting to play: {}", path.display());

    match File::open(&path) {
        Ok(file) => {
            let buf_reader = BufReader::new(file);
            match Decoder::new(buf_reader) {
                Ok(source) => {
                    match OutputStream::try_default() {
                        Ok((_stream, stream_handle)) => {
                            // Create a sink for playback control
                            let sink = Sink::try_new(&stream_handle).unwrap();
                            
                            // Append the source to the sink
                            sink.append(source);
                            
                            println!("Playing {}", song);
                            
                            // Main playback loop
                            while !sink.empty() && audio_state.is_playing.load(Ordering::SeqCst) {
                                // Check if we should pause
                                if audio_state.is_paused.load(Ordering::SeqCst) {
                                    if !sink.is_paused() {
                                        sink.pause();
                                    }
                                } else {
                                    if sink.is_paused() {
                                        sink.play();
                                    }
                                }
                                
                                // Short sleep to prevent CPU hogging
                                std::thread::sleep(std::time::Duration::from_millis(100));
                            }
                            
                            // Make sure to stop and drop the sink
                            sink.stop();
                            println!("Finished playing {}", song);
                        }
                        Err(e) => println!("Error creating audio stream: {}", e),
                    }
                }
                Err(e) => println!("Failed to decode audio file: {}", e),
            }
        }
        Err(e) => println!("Failed to open file '{}': {}", path.display(), e),
    }

    // Reset playing state
    audio_state.is_playing.store(false, Ordering::SeqCst);
    audio_state.is_paused.store(false, Ordering::SeqCst);
}
