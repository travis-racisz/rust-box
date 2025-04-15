# Rust Box 🎶

A modern web-based music player built with Rust (backend) and vanilla JavaScript (frontend), featuring a sleek, glass-inspired UI.

![RustBox](https://github.com/user-attachments/assets/9f4e5230-5e8d-47fb-8fa8-edc625f298df)

## Features

- 👨‍👨‍👦‍👦 Multiple users can connect concurrently
- 🎵 Play local music files (MP3, FLAC, WAV)
- 🎧 Audio playback controls (play, pause, stop, next)
- 📚 Browse your music library by artist and album
- 📋 Queue management for continuous playback
- 📱 Responsive design that works on desktop and mobile devices
- 🔄 Real-time updates via Server-Sent Events (SSE)
- 🎨 Modern glass UI with Catppuccin-inspired theming

## Technology Stack

- **Backend**: Rust with Axum web framework
- **Audio Engine**: Rodio library for audio playback
- **Frontend**: Vanilla JavaScript with modern CSS
- **Real-time Updates**: Server-Sent Events (SSE)
- **Design**: Glass morphism UI with responsive design

## Project Structure

```
/
├── assets/
│   ├── index.html     # Main web interface
│   ├── music/         # Directory for music files
│   │   ├── Artist1/   
│   │   │   ├── Album1/
│   │   │   │   ├── song1.mp3
│   │   │   │   ├── song2.mp3
│   │   │   │   └── cover.jpg
│   │   │   └── Album2/
│   │   └── Artist2/
│   └── ...
└── src/
    └── main.rs        # Rust backend code
```

## Getting Started

### Prerequisites

- Rust (stable version) and Cargo
- Local music collection organized by Artist/Album folders

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/travis-racisz/rust-box
   cd rust-box
   ```

2. Build the project:
   ```bash
   cargo build --release
   ```

> [!IMPORTANT]
> 3. Prepare your music library:
>   - Create `assets/music` directory if it doesn't exist
>   - Organize your music as `assets/music/ArtistName/AlbumName/songs`
>   - Optionally add cover art as JPG/PNG in each album folder

4. Run the application:
   ```bash
   cargo run --release
   ```

5. Open your browser and navigate to:
   ```
   http://localhost:3000
   ```

## Usage

1. **Browse Library**: Navigate through your music collection by artist and album
2. **Play Music**: Click on a song to start playback
3. **Queue Management**: Songs you play are added to the queue
4. **Playback Controls**: Use the buttons to control playback (play/pause, stop, next)
5. **Mobile View**: On smaller screens, the queue can be toggled to save space

## API Endpoints

The backend exposes the following API endpoints:

- `GET /` - Serves the web interface
- `GET /events` - SSE endpoint for real-time updates
- `GET /play/{artist}/{album}/{song}` - Add song to queue
- `POST /add_to_queue/{artist}/{album}/{song}` - Add song to queue (Deprecated)
- `GET /play_queue_index/{index}` - Play a specific song from the queue
- `GET /next` - Skip to next song
- `GET /previous` - Go to previous song (Deprecated)
- `GET /stop` - Stop playback
- `GET /pause` - Toggle pause/play
- `GET /queue` - Get current queue status
- `GET /get_library` - Get a list of artists in the library
- `GET /{artist}` - Get albums for a specific artist
- `GET /{artist}/{album}` - Get songs in a specific album

## Architecture

### Backend

The Rust backend utilizes the Axum web framework and is structured around a central `AppState` that manages:

- Audio playback via Rodio
- Song queue management
- Current playback status
- Real-time updates via SSE

The application uses Tokio for asynchronous operations and manages audio playback in a separate thread to ensure smooth performance.

### Frontend

The frontend is built with vanilla JavaScript and modern CSS features:

- Glass morphism UI with backdrop filters
- Responsive design using CSS Grid and Flexbox
- Real-time updates via EventSource API
- Local storage for user preferences

## Not Implemented (Yet)

- Queue management could be improved with drag-and-drop reordering - would be cool, maybe
- No volume control in the current version - working on
- Limited audio format support (depends on Rodio capabilities)
- Add search functionality - working on 
- Implement playlist support 
- Add audio visualizations
- Support for online streaming sources - wishlist 
- Dark/light theme toggle

## License

[MIT License](LICENSE)

## Acknowledgements

- [Rust](https://www.rust-lang.org/)
- [Axum](https://github.com/tokio-rs/axum)
- [Rodio](https://github.com/RustAudio/rodio)
- [Catppuccin](https://github.com/catppuccin/catppuccin) (for color inspiration)
