use axum::{Router, routing::get, response::Html};
use std::fs::File; 
use std::io::BufReader; 
use rodio::{Decoder, OutputStream, source::Source, Sink};


#[tokio::main]
async fn main() {
    let app = Router::new()
            .route("/", get(serve_index))
            .route("/play/song1", get(play_music));


    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();

    axum::serve(listener, app).await.unwrap();
}

async fn serve_index() -> Html<String> {
   Html(include_str!("../assets/index.html").to_string())
}

async fn play_music(){ 

        tokio::spawn(async move { 
            let(_stream, stream_handle) = OutputStream::try_default().unwrap();

            let sink = Sink::try_new(&stream_handle).unwrap(); 
            let file = BufReader::new(File::open("assets/song1.flac").unwrap());
            let source = Decoder::new(file).unwrap();

            sink.append(source); 

            sink.sleep_until_end();
        });

    


}



