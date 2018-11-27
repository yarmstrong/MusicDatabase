package com.holkem.model;

public class SongMeta extends Song {
	private String album;
	private String artist;

	public String getAlbum() {
		return album;
	}

	public void setAlbum(String album) {
		this.album = album;
	}

	public String getArtist() {
		return artist;
	}

	public void setArtist(String artist) {
		this.artist = artist;
	}

	@Override
	public String toString() {
		return "ARTIST: " + artist + 
			   ", ALBUM: " + album +
			   ", TRACK: " + getTrack();
	}
}
