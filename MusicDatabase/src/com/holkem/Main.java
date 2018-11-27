package com.holkem;

import java.util.List;
import java.util.Scanner;

import com.holkem.model.Artist;
import com.holkem.model.Datasource;
import com.holkem.model.SongMeta;

public class Main {

	public static void main(String[] args) {
		Datasource datasource = new Datasource();
		if (!datasource.open()) {
			System.err.println("Error in opening database connection.");
			return;
		}
		
		List<Artist> artists = datasource.queryAllArtists(Datasource.ORDER_ASC);
		if (artists.isEmpty()) {
			System.out.println("There are no artist saved in the application!");
		} else {
			artists.stream().forEach(System.out::println);
		}
		
		List<String> albums = datasource.queryAlbumsByArtist("ron mai", Datasource.ORDER_DESC);
		if (albums.isEmpty()) {
			System.out.println("No album from that artist!");
		} else {
			albums.stream().forEach(System.out::println);
		}
		
		Scanner sc = new Scanner(System.in);
		System.out.print("Enter song title to get meta details: ");
		// test for non-secure: Go Your Own Way" or 1=1 or "
		
		String query = sc.nextLine();
		List<SongMeta> songs = datasource.secureQuerySongDetails(query, Datasource.ORDER_DESC);
		if (songs.isEmpty()) {
			System.out.println("No album from that artist!");
		} else {
			songs.stream().forEach(System.out::println);
		}
		
		sc.close();
		
		datasource.insertSong("zTitle", "zArtist", "zAlbum", 1);;
		datasource.close();
	}

}
