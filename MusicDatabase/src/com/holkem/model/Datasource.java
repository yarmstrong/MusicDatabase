package com.holkem.model;

import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class Datasource {
	private static final String DB_NAME = "music.db";
	private static final String CONNECTION_STRING = "jdbc:sqlite:" + 
			Paths.get(".", DB_NAME).normalize().toAbsolutePath();
	
	// database tables
	public static final String TABLE_ALBUMS = "albums";
    public static final String COLUMN_ALBUM_ID = "_id";
    public static final String COLUMN_ALBUM_NAME = "name";
    public static final String COLUMN_ALBUM_ARTIST = "artist";
    public static final int INDEX_ALBUM_ID = 1;
    public static final int INDEX_ALBUM_NAME = 2;
    public static final int INDEX_ALBUM_ARTIST = 3;

    public static final String TABLE_ARTISTS = "artists";
    public static final String COLUMN_ARTIST_ID = "_id";
    public static final String COLUMN_ARTIST_NAME = "name";
    public static final int INDEX_ARTIST_ID = 1;
    public static final int INDEX_ARTIST_NAME = 2;

    public static final String TABLE_SONGS = "songs";
    public static final String COLUMN_SONG_ID = "_id";
    public static final String COLUMN_SONG_TRACK = "track";
    public static final String COLUMN_SONG_TITLE = "title";
    public static final String COLUMN_SONG_ALBUM = "album";
    public static final int INDEX_SONG_ID = 1;
    public static final int INDEX_SONG_TRACK = 2;
    public static final int INDEX_SONG_TITLE = 3;
    public static final int INDEX_SONG_ALBUM = 4;
	
	public static final int ORDER_NONE = 0;
	public static final int ORDER_DESC = 2;
	public static final int ORDER_ASC = 1;
	
	// create view
	public static final String TABLE_SONG_ALBUM_ARTIST_VIEW = "artist_list";
	public static final String CREATE_SONG_ALBUM_ARTIST_VIEW;
	
	// sql statements
	private static final String QUERY_SONG_META;
	private static final String INSERT_ARTIST;
	private static final String INSERT_ALBUMS;
	private static final String INSERT_SONGS;
	private static final String QUERY_ARTIST;
	private static final String QUERY_ALBUM;
	
	// prepared statements
	private PreparedStatement querySongMetaAsc;
	private PreparedStatement querySongMetaDesc;
	private PreparedStatement insertIntoArtists;
    private PreparedStatement insertIntoAlbums;
    private PreparedStatement insertIntoSongs;
    private PreparedStatement queryArtist;
    private PreparedStatement queryAlbum;
    
	// connection
	private Connection conn;
	
	static {
		// view should be created before the initializing PreparedStatement objects referring to this table
		CREATE_SONG_ALBUM_ARTIST_VIEW = "CREATE VIEW IF NOT EXISTS " + TABLE_SONG_ALBUM_ARTIST_VIEW + " AS SELECT " + 
				TABLE_ARTISTS + "." + COLUMN_ARTIST_NAME + ", " +
	            TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME + " AS " + COLUMN_SONG_ALBUM + ", " +
	            TABLE_SONGS + "." + COLUMN_SONG_TRACK + ", " + TABLE_SONGS + "." + COLUMN_SONG_TITLE +
	            " FROM " + TABLE_SONGS +
	            " INNER JOIN " + TABLE_ALBUMS + " ON " + TABLE_SONGS +
	            "." + COLUMN_SONG_ALBUM + " = " + TABLE_ALBUMS + "." + COLUMN_ALBUM_ID +
	            " INNER JOIN " + TABLE_ARTISTS + " ON " + TABLE_ALBUMS + "." + COLUMN_ALBUM_ARTIST +
	            " = " + TABLE_ARTISTS + "." + COLUMN_ARTIST_ID +
	            " ORDER BY " +
	            TABLE_ARTISTS + "." + COLUMN_ARTIST_NAME + ", " +
	            TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME + ", " +
	            TABLE_SONGS + "." + COLUMN_SONG_TRACK;
		QUERY_SONG_META = "SELECT " + COLUMN_ARTIST_NAME + ", " +
	            COLUMN_SONG_ALBUM + ", " + COLUMN_SONG_TRACK + " FROM " + TABLE_SONG_ALBUM_ARTIST_VIEW +
	            " WHERE " + COLUMN_SONG_TITLE + " = ?" + " ORDER BY " + COLUMN_ARTIST_NAME + ", " +
	            COLUMN_SONG_ALBUM;
		INSERT_ARTIST = "INSERT INTO " + TABLE_ARTISTS +
	            '(' + COLUMN_ARTIST_NAME + ") VALUES(?)";
	    INSERT_ALBUMS = "INSERT INTO " + TABLE_ALBUMS +
	            '(' + COLUMN_ALBUM_NAME + ", " + COLUMN_ALBUM_ARTIST + ") VALUES(?, ?)";
	    INSERT_SONGS = "INSERT INTO " + TABLE_SONGS +
	            '(' + COLUMN_SONG_TRACK + ", " + COLUMN_SONG_TITLE + ", " + COLUMN_SONG_ALBUM +
	            ") VALUES(?, ?, ?)";
	    QUERY_ARTIST = "SELECT " + COLUMN_ARTIST_ID + " FROM " +
	            TABLE_ARTISTS + " WHERE " + COLUMN_ARTIST_NAME + " = ?";
	    QUERY_ALBUM = "SELECT " + COLUMN_ALBUM_ID + " FROM " +
	            TABLE_ALBUMS + " WHERE " + COLUMN_ALBUM_NAME + " = ?";
	}
	
	public boolean open() {		
		try {
			conn = DriverManager.getConnection(CONNECTION_STRING);
			initializePreparedStatements();
			return true;
		} catch (SQLException e) {
			System.err.println("Error: Couldn't open connection. " + e.getMessage());
			e.printStackTrace();
			return false;
		}
	}
	
	public void close() {
		try {
			if (querySongMetaAsc != null) querySongMetaAsc.close();
			if (querySongMetaDesc != null) querySongMetaDesc.close();
			if (insertIntoArtists != null) insertIntoArtists.close();
			if (insertIntoAlbums != null) insertIntoAlbums.close();
			if (insertIntoSongs != null) insertIntoSongs.close();
			if (queryArtist != null) queryArtist.close();
			if (queryAlbum != null) queryAlbum.close();
			if (conn != null) conn.close();
		} catch (SQLException e) {
			System.err.println("Error: Couldn't close connection. " + e.getMessage());
			e.printStackTrace();
		}
	}
	
	private void initializePreparedStatements() throws SQLException {
		querySongMetaAsc = conn.prepareStatement(QUERY_SONG_META + " ASC");
		querySongMetaDesc = conn.prepareStatement(QUERY_SONG_META + " DESC");
		insertIntoArtists = conn.prepareStatement(INSERT_ARTIST, Statement.RETURN_GENERATED_KEYS);
        insertIntoAlbums = conn.prepareStatement(INSERT_ALBUMS, Statement.RETURN_GENERATED_KEYS);
        insertIntoSongs = conn.prepareStatement(INSERT_SONGS);
        queryArtist = conn.prepareStatement(QUERY_ARTIST);
        queryAlbum = conn.prepareStatement(QUERY_ALBUM);
	}
	
	private int insertArtist(String name) throws SQLException {

        queryArtist.setString(1, name);
        ResultSet results = queryArtist.executeQuery();
        if(results.next()) {
            return results.getInt(1);
        } else {
            // Insert the artist
            insertIntoArtists.setString(1, name);
            int affectedRows = insertIntoArtists.executeUpdate();

            if(affectedRows != 1) {
                throw new SQLException("Couldn't insert artist!");
            }

            ResultSet generatedKeys = insertIntoArtists.getGeneratedKeys();
            if(generatedKeys.next()) {
                return generatedKeys.getInt(1);
            } else {
                throw new SQLException("Couldn't get _id for artist");
            }
        }
    }

    private int insertAlbum(String name, int artistId) throws SQLException {

        queryAlbum.setString(1, name);
        ResultSet results = queryAlbum.executeQuery();
        if(results.next()) {
            return results.getInt(1);
        } else {
            // Insert the album
            insertIntoAlbums.setString(1, name);
            insertIntoAlbums.setInt(2, artistId);
            int affectedRows = insertIntoAlbums.executeUpdate();

            if(affectedRows != 1) {
                throw new SQLException("Couldn't insert album!");
            }

            ResultSet generatedKeys = insertIntoAlbums.getGeneratedKeys();
            if(generatedKeys.next()) {
                return generatedKeys.getInt(1);
            } else {
                throw new SQLException("Couldn't get _id for album");
            }
        }
    }

    public void insertSong(String title, String artist, String album, int track) {
        try {
            conn.setAutoCommit(false);

            int artistId = insertArtist(artist);
            int albumId = insertAlbum(album, artistId);
            insertIntoSongs.setInt(1, track);
            insertIntoSongs.setString(2, title);
            insertIntoSongs.setInt(3, albumId);
            int affectedRows = insertIntoSongs.executeUpdate();
            if(affectedRows == 1) {
                conn.commit();
                System.out.println("Song inserted successfully.");
            } else {
                throw new SQLException("The song insert failed");
            }

        } catch(SQLException e) {
            System.out.println("Insert song exception: " + e.getMessage());
            try {
                System.out.println("Performing rollback");
                conn.rollback();
            } catch(SQLException e2) {
                System.out.println("Oh boy! Things are really bad! " + e2.getMessage());
            }
        } finally {
            try {
                System.out.println("Resetting default commit behavior");
                conn.setAutoCommit(true);
            } catch(SQLException e) {
                System.out.println("Couldn't reset auto-commit! " + e.getMessage());
            }

        }
    }
	
	public List<Artist> queryAllArtists(int sortOrder) {
		
		StringBuilder sb = new StringBuilder("SELECT * FROM ");
		sb.append(TABLE_ARTISTS);
		sb.append(generateSortOrder(sortOrder, COLUMN_ARTIST_NAME));
		
		System.out.println(sb.toString()); // TODO: DELETE
		
		try (Statement statement = conn.createStatement();
			 ResultSet results = statement.executeQuery(sb.toString())) {
			
			List<Artist> artists = new ArrayList<>();
			while (results.next()) {
				Artist artist = new Artist();
				artist.setId(results.getInt(INDEX_ARTIST_ID));
				artist.setName(results.getString(INDEX_ARTIST_NAME));
				artists.add(artist);
			}
			return artists;
			
		} catch (SQLException e) {
			System.err.println("Error: Cannot execute sql query." + e.getMessage());
			e.printStackTrace();
			return null;
		}
	}

	public List<String> queryAlbumsByArtist(String artist, int sortOrder) {
		
		StringBuilder sb = new StringBuilder("SELECT ");
		sb.append(TABLE_ALBUMS).append(".").append(COLUMN_ALBUM_NAME)
		  .append(" FROM ").append(TABLE_ALBUMS);
		sb.append(" INNER JOIN ").append(TABLE_ARTISTS).append(" ON ")
		  .append(TABLE_ALBUMS).append(".").append(COLUMN_ALBUM_ARTIST).append(" = ")
		  .append(TABLE_ARTISTS).append(".").append(COLUMN_ARTIST_ID);
		sb.append(" WHERE ").append(TABLE_ARTISTS).append(".").append(COLUMN_ARTIST_NAME)
		  .append(" LIKE '%").append(artist).append("%'");
		
		sb.append(generateSortOrder(sortOrder, TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME));
		System.out.println(sb.toString());
		
		try (Statement statement = conn.createStatement();
			 ResultSet results = statement.executeQuery(sb.toString())) {
		
			List<String> albums = new ArrayList<>();
			while (results.next()) {
				albums.add(results.getString(1)); // sql result has only 1 column
			}
			return albums;
			
		} catch (SQLException e) {
			System.err.println("Error: Cannot execute sql query." + e.getMessage());
			e.printStackTrace();
			return null;
		}
	}
	
	public List<SongMeta> secureQuerySongDetails(String title, int sortOrder) {
		try {
			ResultSet results = null;
			
			if (sortOrder == ORDER_DESC) {
				querySongMetaDesc.setString(1, title);
				results = querySongMetaDesc.executeQuery();
			}
			else {
				querySongMetaAsc.setString(1, title);
				results = querySongMetaAsc.executeQuery();
			}
				 
			List<SongMeta> songs = new ArrayList<>();
			while (results.next()) {
				SongMeta song = new SongMeta();
				song.setArtist(results.getString(1));
				song.setAlbum(results.getString(2));
				song.setTrack(results.getInt(3));
				songs.add(song);
			}
			return songs;

		} catch (SQLException e) {
			System.err.println("Error: Cannot execute sql query." + e.getMessage());
			e.printStackTrace();
			return null;
		}
	}
	
	private String generateSortOrder(int sortOrder, String sortFields) {
		StringBuilder sb = new StringBuilder(" ORDER BY ");
		sb.append(sortFields).append(" COLLATE NOCASE ");
		
		if (sortOrder == ORDER_DESC) sb.append("DESC");
		else sb.append("ASC");
		
		return sb.toString();
	}
}
