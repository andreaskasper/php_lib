<?php

class fcache {

	private static $_tempdir = null;

	public static function write($key, $data) {
		return (file_put_contents(self::getFile($key), serialize($data)) != false);
	}
	
	public static function read($key, $sec=-1, $callback = null) {
		$local = self::getFile($key);
		if (!file_exists($local) OR ($sec > -1 AND filemtime($local)+rand($sec/2,$sec)>time())) {
			if ($callback == null AND is_callable($callback)) return null;
			if ($callback != null) {
				if (is_string($callback) OR is_array($callback)) $b = call_user_func($callback, $key);
				else $b = $callback($key);
				if ($b != null) self::write($key, $b);
			}
		}
		return unserialize(@file_get_contents($local));
	}
	
	public static function exists($key){
		return file_exists(self::getFile($key));
	}
	
	public static function get($key, $callback = null) {
		$local = self::getFile($key);
		if (!file_exists($local)) {
			if ($callback == null AND is_callable($callback)) return null;
			if ($callback != null) {
				if (is_string($callback) OR is_array($callback)) $b = call_user_func($callback, $key);
				else $b = $callback($key);
				if ($b != null) self::write($key, $b);
			}
		}
		return unserialize(@file_get_contents($local));
	}
	
	public static function expired($key, $sec=-1) {
		$local = self::getFile($key);
		return (!file_exists($local) OR ($sec > -1 AND filemtime()+$sec>time()));
	}
	
	public static function shoulduse($key, $sec=-1) {
		$local = self::getFile($key);
		return !(!file_exists($local) OR ($sec > -1 AND filemtime($local)+rand($sec/2,$sec)>time()));
	}
	
	public static function refresh($key) {
		$local = self::getFile($key);
		if (!file_exists($local)) return false;
		file_put_contents($local,file_get_contents($local));
		return true;
	}
	
	public static function delete($key) {
		return unlink(self::getFile($key));
	}
	
	public static function truncate() {
		$handle=opendir($_ENV["basepath"]."/app/cache/");  
		if (!$handle) return false;
		while($file=readdir($handle)) {  
			if(!is_dir($file) && $file!="." && $file!=".." && (substr($file,-7) == ".fcache")) unlink($file); 
			} 
		closedir($handle);
		return true;
	}
	
	public static function setTempDir($path) {
		if (substr($path, -1) == "/") $path = substr($path, 0, strlen($path)-1);
		self::$_tempdir = $path;
	}
	
	//Hilfsfunktionen
	private static function getFile($key = null) {
		if (self::$_tempdir == null) return $_ENV["basepath"]."/app/cache/".md5($key).".fcache";
		return self::$_tempdir."/".md5($key).".fcache";
	}
}