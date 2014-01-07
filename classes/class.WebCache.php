<?php

/**
 * 
 * @author Andreas Kasper <djassi@users.sourceforge.net>
 * @category lucenzo
 * @copyright 2012 by Andreas Kasper
 * @name WebCache
 * @link http://www.plabsi.com Plabsi Weblabor
 * @license FastFoodLicense
 * @version 0.1.121031
 */

class WebCache {
	
	/**
	 * Zähler für die Webanfragen
	 * @static
	 * @var integer
	 */
	private static $WebRequestCounter = 0;

	/**
	 * Macht eine Webanfrage und gibt den Wert zurück. Wenn keine Daten geladen werden können, kommt NULL.
	 * @param string $url Webadresse
	 * @param integer $sec Cachelaufzeit in Sekunden
	 * @param string|mixed $needle Array oder String der Werte, die in der Antwort vorkommen müssen
	 * @return string|null Quellcode der Webseite oder NULL
	 * @static
	 */
	public static function get($url, $sec = 86400, $needle = "") {
		$local = $_ENV["basepath"]."/app/cache/".md5($url).".webcache";
		if (!file_exists($local) OR (filemtime($local)+rand($sec/2, $sec*(self::$WebRequestCounter+1)) < time())) {
			self::$WebRequestCounter++;
			$html = @file_get_contents($url);
			$j = true;
			if (is_string($needle) and ($needle != "")) $j = (strpos($html, $needle) !== FALSE);
			if (is_array($needle)) foreach ($needle as $a) if ($j AND (strpos($html, $a) === FALSE)) $j = false;;
			if ($j) file_put_contents($local, $html);
		}
		if (!file_exists($local)) return null;
		return file_get_contents($local);
	}
}