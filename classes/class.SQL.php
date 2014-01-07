<?php
/**
 * Klasse zum behandeln von MySQL anfragen
 * @author Andreas Kasper <Andreas.Kasper@plabsi.com>
 * @package ASICMS
 * @version 0.1.20121113
 */
class SQL {

	/**
	 * In dieser Variablen werden die Informationen zur Verbindung gespeichert.
	 * @var $_conn
	 */
	private static $_conn = array();

	/**
	 * Das letzte MySQL-Result
	 * @var mysqlresult
	 */
	var $result = 0;

	var $defaultconnection = -1;

	/**
	 * Sollen Fehler ausgegeben werden?
	 */
	var $showerror = false;

	/**
	 * Letzter ausgeführter Befehl
	 * @var string
	 */
	var $lastcmd = "";
	var $lasterrornr = 0;
	var $lasterror = "";

	/**
	 * Die Historie der ausgeführten Befehle.
	 * @var array
	 */
	var $historycmd = array();

	/**
	 * Einstellungswert der festlegt, ob eine Befehlshistorie geführt werden soll
	 * @var boolean
	 */
	var $savehistory = false;
	public static $counter = 0;
	public static $timer = 0;

	/**
	 * Konstruktor für die SQL-Klasse
	 * @param integer $defaultconnection Die Verbindnungsnummer die Standardmäßig verwendet werden soll. Wenn zusätzlich die DBuri angegeben ist, wird diese überschrieben.
	 * @param uri $DBuri URI der MySQL-Datenbank. (optional)
	 */
	function SQL($defaultconnection = -1, $DBuri = null) {
		if ($DBuri != null AND $defaultconnection >= 0) {
			SQL::init($defaultconnection, $DBuri);
		}
		$this->defaultconnection = $defaultconnection+0;
	}
	
	public static function init($ConnNr = 0, $DBuri) {
		self::$_conn[$ConnNr]["conn"] = $DBuri;
		$a = parse_url($DBuri);
		$b = explode("/", $a["path"]);
		if ($a["scheme"] != "mysql") trigger_error("Kein MySQL-Schema");
		if (!isset($a["port"])) $a["port"] = 3306;
		self::$_conn[$ConnNr]["host"] = $a["host"];
		self::$_conn[$ConnNr]["port"] = $a["port"];
		self::$_conn[$ConnNr]["user"] = $a["user"];
		self::$_conn[$ConnNr]["password"] = (isset($a["pass"])?$a["pass"]:"");
		self::$_conn[$ConnNr]["database"] = $b[1];
		self::$_conn[$ConnNr]["prefix"] = $b[2];
		return true;
	}

	/** Führt einen SQL Befehl aus.
	 *
	 * @param integer $connection Verweis auf die Verbindnungsnummer in der Konfigurationsdatei
	 * @param string $sql SQL-Befehl
	 * @param boolean $silent Schnellere ausführung aber keine Antwort
	 * @param array $values Die Werte, die im Command ersetzt werden sollen.
	 * @result mysqlresult
	 */
	function cmd($connection = 0, $sql = "", $silent = false, $values = array()) {
		if (($connection+0 == 0) AND ($sql."" == "")) {
			$sql = $connection;
			$connection = $this->defaultconnection;
		}
		if (gettype($connection) == "resource") {
			$conn = $connection;
		} else {
			$conn = $this->Verbindungsnr($connection);
		}
		foreach ($values as $k=>$v) $sql = str_replace("{".$k."}", SQL::convtxt($v), $sql); 
		//Exploit?
/*
		if (strpos($sql, "---") !== FALSE) {
			trigger_error("SQL-Script injection per ---", E_USER_ERROR);
			return 0;
		}
*/
		$this->lasterrornr = 0;
		$this->lasterror = "";
		self::$counter++;
		if ($silent) {
			$dauer = microtime(true);
			mysql_unbuffered_query($sql, $conn);
			$dauer = microtime(true)-$dauer;
			self::$timer += $dauer;
			$this->lastcmd = $sql;
			if ($this->savehistory) $this->historycmd[] = array("cmd" => $sql, "time" => $dauer);
			unset($this->historycmd[100]);
			try {
			
			} catch (Exception $e) {
		}
			return true;
		} else {
			$dauer = microtime(true);
			$this->result = @mysql_query($sql, $conn);
			$dauer = microtime(true)-$dauer;
			self::$timer += $dauer;
			$this->lastcmd = $sql;
			if (!$this->result) {
				$this->lasterrornr = mysql_errno();
				$this->lasterror = mysql_error();
			}
			try {
			
			} catch (Exception $e) {
			}
			if ($this->savehistory) $this->historycmd[] = array("cmd" => $sql, "time" => $dauer);
			unset($this->historycmd[100]);
			return $this->result;
		}
	}
	
	function cmdBackground($connection = 0, $sql = "", $silent = false, $values = array()) {
		$t = new Thread(array($this, "cmd"));
		$t->start($connection, $sql, true, $values);
		return true;
	}

	/** Zählt die Anzahl der Zeilen der vorangegangenen Abfrage.
	 *
	 * @result integer
	 */
	function countrows() {
		return mysql_num_rows($this->result);
	}
	
	/**
	* Diese muss den Parameter SQL_CALC_FOUND_ROWS haben...
	**/
	function countallrows($connection = -1) {
		return $this->cmdvalue($connection, "SELECT FOUND_ROWS()");
	}

	/** Ermittelt die Verbindungsnummer aus einer definierten Connectionnummer aus den Daten der Konfigurationsdatei
	 *
	 * @param integer $connection Verweis auf die Verbindnungsnummer in der Konfigurationsdatei
	 * @result mysqlresult
	 */
	function Verbindungsnr($connection) {
		if ($connection == -1) $connection = $this->defaultconnection;
		if (!isset(self::$_conn[$connection]["vnr"])) {
			if (!isset(self::$_conn[$connection]) && isset($_ENV["config"]["db"][$connection]["conn"])) self::init($connection, $_ENV["config"]["db"][$connection]["conn"]);
			if (!isset(self::$_conn[$connection])) {
				throw new Exception("Kein Verbindungsschema für Datenbank Nummer".$connection."!");
				exit(1);
			}
			if (!function_exists("mysql_connect")) {
				throw new Exception("Die MySQL-Extension wurde nicht auf dem Server installiert!");
				exit();
			}
			self::$_conn[$connection]["vnr"] = mysql_connect(self::$_conn[$connection]["host"].':'.self::$_conn[$connection]["port"],self::$_conn[$connection]["user"],self::$_conn[$connection]["password"]);
			if (!self::$_conn[$connection]["vnr"]) throw new SQLException("Keine Verbindung zum Datenbank-Server ".$connection, 1);
			mysql_query("SET NAMES 'utf8'");
			mysql_query("SET CHARACTER_SET_CLIENT='utf8'");
			mysql_select_db(self::$_conn[$connection]["database"]);
		};
		return self::$_conn[$connection]["vnr"];
	}
	
	public function getTableName($tablename = "", $connection = -1) {
		if ($connection == -1) $connection = $this->defaultconnection;
		return self::$_conn[$connection]["dbprefix"].$tablename;
	}

	/** Ermittelt alle Ergebniszeilen einer MySQL-Abfrage
	 *
	 * @param integer $connection Verweis auf die Verbindnungsnummer in der Konfigurationsdatei
	 * @param string $sql SQL-Befehl
	 * @result array
	 */
	function cmdrows($connection = 0, $sql = "", $values = array(), $key = null) {
		if ($connection == -1) $connection = $this->defaultconnection;
		if (($connection+0 == 0) AND ($sql."" == "")) {
			$sql = $connection;
			$connection = $this->defaultconnection;
		}
		foreach ($values as $k=>$v) $sql = str_replace("{".$k."}", SQL::convtxt($v), $sql); 
		$result = $this->cmd($connection, $sql);
		if (!$result) {
			throw new SQLException("Ungueltiger SQL-Befehl: (".$sql.")!\r\n".mysql_error(),602);
			exit(1);
		};
		$out = array();
		if (mysql_num_rows($result) > 0)
		While ($row = mysql_fetch_array($result, MYSQL_BOTH))
			if ($key == null) $out[] = $row; else $out[$row[$key]] = $row;
		mysql_free_result($result);  //Natürlich geben wir den Speicher wieder frei
		return $out;
	}

	/** Ermittelt eine/die erste Ergebniszeile aus einer MySQL-Abfrage.
	 *
	 * @param integer $connection Verweis auf die Verbindnungsnummer in der Konfigurationsdatei
	 * @param string $sql SQL-Befehl
	 * @result array
	 */
	function cmdrow($connection = -1, $sql = "", $values = array()) {
		if ($connection == -1) $connection = $this->defaultconnection;
		if (($connection+0 == 0) AND ($sql."" == "")) {
			$sql = $connection;
			$connection = $this->defaultconnection;
		}
		foreach ($values as $k=>$v) $sql = str_replace("{".$k."}", SQL::convtxt($v), $sql); 
		$result = $this->cmd($connection, $sql);
		if (!$result) {
			//die("Ungueltiger SQL-Befehl: (".$sql.")!\r\n".mysql_error($this->Verbindungsnr($connection)));
				throw new SQLException("Ungültiger SQL-Befehl: (".$sql.")!\r\n".mysql_error(), 602);
				exit(1);
		}
		$row = array();
		if (mysql_num_rows($result) > 0)
		$row = mysql_fetch_array($result, MYSQL_BOTH);
		mysql_free_result($result);  //Natürlich geben wir den Speicher wieder frei
		return $row;
	}

	function cmdvalue($connection = -1, $sql = "", $values = array()) {
		if ($connection == -1) $connection = $this->defaultconnection;
		if (($connection+0 == 0) AND ($sql."" == "")) {
			$sql = $connection;
			$connection = $this->defaultconnection;
		}
		
		foreach ($values as $k=>$v) $sql = str_replace("{".$k."}", SQL::convtxt($v), $sql);
		if (stripos($sql, "LIMIT") === FALSE) $sql .= " LIMIT 0,1";
		
		$result = $this->cmd($connection, $sql);
		if (!$result) {
			throw new SQLException("Ungueltiger SQL-Befehl: (".$sql.")!\r\n".mysql_error(), 602);
		}
		if (mysql_num_rows($result) > 0)
		$row = mysql_fetch_array($result, MYSQL_NUM);
		return isset($row) ? $row[0] : null;
	}

	/** Führt einen Datenbankenupdate aus
	 *
	 * @param integer $conn Verweis auf die Verbindnungsnummer in der Konfigurationsdatei
	 * @param string $table Name der MySQL-Tabelle.
	 * @param array $arr Ein Array aus Werten, wobei der Schlüssel die Feldbezeichnung ist.
	 * @param string/array $ids Array oder String mit Schlüsselwerten der Tabelle.
	 * @param integer $LimitAnzahl Anzahl der Zeilen, die Upgedated werden dürfen.
	 * @result mysqlresult
	 */
	function Update( $conn = -1, $table = "", $arr = array(), $ids = "", $LimitAnzahl = -1) {
		if ($conn == -1) $conn = $this->defaultconnection;
		if (is_string($ids)) $ids = array($ids);
		if (strpos($table, "`"))
		$table = self::BQStable($conn, $table);
		
		foreach ($arr as $key => $v) $fSet[count($fSet)] = ' `'.$key.'` = "'.SQL::convtxt($v).'"';
		foreach ($ids as $key) $fWhere[count($fWhere)] = ' (`'.$key.'` = "'.SQL::convtxt($arr[$key]).'")';

		$sql = "UPDATE ".$table." SET ".implode(",",$fSet)." WHERE ".implode("AND",$fWhere)." ";
		if ($LimitAnzahl != -1) $sql .= " LIMIT ".$LimitAnzahl;

		return $this->cmd($conn, $sql);
	}

	function BQSset($arr) {
		foreach ($arr as $key => $value) $fSet2[] = ' `'.$key.'` = "'.SQL::convtxt($value).'" ';
		return " SET ".implode(",", $fSet2);
	}

	function BQStable($conn, $str) {
		$a = 0;
		$j = false;
		$g = array("","");
		for ($i = 0; $i < strlen($str); $i++) {
			$z=substr($str, $i, 1);
			if ($z == "`") { $j = !$j; $g[$a] .= $z;}
			elseif (($z == ".") AND (!$j)) $a++;
			else $g[$a] .= $z;
		}
		if ($g[1] == "") {
			$g[1] = $g[0];
			$a = parse_url(self::$_conn[$conn]["conn"]);
			$b = explode("/", $a["path"]);
			$g[0] = $b[1];
		}
		if (strpos($g[0], "`") === FALSE) $g[0] = "`".$g[0]."`";
		if (strpos($g[1], "`") === FALSE) $g[1] = "`".$g[1]."`";
			
		return $g[0].'.'.$g[1];
	}

	/**
	 * Ermittelt die PRIMARY-Felder einer Tabelle
	 * @param integer $conn Verbindungsnummer
	 * @param string $table NAme der Tabelle
	 * @return array
	 */
	function GetTableIDs($conn = -1, $table = "") {
		if ($conn == -1) $conn = $this->defaultconnection;
		$out =array();
		$rows = $this->cmdrows($conn, "SHOW COLUMNS FROM ".$table);
		foreach ($rows as $row) {
			if ($row["Key"] == "PRI") $out[] = $row["Field"];
		}
		return $out;
	}

	/** Erstellt oder updated einen MySQL Datensatz
	 *
	 * @param integer $conn Verweis auf die Verbindnungsnummer in der Konfigurationsdatei
	 * @param string $table Name der MySQL-Tabelle.
	 * @param array $arr Ein Array aus Werten, wobei der Schlüssel die Feldbezeichnung ist.
	 * @param string/array $ids Array oder String mit Schlüsselwerten der Tabelle.
	 * @result mysqlresult
	 */
	function Create( $conn = -1, $table = "", $arr = array()) {
		if ($conn == -1) $conn = $this->defaultconnection;
		$sql = "INSERT LOW_PRIORITY IGNORE INTO ".SQL::BQStable($conn, $table).SQL::BQSset($arr);
		return $this->cmd($conn, $sql);
	}


	/** Erstellt oder updated einen MySQL Datensatz
	 *
	 * @param integer $conn Verweis auf die Verbindnungsnummer in der Konfigurationsdatei
	 * @param string $table Name der MySQL-Tabelle.
	 * @param array $arr Ein Array aus Werten, wobei der Schlüssel die Feldbezeichnung ist.
	 * @param string/array $ids Array oder String mit Schlüsselwerten der Tabelle.
	 * @result mysqlresult
	 */
	function CreateUpdate( $conn = -1, $table = "", $arr = array()) {
		if ($conn == -1) $conn = $this->defaultconnection;

		$fset = self::BQSset($arr);
		$fd = substr($fset,5);

		$sql = "INSERT LOW_PRIORITY INTO ".self::BQStable($conn, $table).$fset." ON DUPLICATE KEY UPDATE ".$fd;
		return $this->cmd($conn, $sql, true);
	}

	function CreateUpdateOld( $conn = -1, $table = "", $arr = array(), $ids = "") {
		if ($conn == -1) $conn = $this->defaultconnection;
		if (strpos($table,".") === FALSE) {
			$table = $_ENV["config"]["mysql"][$conn]["database"].".".$table;
		}
		if ($ids == "") {
			$ids = $this->GetTableIDs($conn, $table);
		}
		if (is_string($ids)) $ids = array($ids);
		if (strpos($table, "`"))
		$table = "`".implode("`.`", explode(".", $table))."`";

		foreach ($arr as $key => $v) $fSet[] = ' `'.$key.'` = "'.$this->convtxt($v).'"';
		foreach ($ids as $key) $fWhere[] = ' (`'.$key.'` = "'.$this->convtxt($arr[$key]).'")';
		foreach ($ids as $key) $fSet2[] = ' `'.$key.'` = "'.$this->convtxt($arr[$key]).'" ';

		$sql = "INSERT LOW_PRIORITY IGNORE INTO ".$table." SET ".implode(",",$fSet2);
		$this->cmd($conn, $sql, true);
		$sql = "UPDATE LOW_PRIORITY ".$table." SET ".implode(",",$fSet)." WHERE ".implode("AND",$fWhere)." ";
		return $this->cmd($conn, $sql, true);
	}

	/** Führt einen Datenbankenselect aus
	 *
	 * @param integer $conn Verweis auf die Verbindungsnummer in der Konfigurationsdatei
	 * @param string $table Name der MySQL-Tabelle.
	 * @param string $where Bestandteil der WHERE Anfrage
	 * @param array $arrFields Welche Felder sollen ausgegeben werden?
	 * @result mysqlresult
	 */
	function Select($conn = -1, $fields = "", $WhereData = array(), $WhereStructure = "1") {
		if ($conn == -1) $conn = $this->defaultconnection;
		if (is_string($fields)) $fields = array($fields);

		foreach ($fields as $key => $Itm) {
			if (substr_count($Itm) == 0) $Itm = $_ENV["config"]["mysql"][$connection]["table"].".".$Itm;
			if (substr_count($Itm) == 1) $Itm = $_ENV["config"]["mysql"][$connection]["database"].".".$Itm;
			//$Itm = str_replace("`", "", $Itm);
			//$Itm = "`".str_replace(".", "`.`", $Itm)."`";
			$arrf[$key] = $Itm;
			$usedfields[$Itm] = $Itm;
			$usedtables[substr($Itm,0, strrpos($Itm, "."))] = substr($Itm,0, strrpos($Itm, "."));
		}

		//FIXME: Vervollständigen

			
		$sql = "SELECT ".implode(",",$arrFields)." FROM ".$table." WHERE ".$Where;
		return $this->cmd($conn, $sql);
	}
	
	function Simple($conn = -1, $command = "", $data = array()) {
		$g = string::ZwischenstringArray($command, "{", "}");
		foreach($g as $a) {
			$command = str_replace("{".$a."}", str_replace(chr(39), chr(39).chr(39), $data[$a]), $command);
			}
		unset($g);
	return $this->cmdrows($conn, $command);
	}
	
	function LastInsertKey($conn = -1) {
		return @mysql_insert_id($this->Verbindungsnr($conn));
		}
	
	/**
	 * Splittet einen Connectionstring in seine Bestandteile auf und fügt Sie in die Konfiguration ein.
	 * Bei erfolg wird die ConnectionID zurückgegeben. Bei Misserfolg FALSE.
	 * 
	 * @param integer $id ConnectionID
	 * @param string $aurl Connectionstring
	 * @return bool/integer
	 */
	function ConnectionString($id = -1, $aurl = "") {
		if ($id == -1) $id = count($_ENV["config"]["mysql"]);
		$b = parse_url($aurl);
		if ($b["scheme"] != "mysql") return FALSE;
		$_ENV["config"]["mysql"][$id]["host"] = $b["host"];
		if ($b["port"] + 0 == 0) $b["port"] = 3306;
		$_ENV["config"]["mysql"][$id]["port"] = $b["port"];
		$_ENV["config"]["mysql"][$id]["user"] = $b["user"];
		$_ENV["config"]["mysql"][$id]["password"] = $b["pass"];
		$g = explode("/", $b["path"]);
		$_ENV["config"]["mysql"][$id]["database"] = $g[0];
		$_ENV["config"]["mysql"][$id]["table"] = $g[1];
		return $id;
	}

	public static function convtxt($txt) {
		$txt = str_replace(chr(34), chr(34).chr(34), $txt);
		return $txt;
	}
	
	function export($conn = -1, $database = "", $filename = "") {
		$doc = new DomDocument("1.0", "UTF-8");
		$doc->formatOutput = true;
		$root = $doc->appendChild($doc->createElement("MySQL"));
		$root->setAttributeNS(XMLNS_NAMESPACE, "xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance");
		$root->setAttributeNS("http://www.w3.org/2001/XMLSchema-instance", "xsi:noNamespaceSchemaLocation", "http://asicms.sourceforge.net/SQLExport.xsd");
		
		$root->setAttribute("version", "1.0");

		$rootdb = $root->appendChild($doc->createElement("database"));
		$rootdb->setAttribute("name" , $database);
		$trows = $this->cmdrows($conn, "SELECT * FROM information_schema.TABLES WHERE TABLE_SCHEMA = '".$database."'");
		foreach ($trows as $trow) {
			$roott = $rootdb->appendChild($doc->createElement("table"));
			$roott->setAttribute("name" , $trow["TABLE_NAME"]);
			$roott->setAttribute("engine" , $trow["ENGINE"]);

			$crows = $this->cmdrows($conn, "SELECT * FROM information_schema.COLUMNS WHERE (`TABLE_SCHEMA` = '".$database."') AND (`TABLE_NAME`='".$trow["TABLE_NAME"]."') ORDER BY ORDINAL_POSITION");
			foreach ($crows as $crow) {
				$rootc = $roott->appendChild($doc->createElement("field"));
				$rootc->setAttribute("name" , $crow["COLUMN_NAME"]);
				$rootc->setAttribute("type" , $crow["COLUMN_TYPE"]);
				$rootc->setAttribute("collation" , $crow["COLLATION_NAME"]);
				if ($crow["COLUMN_KEY"] == "PRI")
					$rootc->setAttribute("primary" , true);
				if ($crow["EXTRA"] != "")
					$rootc->setAttribute("extra" , $crow["EXTRA"]);
				}
			
			$crows = $this->cmdrows($conn, "SELECT * FROM information_schema.TABLE_CONSTRAINTS WHERE (`CONSTRAINT_SCHEMA` = '".$database."') AND (`TABLE_NAME`='".$trow["TABLE_NAME"]."')");
			$rooti = $roott->appendChild($doc->createElement("indexes"));
			foreach ($crows as $crow) {
				$rootc = $rooti->appendChild($doc->createElement("index"));
				$rootc->setAttribute("name" , $crow["CONSTRAINT_NAME"]);
				$rootc->setAttribute("type" , $crow["CONSTRAINT_TYPE"]);
				$rootc->appendChild($doc->createTextNode($crow["CONSTRAINT_TYPE"]));
				}
				
			}
		
		if ($filename == "") echo($doc->saveXML());
		}
		
	function import($conn = -1, $filename = "") {
		$doc = new DomDocument("1.0", "UTF-8");
		$doc = loadXML($filename);
		//TODO: Muss fertiggestellt werden!
		}
}

class SQLException extends Exception {

}
?>