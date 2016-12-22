// Generated from C:/Users/gefeili/Documents/gefei/IdeaProject/Spinach/sql/catalyst/src/main/antlr4/org/apache/spark/sql/catalyst/parser\SqlBase.g4 by ANTLR 4.6
package org.apache.spark.sql.catalyst.parser;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.*;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class SqlBaseLexer extends Lexer {
	static { RuntimeMetaData.checkVersion("4.6", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, T__6=7, SELECT=8, FROM=9, 
		ADD=10, AS=11, ALL=12, DISTINCT=13, WHERE=14, GROUP=15, BY=16, GROUPING=17, 
		SETS=18, CUBE=19, ROLLUP=20, ORDER=21, HAVING=22, LIMIT=23, AT=24, OR=25, 
		AND=26, IN=27, NOT=28, NO=29, EXISTS=30, BETWEEN=31, LIKE=32, RLIKE=33, 
		IS=34, NULL=35, TRUE=36, FALSE=37, NULLS=38, ASC=39, DESC=40, FOR=41, 
		INTERVAL=42, CASE=43, WHEN=44, THEN=45, ELSE=46, END=47, JOIN=48, CROSS=49, 
		OUTER=50, INNER=51, LEFT=52, SEMI=53, RIGHT=54, FULL=55, NATURAL=56, ON=57, 
		LATERAL=58, WINDOW=59, OVER=60, PARTITION=61, RANGE=62, ROWS=63, UNBOUNDED=64, 
		PRECEDING=65, FOLLOWING=66, CURRENT=67, ROW=68, WITH=69, VALUES=70, CREATE=71, 
		TABLE=72, VIEW=73, REPLACE=74, INSERT=75, DELETE=76, INTO=77, DESCRIBE=78, 
		EXPLAIN=79, FORMAT=80, LOGICAL=81, CODEGEN=82, CAST=83, SHOW=84, TABLES=85, 
		COLUMNS=86, COLUMN=87, USE=88, PARTITIONS=89, FUNCTIONS=90, DROP=91, UNION=92, 
		EXCEPT=93, INTERSECT=94, TO=95, TABLESAMPLE=96, STRATIFY=97, ALTER=98, 
		RENAME=99, ARRAY=100, MAP=101, STRUCT=102, COMMENT=103, SET=104, RESET=105, 
		DATA=106, START=107, TRANSACTION=108, COMMIT=109, ROLLBACK=110, MACRO=111, 
		IF=112, EQ=113, NSEQ=114, NEQ=115, NEQJ=116, LT=117, LTE=118, GT=119, 
		GTE=120, PLUS=121, MINUS=122, ASTERISK=123, SLASH=124, PERCENT=125, DIV=126, 
		TILDE=127, AMPERSAND=128, PIPE=129, HAT=130, PERCENTLIT=131, BUCKET=132, 
		OUT=133, OF=134, SORT=135, CLUSTER=136, DISTRIBUTE=137, OVERWRITE=138, 
		TRANSFORM=139, REDUCE=140, USING=141, SERDE=142, SERDEPROPERTIES=143, 
		RECORDREADER=144, RECORDWRITER=145, DELIMITED=146, FIELDS=147, TERMINATED=148, 
		COLLECTION=149, ITEMS=150, KEYS=151, ESCAPED=152, LINES=153, SEPARATED=154, 
		FUNCTION=155, EXTENDED=156, REFRESH=157, CLEAR=158, CACHE=159, UNCACHE=160, 
		LAZY=161, FORMATTED=162, TEMPORARY=163, OPTIONS=164, UNSET=165, TBLPROPERTIES=166, 
		DBPROPERTIES=167, BUCKETS=168, SKEWED=169, STORED=170, DIRECTORIES=171, 
		LOCATION=172, EXCHANGE=173, ARCHIVE=174, UNARCHIVE=175, FILEFORMAT=176, 
		TOUCH=177, COMPACT=178, CONCATENATE=179, CHANGE=180, CASCADE=181, RESTRICT=182, 
		CLUSTERED=183, SORTED=184, PURGE=185, INPUTFORMAT=186, OUTPUTFORMAT=187, 
		DATABASE=188, DATABASES=189, DFS=190, TRUNCATE=191, ANALYZE=192, COMPUTE=193, 
		LIST=194, STATISTICS=195, PARTITIONED=196, EXTERNAL=197, DEFINED=198, 
		REVOKE=199, GRANT=200, LOCK=201, UNLOCK=202, MSCK=203, REPAIR=204, EXPORT=205, 
		IMPORT=206, LOAD=207, ROLE=208, ROLES=209, COMPACTIONS=210, PRINCIPALS=211, 
		TRANSACTIONS=212, INDEX=213, INDEXES=214, LOCKS=215, OPTION=216, ANTI=217, 
		LOCAL=218, INPATH=219, SINDEX=220, STRING=221, BIGINT_LITERAL=222, SMALLINT_LITERAL=223, 
		TINYINT_LITERAL=224, BYTELENGTH_LITERAL=225, INTEGER_VALUE=226, DECIMAL_VALUE=227, 
		SCIENTIFIC_DECIMAL_VALUE=228, DOUBLE_LITERAL=229, IDENTIFIER=230, BACKQUOTED_IDENTIFIER=231, 
		SIMPLE_COMMENT=232, BRACKETED_COMMENT=233, WS=234, UNRECOGNIZED=235;
	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	public static final String[] ruleNames = {
		"T__0", "T__1", "T__2", "T__3", "T__4", "T__5", "T__6", "SELECT", "FROM", 
		"ADD", "AS", "ALL", "DISTINCT", "WHERE", "GROUP", "BY", "GROUPING", "SETS", 
		"CUBE", "ROLLUP", "ORDER", "HAVING", "LIMIT", "AT", "OR", "AND", "IN", 
		"NOT", "NO", "EXISTS", "BETWEEN", "LIKE", "RLIKE", "IS", "NULL", "TRUE", 
		"FALSE", "NULLS", "ASC", "DESC", "FOR", "INTERVAL", "CASE", "WHEN", "THEN", 
		"ELSE", "END", "JOIN", "CROSS", "OUTER", "INNER", "LEFT", "SEMI", "RIGHT", 
		"FULL", "NATURAL", "ON", "LATERAL", "WINDOW", "OVER", "PARTITION", "RANGE", 
		"ROWS", "UNBOUNDED", "PRECEDING", "FOLLOWING", "CURRENT", "ROW", "WITH", 
		"VALUES", "CREATE", "TABLE", "VIEW", "REPLACE", "INSERT", "DELETE", "INTO", 
		"DESCRIBE", "EXPLAIN", "FORMAT", "LOGICAL", "CODEGEN", "CAST", "SHOW", 
		"TABLES", "COLUMNS", "COLUMN", "USE", "PARTITIONS", "FUNCTIONS", "DROP", 
		"UNION", "EXCEPT", "INTERSECT", "TO", "TABLESAMPLE", "STRATIFY", "ALTER", 
		"RENAME", "ARRAY", "MAP", "STRUCT", "COMMENT", "SET", "RESET", "DATA", 
		"START", "TRANSACTION", "COMMIT", "ROLLBACK", "MACRO", "IF", "EQ", "NSEQ", 
		"NEQ", "NEQJ", "LT", "LTE", "GT", "GTE", "PLUS", "MINUS", "ASTERISK", 
		"SLASH", "PERCENT", "DIV", "TILDE", "AMPERSAND", "PIPE", "HAT", "PERCENTLIT", 
		"BUCKET", "OUT", "OF", "SORT", "CLUSTER", "DISTRIBUTE", "OVERWRITE", "TRANSFORM", 
		"REDUCE", "USING", "SERDE", "SERDEPROPERTIES", "RECORDREADER", "RECORDWRITER", 
		"DELIMITED", "FIELDS", "TERMINATED", "COLLECTION", "ITEMS", "KEYS", "ESCAPED", 
		"LINES", "SEPARATED", "FUNCTION", "EXTENDED", "REFRESH", "CLEAR", "CACHE", 
		"UNCACHE", "LAZY", "FORMATTED", "TEMPORARY", "OPTIONS", "UNSET", "TBLPROPERTIES", 
		"DBPROPERTIES", "BUCKETS", "SKEWED", "STORED", "DIRECTORIES", "LOCATION", 
		"EXCHANGE", "ARCHIVE", "UNARCHIVE", "FILEFORMAT", "TOUCH", "COMPACT", 
		"CONCATENATE", "CHANGE", "CASCADE", "RESTRICT", "CLUSTERED", "SORTED", 
		"PURGE", "INPUTFORMAT", "OUTPUTFORMAT", "DATABASE", "DATABASES", "DFS", 
		"TRUNCATE", "ANALYZE", "COMPUTE", "LIST", "STATISTICS", "PARTITIONED", 
		"EXTERNAL", "DEFINED", "REVOKE", "GRANT", "LOCK", "UNLOCK", "MSCK", "REPAIR", 
		"EXPORT", "IMPORT", "LOAD", "ROLE", "ROLES", "COMPACTIONS", "PRINCIPALS", 
		"TRANSACTIONS", "INDEX", "INDEXES", "LOCKS", "OPTION", "ANTI", "LOCAL", 
		"INPATH", "SINDEX", "STRING", "BIGINT_LITERAL", "SMALLINT_LITERAL", "TINYINT_LITERAL", 
		"BYTELENGTH_LITERAL", "INTEGER_VALUE", "DECIMAL_VALUE", "SCIENTIFIC_DECIMAL_VALUE", 
		"DOUBLE_LITERAL", "IDENTIFIER", "BACKQUOTED_IDENTIFIER", "EXPONENT", "DIGIT", 
		"LETTER", "SIMPLE_COMMENT", "BRACKETED_COMMENT", "WS", "UNRECOGNIZED"
	};

	private static final String[] _LITERAL_NAMES = {
		null, "'('", "')'", "','", "'.'", "'['", "']'", "':'", "'SELECT'", "'FROM'", 
		"'ADD'", "'AS'", "'ALL'", "'DISTINCT'", "'WHERE'", "'GROUP'", "'BY'", 
		"'GROUPING'", "'SETS'", "'CUBE'", "'ROLLUP'", "'ORDER'", "'HAVING'", "'LIMIT'", 
		"'AT'", "'OR'", "'AND'", "'IN'", null, "'NO'", "'EXISTS'", "'BETWEEN'", 
		"'LIKE'", null, "'IS'", "'NULL'", "'TRUE'", "'FALSE'", "'NULLS'", "'ASC'", 
		"'DESC'", "'FOR'", "'INTERVAL'", "'CASE'", "'WHEN'", "'THEN'", "'ELSE'", 
		"'END'", "'JOIN'", "'CROSS'", "'OUTER'", "'INNER'", "'LEFT'", "'SEMI'", 
		"'RIGHT'", "'FULL'", "'NATURAL'", "'ON'", "'LATERAL'", "'WINDOW'", "'OVER'", 
		"'PARTITION'", "'RANGE'", "'ROWS'", "'UNBOUNDED'", "'PRECEDING'", "'FOLLOWING'", 
		"'CURRENT'", "'ROW'", "'WITH'", "'VALUES'", "'CREATE'", "'TABLE'", "'VIEW'", 
		"'REPLACE'", "'INSERT'", "'DELETE'", "'INTO'", "'DESCRIBE'", "'EXPLAIN'", 
		"'FORMAT'", "'LOGICAL'", "'CODEGEN'", "'CAST'", "'SHOW'", "'TABLES'", 
		"'COLUMNS'", "'COLUMN'", "'USE'", "'PARTITIONS'", "'FUNCTIONS'", "'DROP'", 
		"'UNION'", "'EXCEPT'", "'INTERSECT'", "'TO'", "'TABLESAMPLE'", "'STRATIFY'", 
		"'ALTER'", "'RENAME'", "'ARRAY'", "'MAP'", "'STRUCT'", "'COMMENT'", "'SET'", 
		"'RESET'", "'DATA'", "'START'", "'TRANSACTION'", "'COMMIT'", "'ROLLBACK'", 
		"'MACRO'", "'IF'", null, "'<=>'", "'<>'", "'!='", "'<'", null, "'>'", 
		null, "'+'", "'-'", "'*'", "'/'", "'%'", "'DIV'", "'~'", "'&'", "'|'", 
		"'^'", "'PERCENT'", "'BUCKET'", "'OUT'", "'OF'", "'SORT'", "'CLUSTER'", 
		"'DISTRIBUTE'", "'OVERWRITE'", "'TRANSFORM'", "'REDUCE'", "'USING'", "'SERDE'", 
		"'SERDEPROPERTIES'", "'RECORDREADER'", "'RECORDWRITER'", "'DELIMITED'", 
		"'FIELDS'", "'TERMINATED'", "'COLLECTION'", "'ITEMS'", "'KEYS'", "'ESCAPED'", 
		"'LINES'", "'SEPARATED'", "'FUNCTION'", "'EXTENDED'", "'REFRESH'", "'CLEAR'", 
		"'CACHE'", "'UNCACHE'", "'LAZY'", "'FORMATTED'", null, "'OPTIONS'", "'UNSET'", 
		"'TBLPROPERTIES'", "'DBPROPERTIES'", "'BUCKETS'", "'SKEWED'", "'STORED'", 
		"'DIRECTORIES'", "'LOCATION'", "'EXCHANGE'", "'ARCHIVE'", "'UNARCHIVE'", 
		"'FILEFORMAT'", "'TOUCH'", "'COMPACT'", "'CONCATENATE'", "'CHANGE'", "'CASCADE'", 
		"'RESTRICT'", "'CLUSTERED'", "'SORTED'", "'PURGE'", "'INPUTFORMAT'", "'OUTPUTFORMAT'", 
		null, null, "'DFS'", "'TRUNCATE'", "'ANALYZE'", "'COMPUTE'", "'LIST'", 
		"'STATISTICS'", "'PARTITIONED'", "'EXTERNAL'", "'DEFINED'", "'REVOKE'", 
		"'GRANT'", "'LOCK'", "'UNLOCK'", "'MSCK'", "'REPAIR'", "'EXPORT'", "'IMPORT'", 
		"'LOAD'", "'ROLE'", "'ROLES'", "'COMPACTIONS'", "'PRINCIPALS'", "'TRANSACTIONS'", 
		"'INDEX'", "'INDEXES'", "'LOCKS'", "'OPTION'", "'ANTI'", "'LOCAL'", "'INPATH'", 
		"'SINDEX'"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, null, null, null, null, null, null, null, "SELECT", "FROM", "ADD", 
		"AS", "ALL", "DISTINCT", "WHERE", "GROUP", "BY", "GROUPING", "SETS", "CUBE", 
		"ROLLUP", "ORDER", "HAVING", "LIMIT", "AT", "OR", "AND", "IN", "NOT", 
		"NO", "EXISTS", "BETWEEN", "LIKE", "RLIKE", "IS", "NULL", "TRUE", "FALSE", 
		"NULLS", "ASC", "DESC", "FOR", "INTERVAL", "CASE", "WHEN", "THEN", "ELSE", 
		"END", "JOIN", "CROSS", "OUTER", "INNER", "LEFT", "SEMI", "RIGHT", "FULL", 
		"NATURAL", "ON", "LATERAL", "WINDOW", "OVER", "PARTITION", "RANGE", "ROWS", 
		"UNBOUNDED", "PRECEDING", "FOLLOWING", "CURRENT", "ROW", "WITH", "VALUES", 
		"CREATE", "TABLE", "VIEW", "REPLACE", "INSERT", "DELETE", "INTO", "DESCRIBE", 
		"EXPLAIN", "FORMAT", "LOGICAL", "CODEGEN", "CAST", "SHOW", "TABLES", "COLUMNS", 
		"COLUMN", "USE", "PARTITIONS", "FUNCTIONS", "DROP", "UNION", "EXCEPT", 
		"INTERSECT", "TO", "TABLESAMPLE", "STRATIFY", "ALTER", "RENAME", "ARRAY", 
		"MAP", "STRUCT", "COMMENT", "SET", "RESET", "DATA", "START", "TRANSACTION", 
		"COMMIT", "ROLLBACK", "MACRO", "IF", "EQ", "NSEQ", "NEQ", "NEQJ", "LT", 
		"LTE", "GT", "GTE", "PLUS", "MINUS", "ASTERISK", "SLASH", "PERCENT", "DIV", 
		"TILDE", "AMPERSAND", "PIPE", "HAT", "PERCENTLIT", "BUCKET", "OUT", "OF", 
		"SORT", "CLUSTER", "DISTRIBUTE", "OVERWRITE", "TRANSFORM", "REDUCE", "USING", 
		"SERDE", "SERDEPROPERTIES", "RECORDREADER", "RECORDWRITER", "DELIMITED", 
		"FIELDS", "TERMINATED", "COLLECTION", "ITEMS", "KEYS", "ESCAPED", "LINES", 
		"SEPARATED", "FUNCTION", "EXTENDED", "REFRESH", "CLEAR", "CACHE", "UNCACHE", 
		"LAZY", "FORMATTED", "TEMPORARY", "OPTIONS", "UNSET", "TBLPROPERTIES", 
		"DBPROPERTIES", "BUCKETS", "SKEWED", "STORED", "DIRECTORIES", "LOCATION", 
		"EXCHANGE", "ARCHIVE", "UNARCHIVE", "FILEFORMAT", "TOUCH", "COMPACT", 
		"CONCATENATE", "CHANGE", "CASCADE", "RESTRICT", "CLUSTERED", "SORTED", 
		"PURGE", "INPUTFORMAT", "OUTPUTFORMAT", "DATABASE", "DATABASES", "DFS", 
		"TRUNCATE", "ANALYZE", "COMPUTE", "LIST", "STATISTICS", "PARTITIONED", 
		"EXTERNAL", "DEFINED", "REVOKE", "GRANT", "LOCK", "UNLOCK", "MSCK", "REPAIR", 
		"EXPORT", "IMPORT", "LOAD", "ROLE", "ROLES", "COMPACTIONS", "PRINCIPALS", 
		"TRANSACTIONS", "INDEX", "INDEXES", "LOCKS", "OPTION", "ANTI", "LOCAL", 
		"INPATH", "SINDEX", "STRING", "BIGINT_LITERAL", "SMALLINT_LITERAL", "TINYINT_LITERAL", 
		"BYTELENGTH_LITERAL", "INTEGER_VALUE", "DECIMAL_VALUE", "SCIENTIFIC_DECIMAL_VALUE", 
		"DOUBLE_LITERAL", "IDENTIFIER", "BACKQUOTED_IDENTIFIER", "SIMPLE_COMMENT", 
		"BRACKETED_COMMENT", "WS", "UNRECOGNIZED"
	};
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}


	public SqlBaseLexer(CharStream input) {
		super(input);
		_interp = new LexerATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@Override
	public String getGrammarFileName() { return "SqlBase.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public String[] getModeNames() { return modeNames; }

	@Override
	public ATN getATN() { return _ATN; }

	public static final String _serializedATN =
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\2\u00ed\u085f\b\1\4"+
		"\2\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n"+
		"\4\13\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22"+
		"\t\22\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31"+
		"\t\31\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t"+
		" \4!\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t"+
		"+\4,\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63\4\64"+
		"\t\64\4\65\t\65\4\66\t\66\4\67\t\67\48\t8\49\t9\4:\t:\4;\t;\4<\t<\4=\t"+
		"=\4>\t>\4?\t?\4@\t@\4A\tA\4B\tB\4C\tC\4D\tD\4E\tE\4F\tF\4G\tG\4H\tH\4"+
		"I\tI\4J\tJ\4K\tK\4L\tL\4M\tM\4N\tN\4O\tO\4P\tP\4Q\tQ\4R\tR\4S\tS\4T\t"+
		"T\4U\tU\4V\tV\4W\tW\4X\tX\4Y\tY\4Z\tZ\4[\t[\4\\\t\\\4]\t]\4^\t^\4_\t_"+
		"\4`\t`\4a\ta\4b\tb\4c\tc\4d\td\4e\te\4f\tf\4g\tg\4h\th\4i\ti\4j\tj\4k"+
		"\tk\4l\tl\4m\tm\4n\tn\4o\to\4p\tp\4q\tq\4r\tr\4s\ts\4t\tt\4u\tu\4v\tv"+
		"\4w\tw\4x\tx\4y\ty\4z\tz\4{\t{\4|\t|\4}\t}\4~\t~\4\177\t\177\4\u0080\t"+
		"\u0080\4\u0081\t\u0081\4\u0082\t\u0082\4\u0083\t\u0083\4\u0084\t\u0084"+
		"\4\u0085\t\u0085\4\u0086\t\u0086\4\u0087\t\u0087\4\u0088\t\u0088\4\u0089"+
		"\t\u0089\4\u008a\t\u008a\4\u008b\t\u008b\4\u008c\t\u008c\4\u008d\t\u008d"+
		"\4\u008e\t\u008e\4\u008f\t\u008f\4\u0090\t\u0090\4\u0091\t\u0091\4\u0092"+
		"\t\u0092\4\u0093\t\u0093\4\u0094\t\u0094\4\u0095\t\u0095\4\u0096\t\u0096"+
		"\4\u0097\t\u0097\4\u0098\t\u0098\4\u0099\t\u0099\4\u009a\t\u009a\4\u009b"+
		"\t\u009b\4\u009c\t\u009c\4\u009d\t\u009d\4\u009e\t\u009e\4\u009f\t\u009f"+
		"\4\u00a0\t\u00a0\4\u00a1\t\u00a1\4\u00a2\t\u00a2\4\u00a3\t\u00a3\4\u00a4"+
		"\t\u00a4\4\u00a5\t\u00a5\4\u00a6\t\u00a6\4\u00a7\t\u00a7\4\u00a8\t\u00a8"+
		"\4\u00a9\t\u00a9\4\u00aa\t\u00aa\4\u00ab\t\u00ab\4\u00ac\t\u00ac\4\u00ad"+
		"\t\u00ad\4\u00ae\t\u00ae\4\u00af\t\u00af\4\u00b0\t\u00b0\4\u00b1\t\u00b1"+
		"\4\u00b2\t\u00b2\4\u00b3\t\u00b3\4\u00b4\t\u00b4\4\u00b5\t\u00b5\4\u00b6"+
		"\t\u00b6\4\u00b7\t\u00b7\4\u00b8\t\u00b8\4\u00b9\t\u00b9\4\u00ba\t\u00ba"+
		"\4\u00bb\t\u00bb\4\u00bc\t\u00bc\4\u00bd\t\u00bd\4\u00be\t\u00be\4\u00bf"+
		"\t\u00bf\4\u00c0\t\u00c0\4\u00c1\t\u00c1\4\u00c2\t\u00c2\4\u00c3\t\u00c3"+
		"\4\u00c4\t\u00c4\4\u00c5\t\u00c5\4\u00c6\t\u00c6\4\u00c7\t\u00c7\4\u00c8"+
		"\t\u00c8\4\u00c9\t\u00c9\4\u00ca\t\u00ca\4\u00cb\t\u00cb\4\u00cc\t\u00cc"+
		"\4\u00cd\t\u00cd\4\u00ce\t\u00ce\4\u00cf\t\u00cf\4\u00d0\t\u00d0\4\u00d1"+
		"\t\u00d1\4\u00d2\t\u00d2\4\u00d3\t\u00d3\4\u00d4\t\u00d4\4\u00d5\t\u00d5"+
		"\4\u00d6\t\u00d6\4\u00d7\t\u00d7\4\u00d8\t\u00d8\4\u00d9\t\u00d9\4\u00da"+
		"\t\u00da\4\u00db\t\u00db\4\u00dc\t\u00dc\4\u00dd\t\u00dd\4\u00de\t\u00de"+
		"\4\u00df\t\u00df\4\u00e0\t\u00e0\4\u00e1\t\u00e1\4\u00e2\t\u00e2\4\u00e3"+
		"\t\u00e3\4\u00e4\t\u00e4\4\u00e5\t\u00e5\4\u00e6\t\u00e6\4\u00e7\t\u00e7"+
		"\4\u00e8\t\u00e8\4\u00e9\t\u00e9\4\u00ea\t\u00ea\4\u00eb\t\u00eb\4\u00ec"+
		"\t\u00ec\4\u00ed\t\u00ed\4\u00ee\t\u00ee\4\u00ef\t\u00ef\3\2\3\2\3\3\3"+
		"\3\3\4\3\4\3\5\3\5\3\6\3\6\3\7\3\7\3\b\3\b\3\t\3\t\3\t\3\t\3\t\3\t\3\t"+
		"\3\n\3\n\3\n\3\n\3\n\3\13\3\13\3\13\3\13\3\f\3\f\3\f\3\r\3\r\3\r\3\r\3"+
		"\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\17\3\17\3\17\3\17\3\17\3"+
		"\17\3\20\3\20\3\20\3\20\3\20\3\20\3\21\3\21\3\21\3\22\3\22\3\22\3\22\3"+
		"\22\3\22\3\22\3\22\3\22\3\23\3\23\3\23\3\23\3\23\3\24\3\24\3\24\3\24\3"+
		"\24\3\25\3\25\3\25\3\25\3\25\3\25\3\25\3\26\3\26\3\26\3\26\3\26\3\26\3"+
		"\27\3\27\3\27\3\27\3\27\3\27\3\27\3\30\3\30\3\30\3\30\3\30\3\30\3\31\3"+
		"\31\3\31\3\32\3\32\3\32\3\33\3\33\3\33\3\33\3\34\3\34\3\34\3\35\3\35\3"+
		"\35\3\35\5\35\u025b\n\35\3\36\3\36\3\36\3\37\3\37\3\37\3\37\3\37\3\37"+
		"\3\37\3 \3 \3 \3 \3 \3 \3 \3 \3!\3!\3!\3!\3!\3\"\3\"\3\"\3\"\3\"\3\"\3"+
		"\"\3\"\3\"\3\"\3\"\5\"\u027f\n\"\3#\3#\3#\3$\3$\3$\3$\3$\3%\3%\3%\3%\3"+
		"%\3&\3&\3&\3&\3&\3&\3\'\3\'\3\'\3\'\3\'\3\'\3(\3(\3(\3(\3)\3)\3)\3)\3"+
		")\3*\3*\3*\3*\3+\3+\3+\3+\3+\3+\3+\3+\3+\3,\3,\3,\3,\3,\3-\3-\3-\3-\3"+
		"-\3.\3.\3.\3.\3.\3/\3/\3/\3/\3/\3\60\3\60\3\60\3\60\3\61\3\61\3\61\3\61"+
		"\3\61\3\62\3\62\3\62\3\62\3\62\3\62\3\63\3\63\3\63\3\63\3\63\3\63\3\64"+
		"\3\64\3\64\3\64\3\64\3\64\3\65\3\65\3\65\3\65\3\65\3\66\3\66\3\66\3\66"+
		"\3\66\3\67\3\67\3\67\3\67\3\67\3\67\38\38\38\38\38\39\39\39\39\39\39\3"+
		"9\39\3:\3:\3:\3;\3;\3;\3;\3;\3;\3;\3;\3<\3<\3<\3<\3<\3<\3<\3=\3=\3=\3"+
		"=\3=\3>\3>\3>\3>\3>\3>\3>\3>\3>\3>\3?\3?\3?\3?\3?\3?\3@\3@\3@\3@\3@\3"+
		"A\3A\3A\3A\3A\3A\3A\3A\3A\3A\3B\3B\3B\3B\3B\3B\3B\3B\3B\3B\3C\3C\3C\3"+
		"C\3C\3C\3C\3C\3C\3C\3D\3D\3D\3D\3D\3D\3D\3D\3E\3E\3E\3E\3F\3F\3F\3F\3"+
		"F\3G\3G\3G\3G\3G\3G\3G\3H\3H\3H\3H\3H\3H\3H\3I\3I\3I\3I\3I\3I\3J\3J\3"+
		"J\3J\3J\3K\3K\3K\3K\3K\3K\3K\3K\3L\3L\3L\3L\3L\3L\3L\3M\3M\3M\3M\3M\3"+
		"M\3M\3N\3N\3N\3N\3N\3O\3O\3O\3O\3O\3O\3O\3O\3O\3P\3P\3P\3P\3P\3P\3P\3"+
		"P\3Q\3Q\3Q\3Q\3Q\3Q\3Q\3R\3R\3R\3R\3R\3R\3R\3R\3S\3S\3S\3S\3S\3S\3S\3"+
		"S\3T\3T\3T\3T\3T\3U\3U\3U\3U\3U\3V\3V\3V\3V\3V\3V\3V\3W\3W\3W\3W\3W\3"+
		"W\3W\3W\3X\3X\3X\3X\3X\3X\3X\3Y\3Y\3Y\3Y\3Z\3Z\3Z\3Z\3Z\3Z\3Z\3Z\3Z\3"+
		"Z\3Z\3[\3[\3[\3[\3[\3[\3[\3[\3[\3[\3\\\3\\\3\\\3\\\3\\\3]\3]\3]\3]\3]"+
		"\3]\3^\3^\3^\3^\3^\3^\3^\3_\3_\3_\3_\3_\3_\3_\3_\3_\3_\3`\3`\3`\3a\3a"+
		"\3a\3a\3a\3a\3a\3a\3a\3a\3a\3a\3b\3b\3b\3b\3b\3b\3b\3b\3b\3c\3c\3c\3c"+
		"\3c\3c\3d\3d\3d\3d\3d\3d\3d\3e\3e\3e\3e\3e\3e\3f\3f\3f\3f\3g\3g\3g\3g"+
		"\3g\3g\3g\3h\3h\3h\3h\3h\3h\3h\3h\3i\3i\3i\3i\3j\3j\3j\3j\3j\3j\3k\3k"+
		"\3k\3k\3k\3l\3l\3l\3l\3l\3l\3m\3m\3m\3m\3m\3m\3m\3m\3m\3m\3m\3m\3n\3n"+
		"\3n\3n\3n\3n\3n\3o\3o\3o\3o\3o\3o\3o\3o\3o\3p\3p\3p\3p\3p\3p\3q\3q\3q"+
		"\3r\3r\3r\5r\u0483\nr\3s\3s\3s\3s\3t\3t\3t\3u\3u\3u\3v\3v\3w\3w\3w\3w"+
		"\5w\u0495\nw\3x\3x\3y\3y\3y\3y\5y\u049d\ny\3z\3z\3{\3{\3|\3|\3}\3}\3~"+
		"\3~\3\177\3\177\3\177\3\177\3\u0080\3\u0080\3\u0081\3\u0081\3\u0082\3"+
		"\u0082\3\u0083\3\u0083\3\u0084\3\u0084\3\u0084\3\u0084\3\u0084\3\u0084"+
		"\3\u0084\3\u0084\3\u0085\3\u0085\3\u0085\3\u0085\3\u0085\3\u0085\3\u0085"+
		"\3\u0086\3\u0086\3\u0086\3\u0086\3\u0087\3\u0087\3\u0087\3\u0088\3\u0088"+
		"\3\u0088\3\u0088\3\u0088\3\u0089\3\u0089\3\u0089\3\u0089\3\u0089\3\u0089"+
		"\3\u0089\3\u0089\3\u008a\3\u008a\3\u008a\3\u008a\3\u008a\3\u008a\3\u008a"+
		"\3\u008a\3\u008a\3\u008a\3\u008a\3\u008b\3\u008b\3\u008b\3\u008b\3\u008b"+
		"\3\u008b\3\u008b\3\u008b\3\u008b\3\u008b\3\u008c\3\u008c\3\u008c\3\u008c"+
		"\3\u008c\3\u008c\3\u008c\3\u008c\3\u008c\3\u008c\3\u008d\3\u008d\3\u008d"+
		"\3\u008d\3\u008d\3\u008d\3\u008d\3\u008e\3\u008e\3\u008e\3\u008e\3\u008e"+
		"\3\u008e\3\u008f\3\u008f\3\u008f\3\u008f\3\u008f\3\u008f\3\u0090\3\u0090"+
		"\3\u0090\3\u0090\3\u0090\3\u0090\3\u0090\3\u0090\3\u0090\3\u0090\3\u0090"+
		"\3\u0090\3\u0090\3\u0090\3\u0090\3\u0090\3\u0091\3\u0091\3\u0091\3\u0091"+
		"\3\u0091\3\u0091\3\u0091\3\u0091\3\u0091\3\u0091\3\u0091\3\u0091\3\u0091"+
		"\3\u0092\3\u0092\3\u0092\3\u0092\3\u0092\3\u0092\3\u0092\3\u0092\3\u0092"+
		"\3\u0092\3\u0092\3\u0092\3\u0092\3\u0093\3\u0093\3\u0093\3\u0093\3\u0093"+
		"\3\u0093\3\u0093\3\u0093\3\u0093\3\u0093\3\u0094\3\u0094\3\u0094\3\u0094"+
		"\3\u0094\3\u0094\3\u0094\3\u0095\3\u0095\3\u0095\3\u0095\3\u0095\3\u0095"+
		"\3\u0095\3\u0095\3\u0095\3\u0095\3\u0095\3\u0096\3\u0096\3\u0096\3\u0096"+
		"\3\u0096\3\u0096\3\u0096\3\u0096\3\u0096\3\u0096\3\u0096\3\u0097\3\u0097"+
		"\3\u0097\3\u0097\3\u0097\3\u0097\3\u0098\3\u0098\3\u0098\3\u0098\3\u0098"+
		"\3\u0099\3\u0099\3\u0099\3\u0099\3\u0099\3\u0099\3\u0099\3\u0099\3\u009a"+
		"\3\u009a\3\u009a\3\u009a\3\u009a\3\u009a\3\u009b\3\u009b\3\u009b\3\u009b"+
		"\3\u009b\3\u009b\3\u009b\3\u009b\3\u009b\3\u009b\3\u009c\3\u009c\3\u009c"+
		"\3\u009c\3\u009c\3\u009c\3\u009c\3\u009c\3\u009c\3\u009d\3\u009d\3\u009d"+
		"\3\u009d\3\u009d\3\u009d\3\u009d\3\u009d\3\u009d\3\u009e\3\u009e\3\u009e"+
		"\3\u009e\3\u009e\3\u009e\3\u009e\3\u009e\3\u009f\3\u009f\3\u009f\3\u009f"+
		"\3\u009f\3\u009f\3\u00a0\3\u00a0\3\u00a0\3\u00a0\3\u00a0\3\u00a0\3\u00a1"+
		"\3\u00a1\3\u00a1\3\u00a1\3\u00a1\3\u00a1\3\u00a1\3\u00a1\3\u00a2\3\u00a2"+
		"\3\u00a2\3\u00a2\3\u00a2\3\u00a3\3\u00a3\3\u00a3\3\u00a3\3\u00a3\3\u00a3"+
		"\3\u00a3\3\u00a3\3\u00a3\3\u00a3\3\u00a4\3\u00a4\3\u00a4\3\u00a4\3\u00a4"+
		"\3\u00a4\3\u00a4\3\u00a4\3\u00a4\3\u00a4\3\u00a4\3\u00a4\3\u00a4\5\u00a4"+
		"\u05c8\n\u00a4\3\u00a5\3\u00a5\3\u00a5\3\u00a5\3\u00a5\3\u00a5\3\u00a5"+
		"\3\u00a5\3\u00a6\3\u00a6\3\u00a6\3\u00a6\3\u00a6\3\u00a6\3\u00a7\3\u00a7"+
		"\3\u00a7\3\u00a7\3\u00a7\3\u00a7\3\u00a7\3\u00a7\3\u00a7\3\u00a7\3\u00a7"+
		"\3\u00a7\3\u00a7\3\u00a7\3\u00a8\3\u00a8\3\u00a8\3\u00a8\3\u00a8\3\u00a8"+
		"\3\u00a8\3\u00a8\3\u00a8\3\u00a8\3\u00a8\3\u00a8\3\u00a8\3\u00a9\3\u00a9"+
		"\3\u00a9\3\u00a9\3\u00a9\3\u00a9\3\u00a9\3\u00a9\3\u00aa\3\u00aa\3\u00aa"+
		"\3\u00aa\3\u00aa\3\u00aa\3\u00aa\3\u00ab\3\u00ab\3\u00ab\3\u00ab\3\u00ab"+
		"\3\u00ab\3\u00ab\3\u00ac\3\u00ac\3\u00ac\3\u00ac\3\u00ac\3\u00ac\3\u00ac"+
		"\3\u00ac\3\u00ac\3\u00ac\3\u00ac\3\u00ac\3\u00ad\3\u00ad\3\u00ad\3\u00ad"+
		"\3\u00ad\3\u00ad\3\u00ad\3\u00ad\3\u00ad\3\u00ae\3\u00ae\3\u00ae\3\u00ae"+
		"\3\u00ae\3\u00ae\3\u00ae\3\u00ae\3\u00ae\3\u00af\3\u00af\3\u00af\3\u00af"+
		"\3\u00af\3\u00af\3\u00af\3\u00af\3\u00b0\3\u00b0\3\u00b0\3\u00b0\3\u00b0"+
		"\3\u00b0\3\u00b0\3\u00b0\3\u00b0\3\u00b0\3\u00b1\3\u00b1\3\u00b1\3\u00b1"+
		"\3\u00b1\3\u00b1\3\u00b1\3\u00b1\3\u00b1\3\u00b1\3\u00b1\3\u00b2\3\u00b2"+
		"\3\u00b2\3\u00b2\3\u00b2\3\u00b2\3\u00b3\3\u00b3\3\u00b3\3\u00b3\3\u00b3"+
		"\3\u00b3\3\u00b3\3\u00b3\3\u00b4\3\u00b4\3\u00b4\3\u00b4\3\u00b4\3\u00b4"+
		"\3\u00b4\3\u00b4\3\u00b4\3\u00b4\3\u00b4\3\u00b4\3\u00b5\3\u00b5\3\u00b5"+
		"\3\u00b5\3\u00b5\3\u00b5\3\u00b5\3\u00b6\3\u00b6\3\u00b6\3\u00b6\3\u00b6"+
		"\3\u00b6\3\u00b6\3\u00b6\3\u00b7\3\u00b7\3\u00b7\3\u00b7\3\u00b7\3\u00b7"+
		"\3\u00b7\3\u00b7\3\u00b7\3\u00b8\3\u00b8\3\u00b8\3\u00b8\3\u00b8\3\u00b8"+
		"\3\u00b8\3\u00b8\3\u00b8\3\u00b8\3\u00b9\3\u00b9\3\u00b9\3\u00b9\3\u00b9"+
		"\3\u00b9\3\u00b9\3\u00ba\3\u00ba\3\u00ba\3\u00ba\3\u00ba\3\u00ba\3\u00bb"+
		"\3\u00bb\3\u00bb\3\u00bb\3\u00bb\3\u00bb\3\u00bb\3\u00bb\3\u00bb\3\u00bb"+
		"\3\u00bb\3\u00bb\3\u00bc\3\u00bc\3\u00bc\3\u00bc\3\u00bc\3\u00bc\3\u00bc"+
		"\3\u00bc\3\u00bc\3\u00bc\3\u00bc\3\u00bc\3\u00bc\3\u00bd\3\u00bd\3\u00bd"+
		"\3\u00bd\3\u00bd\3\u00bd\3\u00bd\3\u00bd\3\u00bd\3\u00bd\3\u00bd\3\u00bd"+
		"\3\u00bd\3\u00bd\5\u00bd\u06b4\n\u00bd\3\u00be\3\u00be\3\u00be\3\u00be"+
		"\3\u00be\3\u00be\3\u00be\3\u00be\3\u00be\3\u00be\3\u00be\3\u00be\3\u00be"+
		"\3\u00be\3\u00be\3\u00be\5\u00be\u06c6\n\u00be\3\u00bf\3\u00bf\3\u00bf"+
		"\3\u00bf\3\u00c0\3\u00c0\3\u00c0\3\u00c0\3\u00c0\3\u00c0\3\u00c0\3\u00c0"+
		"\3\u00c0\3\u00c1\3\u00c1\3\u00c1\3\u00c1\3\u00c1\3\u00c1\3\u00c1\3\u00c1"+
		"\3\u00c2\3\u00c2\3\u00c2\3\u00c2\3\u00c2\3\u00c2\3\u00c2\3\u00c2\3\u00c3"+
		"\3\u00c3\3\u00c3\3\u00c3\3\u00c3\3\u00c4\3\u00c4\3\u00c4\3\u00c4\3\u00c4"+
		"\3\u00c4\3\u00c4\3\u00c4\3\u00c4\3\u00c4\3\u00c4\3\u00c5\3\u00c5\3\u00c5"+
		"\3\u00c5\3\u00c5\3\u00c5\3\u00c5\3\u00c5\3\u00c5\3\u00c5\3\u00c5\3\u00c5"+
		"\3\u00c6\3\u00c6\3\u00c6\3\u00c6\3\u00c6\3\u00c6\3\u00c6\3\u00c6\3\u00c6"+
		"\3\u00c7\3\u00c7\3\u00c7\3\u00c7\3\u00c7\3\u00c7\3\u00c7\3\u00c7\3\u00c8"+
		"\3\u00c8\3\u00c8\3\u00c8\3\u00c8\3\u00c8\3\u00c8\3\u00c9\3\u00c9\3\u00c9"+
		"\3\u00c9\3\u00c9\3\u00c9\3\u00ca\3\u00ca\3\u00ca\3\u00ca\3\u00ca\3\u00cb"+
		"\3\u00cb\3\u00cb\3\u00cb\3\u00cb\3\u00cb\3\u00cb\3\u00cc\3\u00cc\3\u00cc"+
		"\3\u00cc\3\u00cc\3\u00cd\3\u00cd\3\u00cd\3\u00cd\3\u00cd\3\u00cd\3\u00cd"+
		"\3\u00ce\3\u00ce\3\u00ce\3\u00ce\3\u00ce\3\u00ce\3\u00ce\3\u00cf\3\u00cf"+
		"\3\u00cf\3\u00cf\3\u00cf\3\u00cf\3\u00cf\3\u00d0\3\u00d0\3\u00d0\3\u00d0"+
		"\3\u00d0\3\u00d1\3\u00d1\3\u00d1\3\u00d1\3\u00d1\3\u00d2\3\u00d2\3\u00d2"+
		"\3\u00d2\3\u00d2\3\u00d2\3\u00d3\3\u00d3\3\u00d3\3\u00d3\3\u00d3\3\u00d3"+
		"\3\u00d3\3\u00d3\3\u00d3\3\u00d3\3\u00d3\3\u00d3\3\u00d4\3\u00d4\3\u00d4"+
		"\3\u00d4\3\u00d4\3\u00d4\3\u00d4\3\u00d4\3\u00d4\3\u00d4\3\u00d4\3\u00d5"+
		"\3\u00d5\3\u00d5\3\u00d5\3\u00d5\3\u00d5\3\u00d5\3\u00d5\3\u00d5\3\u00d5"+
		"\3\u00d5\3\u00d5\3\u00d5\3\u00d6\3\u00d6\3\u00d6\3\u00d6\3\u00d6\3\u00d6"+
		"\3\u00d7\3\u00d7\3\u00d7\3\u00d7\3\u00d7\3\u00d7\3\u00d7\3\u00d7\3\u00d8"+
		"\3\u00d8\3\u00d8\3\u00d8\3\u00d8\3\u00d8\3\u00d9\3\u00d9\3\u00d9\3\u00d9"+
		"\3\u00d9\3\u00d9\3\u00d9\3\u00da\3\u00da\3\u00da\3\u00da\3\u00da\3\u00db"+
		"\3\u00db\3\u00db\3\u00db\3\u00db\3\u00db\3\u00dc\3\u00dc\3\u00dc\3\u00dc"+
		"\3\u00dc\3\u00dc\3\u00dc\3\u00dd\3\u00dd\3\u00dd\3\u00dd\3\u00dd\3\u00dd"+
		"\3\u00dd\3\u00de\3\u00de\3\u00de\3\u00de\7\u00de\u07b1\n\u00de\f\u00de"+
		"\16\u00de\u07b4\13\u00de\3\u00de\3\u00de\3\u00de\3\u00de\3\u00de\7\u00de"+
		"\u07bb\n\u00de\f\u00de\16\u00de\u07be\13\u00de\3\u00de\5\u00de\u07c1\n"+
		"\u00de\3\u00df\6\u00df\u07c4\n\u00df\r\u00df\16\u00df\u07c5\3\u00df\3"+
		"\u00df\3\u00e0\6\u00e0\u07cb\n\u00e0\r\u00e0\16\u00e0\u07cc\3\u00e0\3"+
		"\u00e0\3\u00e1\6\u00e1\u07d2\n\u00e1\r\u00e1\16\u00e1\u07d3\3\u00e1\3"+
		"\u00e1\3\u00e2\6\u00e2\u07d9\n\u00e2\r\u00e2\16\u00e2\u07da\3\u00e2\3"+
		"\u00e2\3\u00e3\6\u00e3\u07e0\n\u00e3\r\u00e3\16\u00e3\u07e1\3\u00e4\6"+
		"\u00e4\u07e5\n\u00e4\r\u00e4\16\u00e4\u07e6\3\u00e4\3\u00e4\7\u00e4\u07eb"+
		"\n\u00e4\f\u00e4\16\u00e4\u07ee\13\u00e4\3\u00e4\3\u00e4\6\u00e4\u07f2"+
		"\n\u00e4\r\u00e4\16\u00e4\u07f3\5\u00e4\u07f6\n\u00e4\3\u00e5\6\u00e5"+
		"\u07f9\n\u00e5\r\u00e5\16\u00e5\u07fa\3\u00e5\3\u00e5\7\u00e5\u07ff\n"+
		"\u00e5\f\u00e5\16\u00e5\u0802\13\u00e5\5\u00e5\u0804\n\u00e5\3\u00e5\3"+
		"\u00e5\3\u00e5\3\u00e5\6\u00e5\u080a\n\u00e5\r\u00e5\16\u00e5\u080b\3"+
		"\u00e5\3\u00e5\5\u00e5\u0810\n\u00e5\3\u00e6\3\u00e6\3\u00e6\5\u00e6\u0815"+
		"\n\u00e6\3\u00e6\3\u00e6\3\u00e7\3\u00e7\3\u00e7\6\u00e7\u081c\n\u00e7"+
		"\r\u00e7\16\u00e7\u081d\3\u00e8\3\u00e8\3\u00e8\3\u00e8\7\u00e8\u0824"+
		"\n\u00e8\f\u00e8\16\u00e8\u0827\13\u00e8\3\u00e8\3\u00e8\3\u00e9\3\u00e9"+
		"\5\u00e9\u082d\n\u00e9\3\u00e9\6\u00e9\u0830\n\u00e9\r\u00e9\16\u00e9"+
		"\u0831\3\u00ea\3\u00ea\3\u00eb\3\u00eb\3\u00ec\3\u00ec\3\u00ec\3\u00ec"+
		"\7\u00ec\u083c\n\u00ec\f\u00ec\16\u00ec\u083f\13\u00ec\3\u00ec\5\u00ec"+
		"\u0842\n\u00ec\3\u00ec\5\u00ec\u0845\n\u00ec\3\u00ec\3\u00ec\3\u00ed\3"+
		"\u00ed\3\u00ed\3\u00ed\7\u00ed\u084d\n\u00ed\f\u00ed\16\u00ed\u0850\13"+
		"\u00ed\3\u00ed\3\u00ed\3\u00ed\3\u00ed\3\u00ed\3\u00ee\6\u00ee\u0858\n"+
		"\u00ee\r\u00ee\16\u00ee\u0859\3\u00ee\3\u00ee\3\u00ef\3\u00ef\3\u084e"+
		"\2\u00f0\3\3\5\4\7\5\t\6\13\7\r\b\17\t\21\n\23\13\25\f\27\r\31\16\33\17"+
		"\35\20\37\21!\22#\23%\24\'\25)\26+\27-\30/\31\61\32\63\33\65\34\67\35"+
		"9\36;\37= ?!A\"C#E$G%I&K\'M(O)Q*S+U,W-Y.[/]\60_\61a\62c\63e\64g\65i\66"+
		"k\67m8o9q:s;u<w=y>{?}@\177A\u0081B\u0083C\u0085D\u0087E\u0089F\u008bG"+
		"\u008dH\u008fI\u0091J\u0093K\u0095L\u0097M\u0099N\u009bO\u009dP\u009f"+
		"Q\u00a1R\u00a3S\u00a5T\u00a7U\u00a9V\u00abW\u00adX\u00afY\u00b1Z\u00b3"+
		"[\u00b5\\\u00b7]\u00b9^\u00bb_\u00bd`\u00bfa\u00c1b\u00c3c\u00c5d\u00c7"+
		"e\u00c9f\u00cbg\u00cdh\u00cfi\u00d1j\u00d3k\u00d5l\u00d7m\u00d9n\u00db"+
		"o\u00ddp\u00dfq\u00e1r\u00e3s\u00e5t\u00e7u\u00e9v\u00ebw\u00edx\u00ef"+
		"y\u00f1z\u00f3{\u00f5|\u00f7}\u00f9~\u00fb\177\u00fd\u0080\u00ff\u0081"+
		"\u0101\u0082\u0103\u0083\u0105\u0084\u0107\u0085\u0109\u0086\u010b\u0087"+
		"\u010d\u0088\u010f\u0089\u0111\u008a\u0113\u008b\u0115\u008c\u0117\u008d"+
		"\u0119\u008e\u011b\u008f\u011d\u0090\u011f\u0091\u0121\u0092\u0123\u0093"+
		"\u0125\u0094\u0127\u0095\u0129\u0096\u012b\u0097\u012d\u0098\u012f\u0099"+
		"\u0131\u009a\u0133\u009b\u0135\u009c\u0137\u009d\u0139\u009e\u013b\u009f"+
		"\u013d\u00a0\u013f\u00a1\u0141\u00a2\u0143\u00a3\u0145\u00a4\u0147\u00a5"+
		"\u0149\u00a6\u014b\u00a7\u014d\u00a8\u014f\u00a9\u0151\u00aa\u0153\u00ab"+
		"\u0155\u00ac\u0157\u00ad\u0159\u00ae\u015b\u00af\u015d\u00b0\u015f\u00b1"+
		"\u0161\u00b2\u0163\u00b3\u0165\u00b4\u0167\u00b5\u0169\u00b6\u016b\u00b7"+
		"\u016d\u00b8\u016f\u00b9\u0171\u00ba\u0173\u00bb\u0175\u00bc\u0177\u00bd"+
		"\u0179\u00be\u017b\u00bf\u017d\u00c0\u017f\u00c1\u0181\u00c2\u0183\u00c3"+
		"\u0185\u00c4\u0187\u00c5\u0189\u00c6\u018b\u00c7\u018d\u00c8\u018f\u00c9"+
		"\u0191\u00ca\u0193\u00cb\u0195\u00cc\u0197\u00cd\u0199\u00ce\u019b\u00cf"+
		"\u019d\u00d0\u019f\u00d1\u01a1\u00d2\u01a3\u00d3\u01a5\u00d4\u01a7\u00d5"+
		"\u01a9\u00d6\u01ab\u00d7\u01ad\u00d8\u01af\u00d9\u01b1\u00da\u01b3\u00db"+
		"\u01b5\u00dc\u01b7\u00dd\u01b9\u00de\u01bb\u00df\u01bd\u00e0\u01bf\u00e1"+
		"\u01c1\u00e2\u01c3\u00e3\u01c5\u00e4\u01c7\u00e5\u01c9\u00e6\u01cb\u00e7"+
		"\u01cd\u00e8\u01cf\u00e9\u01d1\2\u01d3\2\u01d5\2\u01d7\u00ea\u01d9\u00eb"+
		"\u01db\u00ec\u01dd\u00ed\3\2\13\4\2))^^\4\2$$^^\6\2DDIIMMOO\3\2bb\4\2"+
		"--//\3\2\62;\3\2C\\\4\2\f\f\17\17\5\2\13\f\17\17\"\"\u0884\2\3\3\2\2\2"+
		"\2\5\3\2\2\2\2\7\3\2\2\2\2\t\3\2\2\2\2\13\3\2\2\2\2\r\3\2\2\2\2\17\3\2"+
		"\2\2\2\21\3\2\2\2\2\23\3\2\2\2\2\25\3\2\2\2\2\27\3\2\2\2\2\31\3\2\2\2"+
		"\2\33\3\2\2\2\2\35\3\2\2\2\2\37\3\2\2\2\2!\3\2\2\2\2#\3\2\2\2\2%\3\2\2"+
		"\2\2\'\3\2\2\2\2)\3\2\2\2\2+\3\2\2\2\2-\3\2\2\2\2/\3\2\2\2\2\61\3\2\2"+
		"\2\2\63\3\2\2\2\2\65\3\2\2\2\2\67\3\2\2\2\29\3\2\2\2\2;\3\2\2\2\2=\3\2"+
		"\2\2\2?\3\2\2\2\2A\3\2\2\2\2C\3\2\2\2\2E\3\2\2\2\2G\3\2\2\2\2I\3\2\2\2"+
		"\2K\3\2\2\2\2M\3\2\2\2\2O\3\2\2\2\2Q\3\2\2\2\2S\3\2\2\2\2U\3\2\2\2\2W"+
		"\3\2\2\2\2Y\3\2\2\2\2[\3\2\2\2\2]\3\2\2\2\2_\3\2\2\2\2a\3\2\2\2\2c\3\2"+
		"\2\2\2e\3\2\2\2\2g\3\2\2\2\2i\3\2\2\2\2k\3\2\2\2\2m\3\2\2\2\2o\3\2\2\2"+
		"\2q\3\2\2\2\2s\3\2\2\2\2u\3\2\2\2\2w\3\2\2\2\2y\3\2\2\2\2{\3\2\2\2\2}"+
		"\3\2\2\2\2\177\3\2\2\2\2\u0081\3\2\2\2\2\u0083\3\2\2\2\2\u0085\3\2\2\2"+
		"\2\u0087\3\2\2\2\2\u0089\3\2\2\2\2\u008b\3\2\2\2\2\u008d\3\2\2\2\2\u008f"+
		"\3\2\2\2\2\u0091\3\2\2\2\2\u0093\3\2\2\2\2\u0095\3\2\2\2\2\u0097\3\2\2"+
		"\2\2\u0099\3\2\2\2\2\u009b\3\2\2\2\2\u009d\3\2\2\2\2\u009f\3\2\2\2\2\u00a1"+
		"\3\2\2\2\2\u00a3\3\2\2\2\2\u00a5\3\2\2\2\2\u00a7\3\2\2\2\2\u00a9\3\2\2"+
		"\2\2\u00ab\3\2\2\2\2\u00ad\3\2\2\2\2\u00af\3\2\2\2\2\u00b1\3\2\2\2\2\u00b3"+
		"\3\2\2\2\2\u00b5\3\2\2\2\2\u00b7\3\2\2\2\2\u00b9\3\2\2\2\2\u00bb\3\2\2"+
		"\2\2\u00bd\3\2\2\2\2\u00bf\3\2\2\2\2\u00c1\3\2\2\2\2\u00c3\3\2\2\2\2\u00c5"+
		"\3\2\2\2\2\u00c7\3\2\2\2\2\u00c9\3\2\2\2\2\u00cb\3\2\2\2\2\u00cd\3\2\2"+
		"\2\2\u00cf\3\2\2\2\2\u00d1\3\2\2\2\2\u00d3\3\2\2\2\2\u00d5\3\2\2\2\2\u00d7"+
		"\3\2\2\2\2\u00d9\3\2\2\2\2\u00db\3\2\2\2\2\u00dd\3\2\2\2\2\u00df\3\2\2"+
		"\2\2\u00e1\3\2\2\2\2\u00e3\3\2\2\2\2\u00e5\3\2\2\2\2\u00e7\3\2\2\2\2\u00e9"+
		"\3\2\2\2\2\u00eb\3\2\2\2\2\u00ed\3\2\2\2\2\u00ef\3\2\2\2\2\u00f1\3\2\2"+
		"\2\2\u00f3\3\2\2\2\2\u00f5\3\2\2\2\2\u00f7\3\2\2\2\2\u00f9\3\2\2\2\2\u00fb"+
		"\3\2\2\2\2\u00fd\3\2\2\2\2\u00ff\3\2\2\2\2\u0101\3\2\2\2\2\u0103\3\2\2"+
		"\2\2\u0105\3\2\2\2\2\u0107\3\2\2\2\2\u0109\3\2\2\2\2\u010b\3\2\2\2\2\u010d"+
		"\3\2\2\2\2\u010f\3\2\2\2\2\u0111\3\2\2\2\2\u0113\3\2\2\2\2\u0115\3\2\2"+
		"\2\2\u0117\3\2\2\2\2\u0119\3\2\2\2\2\u011b\3\2\2\2\2\u011d\3\2\2\2\2\u011f"+
		"\3\2\2\2\2\u0121\3\2\2\2\2\u0123\3\2\2\2\2\u0125\3\2\2\2\2\u0127\3\2\2"+
		"\2\2\u0129\3\2\2\2\2\u012b\3\2\2\2\2\u012d\3\2\2\2\2\u012f\3\2\2\2\2\u0131"+
		"\3\2\2\2\2\u0133\3\2\2\2\2\u0135\3\2\2\2\2\u0137\3\2\2\2\2\u0139\3\2\2"+
		"\2\2\u013b\3\2\2\2\2\u013d\3\2\2\2\2\u013f\3\2\2\2\2\u0141\3\2\2\2\2\u0143"+
		"\3\2\2\2\2\u0145\3\2\2\2\2\u0147\3\2\2\2\2\u0149\3\2\2\2\2\u014b\3\2\2"+
		"\2\2\u014d\3\2\2\2\2\u014f\3\2\2\2\2\u0151\3\2\2\2\2\u0153\3\2\2\2\2\u0155"+
		"\3\2\2\2\2\u0157\3\2\2\2\2\u0159\3\2\2\2\2\u015b\3\2\2\2\2\u015d\3\2\2"+
		"\2\2\u015f\3\2\2\2\2\u0161\3\2\2\2\2\u0163\3\2\2\2\2\u0165\3\2\2\2\2\u0167"+
		"\3\2\2\2\2\u0169\3\2\2\2\2\u016b\3\2\2\2\2\u016d\3\2\2\2\2\u016f\3\2\2"+
		"\2\2\u0171\3\2\2\2\2\u0173\3\2\2\2\2\u0175\3\2\2\2\2\u0177\3\2\2\2\2\u0179"+
		"\3\2\2\2\2\u017b\3\2\2\2\2\u017d\3\2\2\2\2\u017f\3\2\2\2\2\u0181\3\2\2"+
		"\2\2\u0183\3\2\2\2\2\u0185\3\2\2\2\2\u0187\3\2\2\2\2\u0189\3\2\2\2\2\u018b"+
		"\3\2\2\2\2\u018d\3\2\2\2\2\u018f\3\2\2\2\2\u0191\3\2\2\2\2\u0193\3\2\2"+
		"\2\2\u0195\3\2\2\2\2\u0197\3\2\2\2\2\u0199\3\2\2\2\2\u019b\3\2\2\2\2\u019d"+
		"\3\2\2\2\2\u019f\3\2\2\2\2\u01a1\3\2\2\2\2\u01a3\3\2\2\2\2\u01a5\3\2\2"+
		"\2\2\u01a7\3\2\2\2\2\u01a9\3\2\2\2\2\u01ab\3\2\2\2\2\u01ad\3\2\2\2\2\u01af"+
		"\3\2\2\2\2\u01b1\3\2\2\2\2\u01b3\3\2\2\2\2\u01b5\3\2\2\2\2\u01b7\3\2\2"+
		"\2\2\u01b9\3\2\2\2\2\u01bb\3\2\2\2\2\u01bd\3\2\2\2\2\u01bf\3\2\2\2\2\u01c1"+
		"\3\2\2\2\2\u01c3\3\2\2\2\2\u01c5\3\2\2\2\2\u01c7\3\2\2\2\2\u01c9\3\2\2"+
		"\2\2\u01cb\3\2\2\2\2\u01cd\3\2\2\2\2\u01cf\3\2\2\2\2\u01d7\3\2\2\2\2\u01d9"+
		"\3\2\2\2\2\u01db\3\2\2\2\2\u01dd\3\2\2\2\3\u01df\3\2\2\2\5\u01e1\3\2\2"+
		"\2\7\u01e3\3\2\2\2\t\u01e5\3\2\2\2\13\u01e7\3\2\2\2\r\u01e9\3\2\2\2\17"+
		"\u01eb\3\2\2\2\21\u01ed\3\2\2\2\23\u01f4\3\2\2\2\25\u01f9\3\2\2\2\27\u01fd"+
		"\3\2\2\2\31\u0200\3\2\2\2\33\u0204\3\2\2\2\35\u020d\3\2\2\2\37\u0213\3"+
		"\2\2\2!\u0219\3\2\2\2#\u021c\3\2\2\2%\u0225\3\2\2\2\'\u022a\3\2\2\2)\u022f"+
		"\3\2\2\2+\u0236\3\2\2\2-\u023c\3\2\2\2/\u0243\3\2\2\2\61\u0249\3\2\2\2"+
		"\63\u024c\3\2\2\2\65\u024f\3\2\2\2\67\u0253\3\2\2\29\u025a\3\2\2\2;\u025c"+
		"\3\2\2\2=\u025f\3\2\2\2?\u0266\3\2\2\2A\u026e\3\2\2\2C\u027e\3\2\2\2E"+
		"\u0280\3\2\2\2G\u0283\3\2\2\2I\u0288\3\2\2\2K\u028d\3\2\2\2M\u0293\3\2"+
		"\2\2O\u0299\3\2\2\2Q\u029d\3\2\2\2S\u02a2\3\2\2\2U\u02a6\3\2\2\2W\u02af"+
		"\3\2\2\2Y\u02b4\3\2\2\2[\u02b9\3\2\2\2]\u02be\3\2\2\2_\u02c3\3\2\2\2a"+
		"\u02c7\3\2\2\2c\u02cc\3\2\2\2e\u02d2\3\2\2\2g\u02d8\3\2\2\2i\u02de\3\2"+
		"\2\2k\u02e3\3\2\2\2m\u02e8\3\2\2\2o\u02ee\3\2\2\2q\u02f3\3\2\2\2s\u02fb"+
		"\3\2\2\2u\u02fe\3\2\2\2w\u0306\3\2\2\2y\u030d\3\2\2\2{\u0312\3\2\2\2}"+
		"\u031c\3\2\2\2\177\u0322\3\2\2\2\u0081\u0327\3\2\2\2\u0083\u0331\3\2\2"+
		"\2\u0085\u033b\3\2\2\2\u0087\u0345\3\2\2\2\u0089\u034d\3\2\2\2\u008b\u0351"+
		"\3\2\2\2\u008d\u0356\3\2\2\2\u008f\u035d\3\2\2\2\u0091\u0364\3\2\2\2\u0093"+
		"\u036a\3\2\2\2\u0095\u036f\3\2\2\2\u0097\u0377\3\2\2\2\u0099\u037e\3\2"+
		"\2\2\u009b\u0385\3\2\2\2\u009d\u038a\3\2\2\2\u009f\u0393\3\2\2\2\u00a1"+
		"\u039b\3\2\2\2\u00a3\u03a2\3\2\2\2\u00a5\u03aa\3\2\2\2\u00a7\u03b2\3\2"+
		"\2\2\u00a9\u03b7\3\2\2\2\u00ab\u03bc\3\2\2\2\u00ad\u03c3\3\2\2\2\u00af"+
		"\u03cb\3\2\2\2\u00b1\u03d2\3\2\2\2\u00b3\u03d6\3\2\2\2\u00b5\u03e1\3\2"+
		"\2\2\u00b7\u03eb\3\2\2\2\u00b9\u03f0\3\2\2\2\u00bb\u03f6\3\2\2\2\u00bd"+
		"\u03fd\3\2\2\2\u00bf\u0407\3\2\2\2\u00c1\u040a\3\2\2\2\u00c3\u0416\3\2"+
		"\2\2\u00c5\u041f\3\2\2\2\u00c7\u0425\3\2\2\2\u00c9\u042c\3\2\2\2\u00cb"+
		"\u0432\3\2\2\2\u00cd\u0436\3\2\2\2\u00cf\u043d\3\2\2\2\u00d1\u0445\3\2"+
		"\2\2\u00d3\u0449\3\2\2\2\u00d5\u044f\3\2\2\2\u00d7\u0454\3\2\2\2\u00d9"+
		"\u045a\3\2\2\2\u00db\u0466\3\2\2\2\u00dd\u046d\3\2\2\2\u00df\u0476\3\2"+
		"\2\2\u00e1\u047c\3\2\2\2\u00e3\u0482\3\2\2\2\u00e5\u0484\3\2\2\2\u00e7"+
		"\u0488\3\2\2\2\u00e9\u048b\3\2\2\2\u00eb\u048e\3\2\2\2\u00ed\u0494\3\2"+
		"\2\2\u00ef\u0496\3\2\2\2\u00f1\u049c\3\2\2\2\u00f3\u049e\3\2\2\2\u00f5"+
		"\u04a0\3\2\2\2\u00f7\u04a2\3\2\2\2\u00f9\u04a4\3\2\2\2\u00fb\u04a6\3\2"+
		"\2\2\u00fd\u04a8\3\2\2\2\u00ff\u04ac\3\2\2\2\u0101\u04ae\3\2\2\2\u0103"+
		"\u04b0\3\2\2\2\u0105\u04b2\3\2\2\2\u0107\u04b4\3\2\2\2\u0109\u04bc\3\2"+
		"\2\2\u010b\u04c3\3\2\2\2\u010d\u04c7\3\2\2\2\u010f\u04ca\3\2\2\2\u0111"+
		"\u04cf\3\2\2\2\u0113\u04d7\3\2\2\2\u0115\u04e2\3\2\2\2\u0117\u04ec\3\2"+
		"\2\2\u0119\u04f6\3\2\2\2\u011b\u04fd\3\2\2\2\u011d\u0503\3\2\2\2\u011f"+
		"\u0509\3\2\2\2\u0121\u0519\3\2\2\2\u0123\u0526\3\2\2\2\u0125\u0533\3\2"+
		"\2\2\u0127\u053d\3\2\2\2\u0129\u0544\3\2\2\2\u012b\u054f\3\2\2\2\u012d"+
		"\u055a\3\2\2\2\u012f\u0560\3\2\2\2\u0131\u0565\3\2\2\2\u0133\u056d\3\2"+
		"\2\2\u0135\u0573\3\2\2\2\u0137\u057d\3\2\2\2\u0139\u0586\3\2\2\2\u013b"+
		"\u058f\3\2\2\2\u013d\u0597\3\2\2\2\u013f\u059d\3\2\2\2\u0141\u05a3\3\2"+
		"\2\2\u0143\u05ab\3\2\2\2\u0145\u05b0\3\2\2\2\u0147\u05c7\3\2\2\2\u0149"+
		"\u05c9\3\2\2\2\u014b\u05d1\3\2\2\2\u014d\u05d7\3\2\2\2\u014f\u05e5\3\2"+
		"\2\2\u0151\u05f2\3\2\2\2\u0153\u05fa\3\2\2\2\u0155\u0601\3\2\2\2\u0157"+
		"\u0608\3\2\2\2\u0159\u0614\3\2\2\2\u015b\u061d\3\2\2\2\u015d\u0626\3\2"+
		"\2\2\u015f\u062e\3\2\2\2\u0161\u0638\3\2\2\2\u0163\u0643\3\2\2\2\u0165"+
		"\u0649\3\2\2\2\u0167\u0651\3\2\2\2\u0169\u065d\3\2\2\2\u016b\u0664\3\2"+
		"\2\2\u016d\u066c\3\2\2\2\u016f\u0675\3\2\2\2\u0171\u067f\3\2\2\2\u0173"+
		"\u0686\3\2\2\2\u0175\u068c\3\2\2\2\u0177\u0698\3\2\2\2\u0179\u06b3\3\2"+
		"\2\2\u017b\u06c5\3\2\2\2\u017d\u06c7\3\2\2\2\u017f\u06cb\3\2\2\2\u0181"+
		"\u06d4\3\2\2\2\u0183\u06dc\3\2\2\2\u0185\u06e4\3\2\2\2\u0187\u06e9\3\2"+
		"\2\2\u0189\u06f4\3\2\2\2\u018b\u0700\3\2\2\2\u018d\u0709\3\2\2\2\u018f"+
		"\u0711\3\2\2\2\u0191\u0718\3\2\2\2\u0193\u071e\3\2\2\2\u0195\u0723\3\2"+
		"\2\2\u0197\u072a\3\2\2\2\u0199\u072f\3\2\2\2\u019b\u0736\3\2\2\2\u019d"+
		"\u073d\3\2\2\2\u019f\u0744\3\2\2\2\u01a1\u0749\3\2\2\2\u01a3\u074e\3\2"+
		"\2\2\u01a5\u0754\3\2\2\2\u01a7\u0760\3\2\2\2\u01a9\u076b\3\2\2\2\u01ab"+
		"\u0778\3\2\2\2\u01ad\u077e\3\2\2\2\u01af\u0786\3\2\2\2\u01b1\u078c\3\2"+
		"\2\2\u01b3\u0793\3\2\2\2\u01b5\u0798\3\2\2\2\u01b7\u079e\3\2\2\2\u01b9"+
		"\u07a5\3\2\2\2\u01bb\u07c0\3\2\2\2\u01bd\u07c3\3\2\2\2\u01bf\u07ca\3\2"+
		"\2\2\u01c1\u07d1\3\2\2\2\u01c3\u07d8\3\2\2\2\u01c5\u07df\3\2\2\2\u01c7"+
		"\u07f5\3\2\2\2\u01c9\u080f\3\2\2\2\u01cb\u0814\3\2\2\2\u01cd\u081b\3\2"+
		"\2\2\u01cf\u081f\3\2\2\2\u01d1\u082a\3\2\2\2\u01d3\u0833\3\2\2\2\u01d5"+
		"\u0835\3\2\2\2\u01d7\u0837\3\2\2\2\u01d9\u0848\3\2\2\2\u01db\u0857\3\2"+
		"\2\2\u01dd\u085d\3\2\2\2\u01df\u01e0\7*\2\2\u01e0\4\3\2\2\2\u01e1\u01e2"+
		"\7+\2\2\u01e2\6\3\2\2\2\u01e3\u01e4\7.\2\2\u01e4\b\3\2\2\2\u01e5\u01e6"+
		"\7\60\2\2\u01e6\n\3\2\2\2\u01e7\u01e8\7]\2\2\u01e8\f\3\2\2\2\u01e9\u01ea"+
		"\7_\2\2\u01ea\16\3\2\2\2\u01eb\u01ec\7<\2\2\u01ec\20\3\2\2\2\u01ed\u01ee"+
		"\7U\2\2\u01ee\u01ef\7G\2\2\u01ef\u01f0\7N\2\2\u01f0\u01f1\7G\2\2\u01f1"+
		"\u01f2\7E\2\2\u01f2\u01f3\7V\2\2\u01f3\22\3\2\2\2\u01f4\u01f5\7H\2\2\u01f5"+
		"\u01f6\7T\2\2\u01f6\u01f7\7Q\2\2\u01f7\u01f8\7O\2\2\u01f8\24\3\2\2\2\u01f9"+
		"\u01fa\7C\2\2\u01fa\u01fb\7F\2\2\u01fb\u01fc\7F\2\2\u01fc\26\3\2\2\2\u01fd"+
		"\u01fe\7C\2\2\u01fe\u01ff\7U\2\2\u01ff\30\3\2\2\2\u0200\u0201\7C\2\2\u0201"+
		"\u0202\7N\2\2\u0202\u0203\7N\2\2\u0203\32\3\2\2\2\u0204\u0205\7F\2\2\u0205"+
		"\u0206\7K\2\2\u0206\u0207\7U\2\2\u0207\u0208\7V\2\2\u0208\u0209\7K\2\2"+
		"\u0209\u020a\7P\2\2\u020a\u020b\7E\2\2\u020b\u020c\7V\2\2\u020c\34\3\2"+
		"\2\2\u020d\u020e\7Y\2\2\u020e\u020f\7J\2\2\u020f\u0210\7G\2\2\u0210\u0211"+
		"\7T\2\2\u0211\u0212\7G\2\2\u0212\36\3\2\2\2\u0213\u0214\7I\2\2\u0214\u0215"+
		"\7T\2\2\u0215\u0216\7Q\2\2\u0216\u0217\7W\2\2\u0217\u0218\7R\2\2\u0218"+
		" \3\2\2\2\u0219\u021a\7D\2\2\u021a\u021b\7[\2\2\u021b\"\3\2\2\2\u021c"+
		"\u021d\7I\2\2\u021d\u021e\7T\2\2\u021e\u021f\7Q\2\2\u021f\u0220\7W\2\2"+
		"\u0220\u0221\7R\2\2\u0221\u0222\7K\2\2\u0222\u0223\7P\2\2\u0223\u0224"+
		"\7I\2\2\u0224$\3\2\2\2\u0225\u0226\7U\2\2\u0226\u0227\7G\2\2\u0227\u0228"+
		"\7V\2\2\u0228\u0229\7U\2\2\u0229&\3\2\2\2\u022a\u022b\7E\2\2\u022b\u022c"+
		"\7W\2\2\u022c\u022d\7D\2\2\u022d\u022e\7G\2\2\u022e(\3\2\2\2\u022f\u0230"+
		"\7T\2\2\u0230\u0231\7Q\2\2\u0231\u0232\7N\2\2\u0232\u0233\7N\2\2\u0233"+
		"\u0234\7W\2\2\u0234\u0235\7R\2\2\u0235*\3\2\2\2\u0236\u0237\7Q\2\2\u0237"+
		"\u0238\7T\2\2\u0238\u0239\7F\2\2\u0239\u023a\7G\2\2\u023a\u023b\7T\2\2"+
		"\u023b,\3\2\2\2\u023c\u023d\7J\2\2\u023d\u023e\7C\2\2\u023e\u023f\7X\2"+
		"\2\u023f\u0240\7K\2\2\u0240\u0241\7P\2\2\u0241\u0242\7I\2\2\u0242.\3\2"+
		"\2\2\u0243\u0244\7N\2\2\u0244\u0245\7K\2\2\u0245\u0246\7O\2\2\u0246\u0247"+
		"\7K\2\2\u0247\u0248\7V\2\2\u0248\60\3\2\2\2\u0249\u024a\7C\2\2\u024a\u024b"+
		"\7V\2\2\u024b\62\3\2\2\2\u024c\u024d\7Q\2\2\u024d\u024e\7T\2\2\u024e\64"+
		"\3\2\2\2\u024f\u0250\7C\2\2\u0250\u0251\7P\2\2\u0251\u0252\7F\2\2\u0252"+
		"\66\3\2\2\2\u0253\u0254\7K\2\2\u0254\u0255\7P\2\2\u02558\3\2\2\2\u0256"+
		"\u0257\7P\2\2\u0257\u0258\7Q\2\2\u0258\u025b\7V\2\2\u0259\u025b\7#\2\2"+
		"\u025a\u0256\3\2\2\2\u025a\u0259\3\2\2\2\u025b:\3\2\2\2\u025c\u025d\7"+
		"P\2\2\u025d\u025e\7Q\2\2\u025e<\3\2\2\2\u025f\u0260\7G\2\2\u0260\u0261"+
		"\7Z\2\2\u0261\u0262\7K\2\2\u0262\u0263\7U\2\2\u0263\u0264\7V\2\2\u0264"+
		"\u0265\7U\2\2\u0265>\3\2\2\2\u0266\u0267\7D\2\2\u0267\u0268\7G\2\2\u0268"+
		"\u0269\7V\2\2\u0269\u026a\7Y\2\2\u026a\u026b\7G\2\2\u026b\u026c\7G\2\2"+
		"\u026c\u026d\7P\2\2\u026d@\3\2\2\2\u026e\u026f\7N\2\2\u026f\u0270\7K\2"+
		"\2\u0270\u0271\7M\2\2\u0271\u0272\7G\2\2\u0272B\3\2\2\2\u0273\u0274\7"+
		"T\2\2\u0274\u0275\7N\2\2\u0275\u0276\7K\2\2\u0276\u0277\7M\2\2\u0277\u027f"+
		"\7G\2\2\u0278\u0279\7T\2\2\u0279\u027a\7G\2\2\u027a\u027b\7I\2\2\u027b"+
		"\u027c\7G\2\2\u027c\u027d\7Z\2\2\u027d\u027f\7R\2\2\u027e\u0273\3\2\2"+
		"\2\u027e\u0278\3\2\2\2\u027fD\3\2\2\2\u0280\u0281\7K\2\2\u0281\u0282\7"+
		"U\2\2\u0282F\3\2\2\2\u0283\u0284\7P\2\2\u0284\u0285\7W\2\2\u0285\u0286"+
		"\7N\2\2\u0286\u0287\7N\2\2\u0287H\3\2\2\2\u0288\u0289\7V\2\2\u0289\u028a"+
		"\7T\2\2\u028a\u028b\7W\2\2\u028b\u028c\7G\2\2\u028cJ\3\2\2\2\u028d\u028e"+
		"\7H\2\2\u028e\u028f\7C\2\2\u028f\u0290\7N\2\2\u0290\u0291\7U\2\2\u0291"+
		"\u0292\7G\2\2\u0292L\3\2\2\2\u0293\u0294\7P\2\2\u0294\u0295\7W\2\2\u0295"+
		"\u0296\7N\2\2\u0296\u0297\7N\2\2\u0297\u0298\7U\2\2\u0298N\3\2\2\2\u0299"+
		"\u029a\7C\2\2\u029a\u029b\7U\2\2\u029b\u029c\7E\2\2\u029cP\3\2\2\2\u029d"+
		"\u029e\7F\2\2\u029e\u029f\7G\2\2\u029f\u02a0\7U\2\2\u02a0\u02a1\7E\2\2"+
		"\u02a1R\3\2\2\2\u02a2\u02a3\7H\2\2\u02a3\u02a4\7Q\2\2\u02a4\u02a5\7T\2"+
		"\2\u02a5T\3\2\2\2\u02a6\u02a7\7K\2\2\u02a7\u02a8\7P\2\2\u02a8\u02a9\7"+
		"V\2\2\u02a9\u02aa\7G\2\2\u02aa\u02ab\7T\2\2\u02ab\u02ac\7X\2\2\u02ac\u02ad"+
		"\7C\2\2\u02ad\u02ae\7N\2\2\u02aeV\3\2\2\2\u02af\u02b0\7E\2\2\u02b0\u02b1"+
		"\7C\2\2\u02b1\u02b2\7U\2\2\u02b2\u02b3\7G\2\2\u02b3X\3\2\2\2\u02b4\u02b5"+
		"\7Y\2\2\u02b5\u02b6\7J\2\2\u02b6\u02b7\7G\2\2\u02b7\u02b8\7P\2\2\u02b8"+
		"Z\3\2\2\2\u02b9\u02ba\7V\2\2\u02ba\u02bb\7J\2\2\u02bb\u02bc\7G\2\2\u02bc"+
		"\u02bd\7P\2\2\u02bd\\\3\2\2\2\u02be\u02bf\7G\2\2\u02bf\u02c0\7N\2\2\u02c0"+
		"\u02c1\7U\2\2\u02c1\u02c2\7G\2\2\u02c2^\3\2\2\2\u02c3\u02c4\7G\2\2\u02c4"+
		"\u02c5\7P\2\2\u02c5\u02c6\7F\2\2\u02c6`\3\2\2\2\u02c7\u02c8\7L\2\2\u02c8"+
		"\u02c9\7Q\2\2\u02c9\u02ca\7K\2\2\u02ca\u02cb\7P\2\2\u02cbb\3\2\2\2\u02cc"+
		"\u02cd\7E\2\2\u02cd\u02ce\7T\2\2\u02ce\u02cf\7Q\2\2\u02cf\u02d0\7U\2\2"+
		"\u02d0\u02d1\7U\2\2\u02d1d\3\2\2\2\u02d2\u02d3\7Q\2\2\u02d3\u02d4\7W\2"+
		"\2\u02d4\u02d5\7V\2\2\u02d5\u02d6\7G\2\2\u02d6\u02d7\7T\2\2\u02d7f\3\2"+
		"\2\2\u02d8\u02d9\7K\2\2\u02d9\u02da\7P\2\2\u02da\u02db\7P\2\2\u02db\u02dc"+
		"\7G\2\2\u02dc\u02dd\7T\2\2\u02ddh\3\2\2\2\u02de\u02df\7N\2\2\u02df\u02e0"+
		"\7G\2\2\u02e0\u02e1\7H\2\2\u02e1\u02e2\7V\2\2\u02e2j\3\2\2\2\u02e3\u02e4"+
		"\7U\2\2\u02e4\u02e5\7G\2\2\u02e5\u02e6\7O\2\2\u02e6\u02e7\7K\2\2\u02e7"+
		"l\3\2\2\2\u02e8\u02e9\7T\2\2\u02e9\u02ea\7K\2\2\u02ea\u02eb\7I\2\2\u02eb"+
		"\u02ec\7J\2\2\u02ec\u02ed\7V\2\2\u02edn\3\2\2\2\u02ee\u02ef\7H\2\2\u02ef"+
		"\u02f0\7W\2\2\u02f0\u02f1\7N\2\2\u02f1\u02f2\7N\2\2\u02f2p\3\2\2\2\u02f3"+
		"\u02f4\7P\2\2\u02f4\u02f5\7C\2\2\u02f5\u02f6\7V\2\2\u02f6\u02f7\7W\2\2"+
		"\u02f7\u02f8\7T\2\2\u02f8\u02f9\7C\2\2\u02f9\u02fa\7N\2\2\u02far\3\2\2"+
		"\2\u02fb\u02fc\7Q\2\2\u02fc\u02fd\7P\2\2\u02fdt\3\2\2\2\u02fe\u02ff\7"+
		"N\2\2\u02ff\u0300\7C\2\2\u0300\u0301\7V\2\2\u0301\u0302\7G\2\2\u0302\u0303"+
		"\7T\2\2\u0303\u0304\7C\2\2\u0304\u0305\7N\2\2\u0305v\3\2\2\2\u0306\u0307"+
		"\7Y\2\2\u0307\u0308\7K\2\2\u0308\u0309\7P\2\2\u0309\u030a\7F\2\2\u030a"+
		"\u030b\7Q\2\2\u030b\u030c\7Y\2\2\u030cx\3\2\2\2\u030d\u030e\7Q\2\2\u030e"+
		"\u030f\7X\2\2\u030f\u0310\7G\2\2\u0310\u0311\7T\2\2\u0311z\3\2\2\2\u0312"+
		"\u0313\7R\2\2\u0313\u0314\7C\2\2\u0314\u0315\7T\2\2\u0315\u0316\7V\2\2"+
		"\u0316\u0317\7K\2\2\u0317\u0318\7V\2\2\u0318\u0319\7K\2\2\u0319\u031a"+
		"\7Q\2\2\u031a\u031b\7P\2\2\u031b|\3\2\2\2\u031c\u031d\7T\2\2\u031d\u031e"+
		"\7C\2\2\u031e\u031f\7P\2\2\u031f\u0320\7I\2\2\u0320\u0321\7G\2\2\u0321"+
		"~\3\2\2\2\u0322\u0323\7T\2\2\u0323\u0324\7Q\2\2\u0324\u0325\7Y\2\2\u0325"+
		"\u0326\7U\2\2\u0326\u0080\3\2\2\2\u0327\u0328\7W\2\2\u0328\u0329\7P\2"+
		"\2\u0329\u032a\7D\2\2\u032a\u032b\7Q\2\2\u032b\u032c\7W\2\2\u032c\u032d"+
		"\7P\2\2\u032d\u032e\7F\2\2\u032e\u032f\7G\2\2\u032f\u0330\7F\2\2\u0330"+
		"\u0082\3\2\2\2\u0331\u0332\7R\2\2\u0332\u0333\7T\2\2\u0333\u0334\7G\2"+
		"\2\u0334\u0335\7E\2\2\u0335\u0336\7G\2\2\u0336\u0337\7F\2\2\u0337\u0338"+
		"\7K\2\2\u0338\u0339\7P\2\2\u0339\u033a\7I\2\2\u033a\u0084\3\2\2\2\u033b"+
		"\u033c\7H\2\2\u033c\u033d\7Q\2\2\u033d\u033e\7N\2\2\u033e\u033f\7N\2\2"+
		"\u033f\u0340\7Q\2\2\u0340\u0341\7Y\2\2\u0341\u0342\7K\2\2\u0342\u0343"+
		"\7P\2\2\u0343\u0344\7I\2\2\u0344\u0086\3\2\2\2\u0345\u0346\7E\2\2\u0346"+
		"\u0347\7W\2\2\u0347\u0348\7T\2\2\u0348\u0349\7T\2\2\u0349\u034a\7G\2\2"+
		"\u034a\u034b\7P\2\2\u034b\u034c\7V\2\2\u034c\u0088\3\2\2\2\u034d\u034e"+
		"\7T\2\2\u034e\u034f\7Q\2\2\u034f\u0350\7Y\2\2\u0350\u008a\3\2\2\2\u0351"+
		"\u0352\7Y\2\2\u0352\u0353\7K\2\2\u0353\u0354\7V\2\2\u0354\u0355\7J\2\2"+
		"\u0355\u008c\3\2\2\2\u0356\u0357\7X\2\2\u0357\u0358\7C\2\2\u0358\u0359"+
		"\7N\2\2\u0359\u035a\7W\2\2\u035a\u035b\7G\2\2\u035b\u035c\7U\2\2\u035c"+
		"\u008e\3\2\2\2\u035d\u035e\7E\2\2\u035e\u035f\7T\2\2\u035f\u0360\7G\2"+
		"\2\u0360\u0361\7C\2\2\u0361\u0362\7V\2\2\u0362\u0363\7G\2\2\u0363\u0090"+
		"\3\2\2\2\u0364\u0365\7V\2\2\u0365\u0366\7C\2\2\u0366\u0367\7D\2\2\u0367"+
		"\u0368\7N\2\2\u0368\u0369\7G\2\2\u0369\u0092\3\2\2\2\u036a\u036b\7X\2"+
		"\2\u036b\u036c\7K\2\2\u036c\u036d\7G\2\2\u036d\u036e\7Y\2\2\u036e\u0094"+
		"\3\2\2\2\u036f\u0370\7T\2\2\u0370\u0371\7G\2\2\u0371\u0372\7R\2\2\u0372"+
		"\u0373\7N\2\2\u0373\u0374\7C\2\2\u0374\u0375\7E\2\2\u0375\u0376\7G\2\2"+
		"\u0376\u0096\3\2\2\2\u0377\u0378\7K\2\2\u0378\u0379\7P\2\2\u0379\u037a"+
		"\7U\2\2\u037a\u037b\7G\2\2\u037b\u037c\7T\2\2\u037c\u037d\7V\2\2\u037d"+
		"\u0098\3\2\2\2\u037e\u037f\7F\2\2\u037f\u0380\7G\2\2\u0380\u0381\7N\2"+
		"\2\u0381\u0382\7G\2\2\u0382\u0383\7V\2\2\u0383\u0384\7G\2\2\u0384\u009a"+
		"\3\2\2\2\u0385\u0386\7K\2\2\u0386\u0387\7P\2\2\u0387\u0388\7V\2\2\u0388"+
		"\u0389\7Q\2\2\u0389\u009c\3\2\2\2\u038a\u038b\7F\2\2\u038b\u038c\7G\2"+
		"\2\u038c\u038d\7U\2\2\u038d\u038e\7E\2\2\u038e\u038f\7T\2\2\u038f\u0390"+
		"\7K\2\2\u0390\u0391\7D\2\2\u0391\u0392\7G\2\2\u0392\u009e\3\2\2\2\u0393"+
		"\u0394\7G\2\2\u0394\u0395\7Z\2\2\u0395\u0396\7R\2\2\u0396\u0397\7N\2\2"+
		"\u0397\u0398\7C\2\2\u0398\u0399\7K\2\2\u0399\u039a\7P\2\2\u039a\u00a0"+
		"\3\2\2\2\u039b\u039c\7H\2\2\u039c\u039d\7Q\2\2\u039d\u039e\7T\2\2\u039e"+
		"\u039f\7O\2\2\u039f\u03a0\7C\2\2\u03a0\u03a1\7V\2\2\u03a1\u00a2\3\2\2"+
		"\2\u03a2\u03a3\7N\2\2\u03a3\u03a4\7Q\2\2\u03a4\u03a5\7I\2\2\u03a5\u03a6"+
		"\7K\2\2\u03a6\u03a7\7E\2\2\u03a7\u03a8\7C\2\2\u03a8\u03a9\7N\2\2\u03a9"+
		"\u00a4\3\2\2\2\u03aa\u03ab\7E\2\2\u03ab\u03ac\7Q\2\2\u03ac\u03ad\7F\2"+
		"\2\u03ad\u03ae\7G\2\2\u03ae\u03af\7I\2\2\u03af\u03b0\7G\2\2\u03b0\u03b1"+
		"\7P\2\2\u03b1\u00a6\3\2\2\2\u03b2\u03b3\7E\2\2\u03b3\u03b4\7C\2\2\u03b4"+
		"\u03b5\7U\2\2\u03b5\u03b6\7V\2\2\u03b6\u00a8\3\2\2\2\u03b7\u03b8\7U\2"+
		"\2\u03b8\u03b9\7J\2\2\u03b9\u03ba\7Q\2\2\u03ba\u03bb\7Y\2\2\u03bb\u00aa"+
		"\3\2\2\2\u03bc\u03bd\7V\2\2\u03bd\u03be\7C\2\2\u03be\u03bf\7D\2\2\u03bf"+
		"\u03c0\7N\2\2\u03c0\u03c1\7G\2\2\u03c1\u03c2\7U\2\2\u03c2\u00ac\3\2\2"+
		"\2\u03c3\u03c4\7E\2\2\u03c4\u03c5\7Q\2\2\u03c5\u03c6\7N\2\2\u03c6\u03c7"+
		"\7W\2\2\u03c7\u03c8\7O\2\2\u03c8\u03c9\7P\2\2\u03c9\u03ca\7U\2\2\u03ca"+
		"\u00ae\3\2\2\2\u03cb\u03cc\7E\2\2\u03cc\u03cd\7Q\2\2\u03cd\u03ce\7N\2"+
		"\2\u03ce\u03cf\7W\2\2\u03cf\u03d0\7O\2\2\u03d0\u03d1\7P\2\2\u03d1\u00b0"+
		"\3\2\2\2\u03d2\u03d3\7W\2\2\u03d3\u03d4\7U\2\2\u03d4\u03d5\7G\2\2\u03d5"+
		"\u00b2\3\2\2\2\u03d6\u03d7\7R\2\2\u03d7\u03d8\7C\2\2\u03d8\u03d9\7T\2"+
		"\2\u03d9\u03da\7V\2\2\u03da\u03db\7K\2\2\u03db\u03dc\7V\2\2\u03dc\u03dd"+
		"\7K\2\2\u03dd\u03de\7Q\2\2\u03de\u03df\7P\2\2\u03df\u03e0\7U\2\2\u03e0"+
		"\u00b4\3\2\2\2\u03e1\u03e2\7H\2\2\u03e2\u03e3\7W\2\2\u03e3\u03e4\7P\2"+
		"\2\u03e4\u03e5\7E\2\2\u03e5\u03e6\7V\2\2\u03e6\u03e7\7K\2\2\u03e7\u03e8"+
		"\7Q\2\2\u03e8\u03e9\7P\2\2\u03e9\u03ea\7U\2\2\u03ea\u00b6\3\2\2\2\u03eb"+
		"\u03ec\7F\2\2\u03ec\u03ed\7T\2\2\u03ed\u03ee\7Q\2\2\u03ee\u03ef\7R\2\2"+
		"\u03ef\u00b8\3\2\2\2\u03f0\u03f1\7W\2\2\u03f1\u03f2\7P\2\2\u03f2\u03f3"+
		"\7K\2\2\u03f3\u03f4\7Q\2\2\u03f4\u03f5\7P\2\2\u03f5\u00ba\3\2\2\2\u03f6"+
		"\u03f7\7G\2\2\u03f7\u03f8\7Z\2\2\u03f8\u03f9\7E\2\2\u03f9\u03fa\7G\2\2"+
		"\u03fa\u03fb\7R\2\2\u03fb\u03fc\7V\2\2\u03fc\u00bc\3\2\2\2\u03fd\u03fe"+
		"\7K\2\2\u03fe\u03ff\7P\2\2\u03ff\u0400\7V\2\2\u0400\u0401\7G\2\2\u0401"+
		"\u0402\7T\2\2\u0402\u0403\7U\2\2\u0403\u0404\7G\2\2\u0404\u0405\7E\2\2"+
		"\u0405\u0406\7V\2\2\u0406\u00be\3\2\2\2\u0407\u0408\7V\2\2\u0408\u0409"+
		"\7Q\2\2\u0409\u00c0\3\2\2\2\u040a\u040b\7V\2\2\u040b\u040c\7C\2\2\u040c"+
		"\u040d\7D\2\2\u040d\u040e\7N\2\2\u040e\u040f\7G\2\2\u040f\u0410\7U\2\2"+
		"\u0410\u0411\7C\2\2\u0411\u0412\7O\2\2\u0412\u0413\7R\2\2\u0413\u0414"+
		"\7N\2\2\u0414\u0415\7G\2\2\u0415\u00c2\3\2\2\2\u0416\u0417\7U\2\2\u0417"+
		"\u0418\7V\2\2\u0418\u0419\7T\2\2\u0419\u041a\7C\2\2\u041a\u041b\7V\2\2"+
		"\u041b\u041c\7K\2\2\u041c\u041d\7H\2\2\u041d\u041e\7[\2\2\u041e\u00c4"+
		"\3\2\2\2\u041f\u0420\7C\2\2\u0420\u0421\7N\2\2\u0421\u0422\7V\2\2\u0422"+
		"\u0423\7G\2\2\u0423\u0424\7T\2\2\u0424\u00c6\3\2\2\2\u0425\u0426\7T\2"+
		"\2\u0426\u0427\7G\2\2\u0427\u0428\7P\2\2\u0428\u0429\7C\2\2\u0429\u042a"+
		"\7O\2\2\u042a\u042b\7G\2\2\u042b\u00c8\3\2\2\2\u042c\u042d\7C\2\2\u042d"+
		"\u042e\7T\2\2\u042e\u042f\7T\2\2\u042f\u0430\7C\2\2\u0430\u0431\7[\2\2"+
		"\u0431\u00ca\3\2\2\2\u0432\u0433\7O\2\2\u0433\u0434\7C\2\2\u0434\u0435"+
		"\7R\2\2\u0435\u00cc\3\2\2\2\u0436\u0437\7U\2\2\u0437\u0438\7V\2\2\u0438"+
		"\u0439\7T\2\2\u0439\u043a\7W\2\2\u043a\u043b\7E\2\2\u043b\u043c\7V\2\2"+
		"\u043c\u00ce\3\2\2\2\u043d\u043e\7E\2\2\u043e\u043f\7Q\2\2\u043f\u0440"+
		"\7O\2\2\u0440\u0441\7O\2\2\u0441\u0442\7G\2\2\u0442\u0443\7P\2\2\u0443"+
		"\u0444\7V\2\2\u0444\u00d0\3\2\2\2\u0445\u0446\7U\2\2\u0446\u0447\7G\2"+
		"\2\u0447\u0448\7V\2\2\u0448\u00d2\3\2\2\2\u0449\u044a\7T\2\2\u044a\u044b"+
		"\7G\2\2\u044b\u044c\7U\2\2\u044c\u044d\7G\2\2\u044d\u044e\7V\2\2\u044e"+
		"\u00d4\3\2\2\2\u044f\u0450\7F\2\2\u0450\u0451\7C\2\2\u0451\u0452\7V\2"+
		"\2\u0452\u0453\7C\2\2\u0453\u00d6\3\2\2\2\u0454\u0455\7U\2\2\u0455\u0456"+
		"\7V\2\2\u0456\u0457\7C\2\2\u0457\u0458\7T\2\2\u0458\u0459\7V\2\2\u0459"+
		"\u00d8\3\2\2\2\u045a\u045b\7V\2\2\u045b\u045c\7T\2\2\u045c\u045d\7C\2"+
		"\2\u045d\u045e\7P\2\2\u045e\u045f\7U\2\2\u045f\u0460\7C\2\2\u0460\u0461"+
		"\7E\2\2\u0461\u0462\7V\2\2\u0462\u0463\7K\2\2\u0463\u0464\7Q\2\2\u0464"+
		"\u0465\7P\2\2\u0465\u00da\3\2\2\2\u0466\u0467\7E\2\2\u0467\u0468\7Q\2"+
		"\2\u0468\u0469\7O\2\2\u0469\u046a\7O\2\2\u046a\u046b\7K\2\2\u046b\u046c"+
		"\7V\2\2\u046c\u00dc\3\2\2\2\u046d\u046e\7T\2\2\u046e\u046f\7Q\2\2\u046f"+
		"\u0470\7N\2\2\u0470\u0471\7N\2\2\u0471\u0472\7D\2\2\u0472\u0473\7C\2\2"+
		"\u0473\u0474\7E\2\2\u0474\u0475\7M\2\2\u0475\u00de\3\2\2\2\u0476\u0477"+
		"\7O\2\2\u0477\u0478\7C\2\2\u0478\u0479\7E\2\2\u0479\u047a\7T\2\2\u047a"+
		"\u047b\7Q\2\2\u047b\u00e0\3\2\2\2\u047c\u047d\7K\2\2\u047d\u047e\7H\2"+
		"\2\u047e\u00e2\3\2\2\2\u047f\u0483\7?\2\2\u0480\u0481\7?\2\2\u0481\u0483"+
		"\7?\2\2\u0482\u047f\3\2\2\2\u0482\u0480\3\2\2\2\u0483\u00e4\3\2\2\2\u0484"+
		"\u0485\7>\2\2\u0485\u0486\7?\2\2\u0486\u0487\7@\2\2\u0487\u00e6\3\2\2"+
		"\2\u0488\u0489\7>\2\2\u0489\u048a\7@\2\2\u048a\u00e8\3\2\2\2\u048b\u048c"+
		"\7#\2\2\u048c\u048d\7?\2\2\u048d\u00ea\3\2\2\2\u048e\u048f\7>\2\2\u048f"+
		"\u00ec\3\2\2\2\u0490\u0491\7>\2\2\u0491\u0495\7?\2\2\u0492\u0493\7#\2"+
		"\2\u0493\u0495\7@\2\2\u0494\u0490\3\2\2\2\u0494\u0492\3\2\2\2\u0495\u00ee"+
		"\3\2\2\2\u0496\u0497\7@\2\2\u0497\u00f0\3\2\2\2\u0498\u0499\7@\2\2\u0499"+
		"\u049d\7?\2\2\u049a\u049b\7#\2\2\u049b\u049d\7>\2\2\u049c\u0498\3\2\2"+
		"\2\u049c\u049a\3\2\2\2\u049d\u00f2\3\2\2\2\u049e\u049f\7-\2\2\u049f\u00f4"+
		"\3\2\2\2\u04a0\u04a1\7/\2\2\u04a1\u00f6\3\2\2\2\u04a2\u04a3\7,\2\2\u04a3"+
		"\u00f8\3\2\2\2\u04a4\u04a5\7\61\2\2\u04a5\u00fa\3\2\2\2\u04a6\u04a7\7"+
		"\'\2\2\u04a7\u00fc\3\2\2\2\u04a8\u04a9\7F\2\2\u04a9\u04aa\7K\2\2\u04aa"+
		"\u04ab\7X\2\2\u04ab\u00fe\3\2\2\2\u04ac\u04ad\7\u0080\2\2\u04ad\u0100"+
		"\3\2\2\2\u04ae\u04af\7(\2\2\u04af\u0102\3\2\2\2\u04b0\u04b1\7~\2\2\u04b1"+
		"\u0104\3\2\2\2\u04b2\u04b3\7`\2\2\u04b3\u0106\3\2\2\2\u04b4\u04b5\7R\2"+
		"\2\u04b5\u04b6\7G\2\2\u04b6\u04b7\7T\2\2\u04b7\u04b8\7E\2\2\u04b8\u04b9"+
		"\7G\2\2\u04b9\u04ba\7P\2\2\u04ba\u04bb\7V\2\2\u04bb\u0108\3\2\2\2\u04bc"+
		"\u04bd\7D\2\2\u04bd\u04be\7W\2\2\u04be\u04bf\7E\2\2\u04bf\u04c0\7M\2\2"+
		"\u04c0\u04c1\7G\2\2\u04c1\u04c2\7V\2\2\u04c2\u010a\3\2\2\2\u04c3\u04c4"+
		"\7Q\2\2\u04c4\u04c5\7W\2\2\u04c5\u04c6\7V\2\2\u04c6\u010c\3\2\2\2\u04c7"+
		"\u04c8\7Q\2\2\u04c8\u04c9\7H\2\2\u04c9\u010e\3\2\2\2\u04ca\u04cb\7U\2"+
		"\2\u04cb\u04cc\7Q\2\2\u04cc\u04cd\7T\2\2\u04cd\u04ce\7V\2\2\u04ce\u0110"+
		"\3\2\2\2\u04cf\u04d0\7E\2\2\u04d0\u04d1\7N\2\2\u04d1\u04d2\7W\2\2\u04d2"+
		"\u04d3\7U\2\2\u04d3\u04d4\7V\2\2\u04d4\u04d5\7G\2\2\u04d5\u04d6\7T\2\2"+
		"\u04d6\u0112\3\2\2\2\u04d7\u04d8\7F\2\2\u04d8\u04d9\7K\2\2\u04d9\u04da"+
		"\7U\2\2\u04da\u04db\7V\2\2\u04db\u04dc\7T\2\2\u04dc\u04dd\7K\2\2\u04dd"+
		"\u04de\7D\2\2\u04de\u04df\7W\2\2\u04df\u04e0\7V\2\2\u04e0\u04e1\7G\2\2"+
		"\u04e1\u0114\3\2\2\2\u04e2\u04e3\7Q\2\2\u04e3\u04e4\7X\2\2\u04e4\u04e5"+
		"\7G\2\2\u04e5\u04e6\7T\2\2\u04e6\u04e7\7Y\2\2\u04e7\u04e8\7T\2\2\u04e8"+
		"\u04e9\7K\2\2\u04e9\u04ea\7V\2\2\u04ea\u04eb\7G\2\2\u04eb\u0116\3\2\2"+
		"\2\u04ec\u04ed\7V\2\2\u04ed\u04ee\7T\2\2\u04ee\u04ef\7C\2\2\u04ef\u04f0"+
		"\7P\2\2\u04f0\u04f1\7U\2\2\u04f1\u04f2\7H\2\2\u04f2\u04f3\7Q\2\2\u04f3"+
		"\u04f4\7T\2\2\u04f4\u04f5\7O\2\2\u04f5\u0118\3\2\2\2\u04f6\u04f7\7T\2"+
		"\2\u04f7\u04f8\7G\2\2\u04f8\u04f9\7F\2\2\u04f9\u04fa\7W\2\2\u04fa\u04fb"+
		"\7E\2\2\u04fb\u04fc\7G\2\2\u04fc\u011a\3\2\2\2\u04fd\u04fe\7W\2\2\u04fe"+
		"\u04ff\7U\2\2\u04ff\u0500\7K\2\2\u0500\u0501\7P\2\2\u0501\u0502\7I\2\2"+
		"\u0502\u011c\3\2\2\2\u0503\u0504\7U\2\2\u0504\u0505\7G\2\2\u0505\u0506"+
		"\7T\2\2\u0506\u0507\7F\2\2\u0507\u0508\7G\2\2\u0508\u011e\3\2\2\2\u0509"+
		"\u050a\7U\2\2\u050a\u050b\7G\2\2\u050b\u050c\7T\2\2\u050c\u050d\7F\2\2"+
		"\u050d\u050e\7G\2\2\u050e\u050f\7R\2\2\u050f\u0510\7T\2\2\u0510\u0511"+
		"\7Q\2\2\u0511\u0512\7R\2\2\u0512\u0513\7G\2\2\u0513\u0514\7T\2\2\u0514"+
		"\u0515\7V\2\2\u0515\u0516\7K\2\2\u0516\u0517\7G\2\2\u0517\u0518\7U\2\2"+
		"\u0518\u0120\3\2\2\2\u0519\u051a\7T\2\2\u051a\u051b\7G\2\2\u051b\u051c"+
		"\7E\2\2\u051c\u051d\7Q\2\2\u051d\u051e\7T\2\2\u051e\u051f\7F\2\2\u051f"+
		"\u0520\7T\2\2\u0520\u0521\7G\2\2\u0521\u0522\7C\2\2\u0522\u0523\7F\2\2"+
		"\u0523\u0524\7G\2\2\u0524\u0525\7T\2\2\u0525\u0122\3\2\2\2\u0526\u0527"+
		"\7T\2\2\u0527\u0528\7G\2\2\u0528\u0529\7E\2\2\u0529\u052a\7Q\2\2\u052a"+
		"\u052b\7T\2\2\u052b\u052c\7F\2\2\u052c\u052d\7Y\2\2\u052d\u052e\7T\2\2"+
		"\u052e\u052f\7K\2\2\u052f\u0530\7V\2\2\u0530\u0531\7G\2\2\u0531\u0532"+
		"\7T\2\2\u0532\u0124\3\2\2\2\u0533\u0534\7F\2\2\u0534\u0535\7G\2\2\u0535"+
		"\u0536\7N\2\2\u0536\u0537\7K\2\2\u0537\u0538\7O\2\2\u0538\u0539\7K\2\2"+
		"\u0539\u053a\7V\2\2\u053a\u053b\7G\2\2\u053b\u053c\7F\2\2\u053c\u0126"+
		"\3\2\2\2\u053d\u053e\7H\2\2\u053e\u053f\7K\2\2\u053f\u0540\7G\2\2\u0540"+
		"\u0541\7N\2\2\u0541\u0542\7F\2\2\u0542\u0543\7U\2\2\u0543\u0128\3\2\2"+
		"\2\u0544\u0545\7V\2\2\u0545\u0546\7G\2\2\u0546\u0547\7T\2\2\u0547\u0548"+
		"\7O\2\2\u0548\u0549\7K\2\2\u0549\u054a\7P\2\2\u054a\u054b\7C\2\2\u054b"+
		"\u054c\7V\2\2\u054c\u054d\7G\2\2\u054d\u054e\7F\2\2\u054e\u012a\3\2\2"+
		"\2\u054f\u0550\7E\2\2\u0550\u0551\7Q\2\2\u0551\u0552\7N\2\2\u0552\u0553"+
		"\7N\2\2\u0553\u0554\7G\2\2\u0554\u0555\7E\2\2\u0555\u0556\7V\2\2\u0556"+
		"\u0557\7K\2\2\u0557\u0558\7Q\2\2\u0558\u0559\7P\2\2\u0559\u012c\3\2\2"+
		"\2\u055a\u055b\7K\2\2\u055b\u055c\7V\2\2\u055c\u055d\7G\2\2\u055d\u055e"+
		"\7O\2\2\u055e\u055f\7U\2\2\u055f\u012e\3\2\2\2\u0560\u0561\7M\2\2\u0561"+
		"\u0562\7G\2\2\u0562\u0563\7[\2\2\u0563\u0564\7U\2\2\u0564\u0130\3\2\2"+
		"\2\u0565\u0566\7G\2\2\u0566\u0567\7U\2\2\u0567\u0568\7E\2\2\u0568\u0569"+
		"\7C\2\2\u0569\u056a\7R\2\2\u056a\u056b\7G\2\2\u056b\u056c\7F\2\2\u056c"+
		"\u0132\3\2\2\2\u056d\u056e\7N\2\2\u056e\u056f\7K\2\2\u056f\u0570\7P\2"+
		"\2\u0570\u0571\7G\2\2\u0571\u0572\7U\2\2\u0572\u0134\3\2\2\2\u0573\u0574"+
		"\7U\2\2\u0574\u0575\7G\2\2\u0575\u0576\7R\2\2\u0576\u0577\7C\2\2\u0577"+
		"\u0578\7T\2\2\u0578\u0579\7C\2\2\u0579\u057a\7V\2\2\u057a\u057b\7G\2\2"+
		"\u057b\u057c\7F\2\2\u057c\u0136\3\2\2\2\u057d\u057e\7H\2\2\u057e\u057f"+
		"\7W\2\2\u057f\u0580\7P\2\2\u0580\u0581\7E\2\2\u0581\u0582\7V\2\2\u0582"+
		"\u0583\7K\2\2\u0583\u0584\7Q\2\2\u0584\u0585\7P\2\2\u0585\u0138\3\2\2"+
		"\2\u0586\u0587\7G\2\2\u0587\u0588\7Z\2\2\u0588\u0589\7V\2\2\u0589\u058a"+
		"\7G\2\2\u058a\u058b\7P\2\2\u058b\u058c\7F\2\2\u058c\u058d\7G\2\2\u058d"+
		"\u058e\7F\2\2\u058e\u013a\3\2\2\2\u058f\u0590\7T\2\2\u0590\u0591\7G\2"+
		"\2\u0591\u0592\7H\2\2\u0592\u0593\7T\2\2\u0593\u0594\7G\2\2\u0594\u0595"+
		"\7U\2\2\u0595\u0596\7J\2\2\u0596\u013c\3\2\2\2\u0597\u0598\7E\2\2\u0598"+
		"\u0599\7N\2\2\u0599\u059a\7G\2\2\u059a\u059b\7C\2\2\u059b\u059c\7T\2\2"+
		"\u059c\u013e\3\2\2\2\u059d\u059e\7E\2\2\u059e\u059f\7C\2\2\u059f\u05a0"+
		"\7E\2\2\u05a0\u05a1\7J\2\2\u05a1\u05a2\7G\2\2\u05a2\u0140\3\2\2\2\u05a3"+
		"\u05a4\7W\2\2\u05a4\u05a5\7P\2\2\u05a5\u05a6\7E\2\2\u05a6\u05a7\7C\2\2"+
		"\u05a7\u05a8\7E\2\2\u05a8\u05a9\7J\2\2\u05a9\u05aa\7G\2\2\u05aa\u0142"+
		"\3\2\2\2\u05ab\u05ac\7N\2\2\u05ac\u05ad\7C\2\2\u05ad\u05ae\7\\\2\2\u05ae"+
		"\u05af\7[\2\2\u05af\u0144\3\2\2\2\u05b0\u05b1\7H\2\2\u05b1\u05b2\7Q\2"+
		"\2\u05b2\u05b3\7T\2\2\u05b3\u05b4\7O\2\2\u05b4\u05b5\7C\2\2\u05b5\u05b6"+
		"\7V\2\2\u05b6\u05b7\7V\2\2\u05b7\u05b8\7G\2\2\u05b8\u05b9\7F\2\2\u05b9"+
		"\u0146\3\2\2\2\u05ba\u05bb\7V\2\2\u05bb\u05bc\7G\2\2\u05bc\u05bd\7O\2"+
		"\2\u05bd\u05be\7R\2\2\u05be\u05bf\7Q\2\2\u05bf\u05c0\7T\2\2\u05c0\u05c1"+
		"\7C\2\2\u05c1\u05c2\7T\2\2\u05c2\u05c8\7[\2\2\u05c3\u05c4\7V\2\2\u05c4"+
		"\u05c5\7G\2\2\u05c5\u05c6\7O\2\2\u05c6\u05c8\7R\2\2\u05c7\u05ba\3\2\2"+
		"\2\u05c7\u05c3\3\2\2\2\u05c8\u0148\3\2\2\2\u05c9\u05ca\7Q\2\2\u05ca\u05cb"+
		"\7R\2\2\u05cb\u05cc\7V\2\2\u05cc\u05cd\7K\2\2\u05cd\u05ce\7Q\2\2\u05ce"+
		"\u05cf\7P\2\2\u05cf\u05d0\7U\2\2\u05d0\u014a\3\2\2\2\u05d1\u05d2\7W\2"+
		"\2\u05d2\u05d3\7P\2\2\u05d3\u05d4\7U\2\2\u05d4\u05d5\7G\2\2\u05d5\u05d6"+
		"\7V\2\2\u05d6\u014c\3\2\2\2\u05d7\u05d8\7V\2\2\u05d8\u05d9\7D\2\2\u05d9"+
		"\u05da\7N\2\2\u05da\u05db\7R\2\2\u05db\u05dc\7T\2\2\u05dc\u05dd\7Q\2\2"+
		"\u05dd\u05de\7R\2\2\u05de\u05df\7G\2\2\u05df\u05e0\7T\2\2\u05e0\u05e1"+
		"\7V\2\2\u05e1\u05e2\7K\2\2\u05e2\u05e3\7G\2\2\u05e3\u05e4\7U\2\2\u05e4"+
		"\u014e\3\2\2\2\u05e5\u05e6\7F\2\2\u05e6\u05e7\7D\2\2\u05e7\u05e8\7R\2"+
		"\2\u05e8\u05e9\7T\2\2\u05e9\u05ea\7Q\2\2\u05ea\u05eb\7R\2\2\u05eb\u05ec"+
		"\7G\2\2\u05ec\u05ed\7T\2\2\u05ed\u05ee\7V\2\2\u05ee\u05ef\7K\2\2\u05ef"+
		"\u05f0\7G\2\2\u05f0\u05f1\7U\2\2\u05f1\u0150\3\2\2\2\u05f2\u05f3\7D\2"+
		"\2\u05f3\u05f4\7W\2\2\u05f4\u05f5\7E\2\2\u05f5\u05f6\7M\2\2\u05f6\u05f7"+
		"\7G\2\2\u05f7\u05f8\7V\2\2\u05f8\u05f9\7U\2\2\u05f9\u0152\3\2\2\2\u05fa"+
		"\u05fb\7U\2\2\u05fb\u05fc\7M\2\2\u05fc\u05fd\7G\2\2\u05fd\u05fe\7Y\2\2"+
		"\u05fe\u05ff\7G\2\2\u05ff\u0600\7F\2\2\u0600\u0154\3\2\2\2\u0601\u0602"+
		"\7U\2\2\u0602\u0603\7V\2\2\u0603\u0604\7Q\2\2\u0604\u0605\7T\2\2\u0605"+
		"\u0606\7G\2\2\u0606\u0607\7F\2\2\u0607\u0156\3\2\2\2\u0608\u0609\7F\2"+
		"\2\u0609\u060a\7K\2\2\u060a\u060b\7T\2\2\u060b\u060c\7G\2\2\u060c\u060d"+
		"\7E\2\2\u060d\u060e\7V\2\2\u060e\u060f\7Q\2\2\u060f\u0610\7T\2\2\u0610"+
		"\u0611\7K\2\2\u0611\u0612\7G\2\2\u0612\u0613\7U\2\2\u0613\u0158\3\2\2"+
		"\2\u0614\u0615\7N\2\2\u0615\u0616\7Q\2\2\u0616\u0617\7E\2\2\u0617\u0618"+
		"\7C\2\2\u0618\u0619\7V\2\2\u0619\u061a\7K\2\2\u061a\u061b\7Q\2\2\u061b"+
		"\u061c\7P\2\2\u061c\u015a\3\2\2\2\u061d\u061e\7G\2\2\u061e\u061f\7Z\2"+
		"\2\u061f\u0620\7E\2\2\u0620\u0621\7J\2\2\u0621\u0622\7C\2\2\u0622\u0623"+
		"\7P\2\2\u0623\u0624\7I\2\2\u0624\u0625\7G\2\2\u0625\u015c\3\2\2\2\u0626"+
		"\u0627\7C\2\2\u0627\u0628\7T\2\2\u0628\u0629\7E\2\2\u0629\u062a\7J\2\2"+
		"\u062a\u062b\7K\2\2\u062b\u062c\7X\2\2\u062c\u062d\7G\2\2\u062d\u015e"+
		"\3\2\2\2\u062e\u062f\7W\2\2\u062f\u0630\7P\2\2\u0630\u0631\7C\2\2\u0631"+
		"\u0632\7T\2\2\u0632\u0633\7E\2\2\u0633\u0634\7J\2\2\u0634\u0635\7K\2\2"+
		"\u0635\u0636\7X\2\2\u0636\u0637\7G\2\2\u0637\u0160\3\2\2\2\u0638\u0639"+
		"\7H\2\2\u0639\u063a\7K\2\2\u063a\u063b\7N\2\2\u063b\u063c\7G\2\2\u063c"+
		"\u063d\7H\2\2\u063d\u063e\7Q\2\2\u063e\u063f\7T\2\2\u063f\u0640\7O\2\2"+
		"\u0640\u0641\7C\2\2\u0641\u0642\7V\2\2\u0642\u0162\3\2\2\2\u0643\u0644"+
		"\7V\2\2\u0644\u0645\7Q\2\2\u0645\u0646\7W\2\2\u0646\u0647\7E\2\2\u0647"+
		"\u0648\7J\2\2\u0648\u0164\3\2\2\2\u0649\u064a\7E\2\2\u064a\u064b\7Q\2"+
		"\2\u064b\u064c\7O\2\2\u064c\u064d\7R\2\2\u064d\u064e\7C\2\2\u064e\u064f"+
		"\7E\2\2\u064f\u0650\7V\2\2\u0650\u0166\3\2\2\2\u0651\u0652\7E\2\2\u0652"+
		"\u0653\7Q\2\2\u0653\u0654\7P\2\2\u0654\u0655\7E\2\2\u0655\u0656\7C\2\2"+
		"\u0656\u0657\7V\2\2\u0657\u0658\7G\2\2\u0658\u0659\7P\2\2\u0659\u065a"+
		"\7C\2\2\u065a\u065b\7V\2\2\u065b\u065c\7G\2\2\u065c\u0168\3\2\2\2\u065d"+
		"\u065e\7E\2\2\u065e\u065f\7J\2\2\u065f\u0660\7C\2\2\u0660\u0661\7P\2\2"+
		"\u0661\u0662\7I\2\2\u0662\u0663\7G\2\2\u0663\u016a\3\2\2\2\u0664\u0665"+
		"\7E\2\2\u0665\u0666\7C\2\2\u0666\u0667\7U\2\2\u0667\u0668\7E\2\2\u0668"+
		"\u0669\7C\2\2\u0669\u066a\7F\2\2\u066a\u066b\7G\2\2\u066b\u016c\3\2\2"+
		"\2\u066c\u066d\7T\2\2\u066d\u066e\7G\2\2\u066e\u066f\7U\2\2\u066f\u0670"+
		"\7V\2\2\u0670\u0671\7T\2\2\u0671\u0672\7K\2\2\u0672\u0673\7E\2\2\u0673"+
		"\u0674\7V\2\2\u0674\u016e\3\2\2\2\u0675\u0676\7E\2\2\u0676\u0677\7N\2"+
		"\2\u0677\u0678\7W\2\2\u0678\u0679\7U\2\2\u0679\u067a\7V\2\2\u067a\u067b"+
		"\7G\2\2\u067b\u067c\7T\2\2\u067c\u067d\7G\2\2\u067d\u067e\7F\2\2\u067e"+
		"\u0170\3\2\2\2\u067f\u0680\7U\2\2\u0680\u0681\7Q\2\2\u0681\u0682\7T\2"+
		"\2\u0682\u0683\7V\2\2\u0683\u0684\7G\2\2\u0684\u0685\7F\2\2\u0685\u0172"+
		"\3\2\2\2\u0686\u0687\7R\2\2\u0687\u0688\7W\2\2\u0688\u0689\7T\2\2\u0689"+
		"\u068a\7I\2\2\u068a\u068b\7G\2\2\u068b\u0174\3\2\2\2\u068c\u068d\7K\2"+
		"\2\u068d\u068e\7P\2\2\u068e\u068f\7R\2\2\u068f\u0690\7W\2\2\u0690\u0691"+
		"\7V\2\2\u0691\u0692\7H\2\2\u0692\u0693\7Q\2\2\u0693\u0694\7T\2\2\u0694"+
		"\u0695\7O\2\2\u0695\u0696\7C\2\2\u0696\u0697\7V\2\2\u0697\u0176\3\2\2"+
		"\2\u0698\u0699\7Q\2\2\u0699\u069a\7W\2\2\u069a\u069b\7V\2\2\u069b\u069c"+
		"\7R\2\2\u069c\u069d\7W\2\2\u069d\u069e\7V\2\2\u069e\u069f\7H\2\2\u069f"+
		"\u06a0\7Q\2\2\u06a0\u06a1\7T\2\2\u06a1\u06a2\7O\2\2\u06a2\u06a3\7C\2\2"+
		"\u06a3\u06a4\7V\2\2\u06a4\u0178\3\2\2\2\u06a5\u06a6\7F\2\2\u06a6\u06a7"+
		"\7C\2\2\u06a7\u06a8\7V\2\2\u06a8\u06a9\7C\2\2\u06a9\u06aa\7D\2\2\u06aa"+
		"\u06ab\7C\2\2\u06ab\u06ac\7U\2\2\u06ac\u06b4\7G\2\2\u06ad\u06ae\7U\2\2"+
		"\u06ae\u06af\7E\2\2\u06af\u06b0\7J\2\2\u06b0\u06b1\7G\2\2\u06b1\u06b2"+
		"\7O\2\2\u06b2\u06b4\7C\2\2\u06b3\u06a5\3\2\2\2\u06b3\u06ad\3\2\2\2\u06b4"+
		"\u017a\3\2\2\2\u06b5\u06b6\7F\2\2\u06b6\u06b7\7C\2\2\u06b7\u06b8\7V\2"+
		"\2\u06b8\u06b9\7C\2\2\u06b9\u06ba\7D\2\2\u06ba\u06bb\7C\2\2\u06bb\u06bc"+
		"\7U\2\2\u06bc\u06bd\7G\2\2\u06bd\u06c6\7U\2\2\u06be\u06bf\7U\2\2\u06bf"+
		"\u06c0\7E\2\2\u06c0\u06c1\7J\2\2\u06c1\u06c2\7G\2\2\u06c2\u06c3\7O\2\2"+
		"\u06c3\u06c4\7C\2\2\u06c4\u06c6\7U\2\2\u06c5\u06b5\3\2\2\2\u06c5\u06be"+
		"\3\2\2\2\u06c6\u017c\3\2\2\2\u06c7\u06c8\7F\2\2\u06c8\u06c9\7H\2\2\u06c9"+
		"\u06ca\7U\2\2\u06ca\u017e\3\2\2\2\u06cb\u06cc\7V\2\2\u06cc\u06cd\7T\2"+
		"\2\u06cd\u06ce\7W\2\2\u06ce\u06cf\7P\2\2\u06cf\u06d0\7E\2\2\u06d0\u06d1"+
		"\7C\2\2\u06d1\u06d2\7V\2\2\u06d2\u06d3\7G\2\2\u06d3\u0180\3\2\2\2\u06d4"+
		"\u06d5\7C\2\2\u06d5\u06d6\7P\2\2\u06d6\u06d7\7C\2\2\u06d7\u06d8\7N\2\2"+
		"\u06d8\u06d9\7[\2\2\u06d9\u06da\7\\\2\2\u06da\u06db\7G\2\2\u06db\u0182"+
		"\3\2\2\2\u06dc\u06dd\7E\2\2\u06dd\u06de\7Q\2\2\u06de\u06df\7O\2\2\u06df"+
		"\u06e0\7R\2\2\u06e0\u06e1\7W\2\2\u06e1\u06e2\7V\2\2\u06e2\u06e3\7G\2\2"+
		"\u06e3\u0184\3\2\2\2\u06e4\u06e5\7N\2\2\u06e5\u06e6\7K\2\2\u06e6\u06e7"+
		"\7U\2\2\u06e7\u06e8\7V\2\2\u06e8\u0186\3\2\2\2\u06e9\u06ea\7U\2\2\u06ea"+
		"\u06eb\7V\2\2\u06eb\u06ec\7C\2\2\u06ec\u06ed\7V\2\2\u06ed\u06ee\7K\2\2"+
		"\u06ee\u06ef\7U\2\2\u06ef\u06f0\7V\2\2\u06f0\u06f1\7K\2\2\u06f1\u06f2"+
		"\7E\2\2\u06f2\u06f3\7U\2\2\u06f3\u0188\3\2\2\2\u06f4\u06f5\7R\2\2\u06f5"+
		"\u06f6\7C\2\2\u06f6\u06f7\7T\2\2\u06f7\u06f8\7V\2\2\u06f8\u06f9\7K\2\2"+
		"\u06f9\u06fa\7V\2\2\u06fa\u06fb\7K\2\2\u06fb\u06fc\7Q\2\2\u06fc\u06fd"+
		"\7P\2\2\u06fd\u06fe\7G\2\2\u06fe\u06ff\7F\2\2\u06ff\u018a\3\2\2\2\u0700"+
		"\u0701\7G\2\2\u0701\u0702\7Z\2\2\u0702\u0703\7V\2\2\u0703\u0704\7G\2\2"+
		"\u0704\u0705\7T\2\2\u0705\u0706\7P\2\2\u0706\u0707\7C\2\2\u0707\u0708"+
		"\7N\2\2\u0708\u018c\3\2\2\2\u0709\u070a\7F\2\2\u070a\u070b\7G\2\2\u070b"+
		"\u070c\7H\2\2\u070c\u070d\7K\2\2\u070d\u070e\7P\2\2\u070e\u070f\7G\2\2"+
		"\u070f\u0710\7F\2\2\u0710\u018e\3\2\2\2\u0711\u0712\7T\2\2\u0712\u0713"+
		"\7G\2\2\u0713\u0714\7X\2\2\u0714\u0715\7Q\2\2\u0715\u0716\7M\2\2\u0716"+
		"\u0717\7G\2\2\u0717\u0190\3\2\2\2\u0718\u0719\7I\2\2\u0719\u071a\7T\2"+
		"\2\u071a\u071b\7C\2\2\u071b\u071c\7P\2\2\u071c\u071d\7V\2\2\u071d\u0192"+
		"\3\2\2\2\u071e\u071f\7N\2\2\u071f\u0720\7Q\2\2\u0720\u0721\7E\2\2\u0721"+
		"\u0722\7M\2\2\u0722\u0194\3\2\2\2\u0723\u0724\7W\2\2\u0724\u0725\7P\2"+
		"\2\u0725\u0726\7N\2\2\u0726\u0727\7Q\2\2\u0727\u0728\7E\2\2\u0728\u0729"+
		"\7M\2\2\u0729\u0196\3\2\2\2\u072a\u072b\7O\2\2\u072b\u072c\7U\2\2\u072c"+
		"\u072d\7E\2\2\u072d\u072e\7M\2\2\u072e\u0198\3\2\2\2\u072f\u0730\7T\2"+
		"\2\u0730\u0731\7G\2\2\u0731\u0732\7R\2\2\u0732\u0733\7C\2\2\u0733\u0734"+
		"\7K\2\2\u0734\u0735\7T\2\2\u0735\u019a\3\2\2\2\u0736\u0737\7G\2\2\u0737"+
		"\u0738\7Z\2\2\u0738\u0739\7R\2\2\u0739\u073a\7Q\2\2\u073a\u073b\7T\2\2"+
		"\u073b\u073c\7V\2\2\u073c\u019c\3\2\2\2\u073d\u073e\7K\2\2\u073e\u073f"+
		"\7O\2\2\u073f\u0740\7R\2\2\u0740\u0741\7Q\2\2\u0741\u0742\7T\2\2\u0742"+
		"\u0743\7V\2\2\u0743\u019e\3\2\2\2\u0744\u0745\7N\2\2\u0745\u0746\7Q\2"+
		"\2\u0746\u0747\7C\2\2\u0747\u0748\7F\2\2\u0748\u01a0\3\2\2\2\u0749\u074a"+
		"\7T\2\2\u074a\u074b\7Q\2\2\u074b\u074c\7N\2\2\u074c\u074d\7G\2\2\u074d"+
		"\u01a2\3\2\2\2\u074e\u074f\7T\2\2\u074f\u0750\7Q\2\2\u0750\u0751\7N\2"+
		"\2\u0751\u0752\7G\2\2\u0752\u0753\7U\2\2\u0753\u01a4\3\2\2\2\u0754\u0755"+
		"\7E\2\2\u0755\u0756\7Q\2\2\u0756\u0757\7O\2\2\u0757\u0758\7R\2\2\u0758"+
		"\u0759\7C\2\2\u0759\u075a\7E\2\2\u075a\u075b\7V\2\2\u075b\u075c\7K\2\2"+
		"\u075c\u075d\7Q\2\2\u075d\u075e\7P\2\2\u075e\u075f\7U\2\2\u075f\u01a6"+
		"\3\2\2\2\u0760\u0761\7R\2\2\u0761\u0762\7T\2\2\u0762\u0763\7K\2\2\u0763"+
		"\u0764\7P\2\2\u0764\u0765\7E\2\2\u0765\u0766\7K\2\2\u0766\u0767\7R\2\2"+
		"\u0767\u0768\7C\2\2\u0768\u0769\7N\2\2\u0769\u076a\7U\2\2\u076a\u01a8"+
		"\3\2\2\2\u076b\u076c\7V\2\2\u076c\u076d\7T\2\2\u076d\u076e\7C\2\2\u076e"+
		"\u076f\7P\2\2\u076f\u0770\7U\2\2\u0770\u0771\7C\2\2\u0771\u0772\7E\2\2"+
		"\u0772\u0773\7V\2\2\u0773\u0774\7K\2\2\u0774\u0775\7Q\2\2\u0775\u0776"+
		"\7P\2\2\u0776\u0777\7U\2\2\u0777\u01aa\3\2\2\2\u0778\u0779\7K\2\2\u0779"+
		"\u077a\7P\2\2\u077a\u077b\7F\2\2\u077b\u077c\7G\2\2\u077c\u077d\7Z\2\2"+
		"\u077d\u01ac\3\2\2\2\u077e\u077f\7K\2\2\u077f\u0780\7P\2\2\u0780\u0781"+
		"\7F\2\2\u0781\u0782\7G\2\2\u0782\u0783\7Z\2\2\u0783\u0784\7G\2\2\u0784"+
		"\u0785\7U\2\2\u0785\u01ae\3\2\2\2\u0786\u0787\7N\2\2\u0787\u0788\7Q\2"+
		"\2\u0788\u0789\7E\2\2\u0789\u078a\7M\2\2\u078a\u078b\7U\2\2\u078b\u01b0"+
		"\3\2\2\2\u078c\u078d\7Q\2\2\u078d\u078e\7R\2\2\u078e\u078f\7V\2\2\u078f"+
		"\u0790\7K\2\2\u0790\u0791\7Q\2\2\u0791\u0792\7P\2\2\u0792\u01b2\3\2\2"+
		"\2\u0793\u0794\7C\2\2\u0794\u0795\7P\2\2\u0795\u0796\7V\2\2\u0796\u0797"+
		"\7K\2\2\u0797\u01b4\3\2\2\2\u0798\u0799\7N\2\2\u0799\u079a\7Q\2\2\u079a"+
		"\u079b\7E\2\2\u079b\u079c\7C\2\2\u079c\u079d\7N\2\2\u079d\u01b6\3\2\2"+
		"\2\u079e\u079f\7K\2\2\u079f\u07a0\7P\2\2\u07a0\u07a1\7R\2\2\u07a1\u07a2"+
		"\7C\2\2\u07a2\u07a3\7V\2\2\u07a3\u07a4\7J\2\2\u07a4\u01b8\3\2\2\2\u07a5"+
		"\u07a6\7U\2\2\u07a6\u07a7\7K\2\2\u07a7\u07a8\7P\2\2\u07a8\u07a9\7F\2\2"+
		"\u07a9\u07aa\7G\2\2\u07aa\u07ab\7Z\2\2\u07ab\u01ba\3\2\2\2\u07ac\u07b2"+
		"\7)\2\2\u07ad\u07b1\n\2\2\2\u07ae\u07af\7^\2\2\u07af\u07b1\13\2\2\2\u07b0"+
		"\u07ad\3\2\2\2\u07b0\u07ae\3\2\2\2\u07b1\u07b4\3\2\2\2\u07b2\u07b0\3\2"+
		"\2\2\u07b2\u07b3\3\2\2\2\u07b3\u07b5\3\2\2\2\u07b4\u07b2\3\2\2\2\u07b5"+
		"\u07c1\7)\2\2\u07b6\u07bc\7$\2\2\u07b7\u07bb\n\3\2\2\u07b8\u07b9\7^\2"+
		"\2\u07b9\u07bb\13\2\2\2\u07ba\u07b7\3\2\2\2\u07ba\u07b8\3\2\2\2\u07bb"+
		"\u07be\3\2\2\2\u07bc\u07ba\3\2\2\2\u07bc\u07bd\3\2\2\2\u07bd\u07bf\3\2"+
		"\2\2\u07be\u07bc\3\2\2\2\u07bf\u07c1\7$\2\2\u07c0\u07ac\3\2\2\2\u07c0"+
		"\u07b6\3\2\2\2\u07c1\u01bc\3\2\2\2\u07c2\u07c4\5\u01d3\u00ea\2\u07c3\u07c2"+
		"\3\2\2\2\u07c4\u07c5\3\2\2\2\u07c5\u07c3\3\2\2\2\u07c5\u07c6\3\2\2\2\u07c6"+
		"\u07c7\3\2\2\2\u07c7\u07c8\7N\2\2\u07c8\u01be\3\2\2\2\u07c9\u07cb\5\u01d3"+
		"\u00ea\2\u07ca\u07c9\3\2\2\2\u07cb\u07cc\3\2\2\2\u07cc\u07ca\3\2\2\2\u07cc"+
		"\u07cd\3\2\2\2\u07cd\u07ce\3\2\2\2\u07ce\u07cf\7U\2\2\u07cf\u01c0\3\2"+
		"\2\2\u07d0\u07d2\5\u01d3\u00ea\2\u07d1\u07d0\3\2\2\2\u07d2\u07d3\3\2\2"+
		"\2\u07d3\u07d1\3\2\2\2\u07d3\u07d4\3\2\2\2\u07d4\u07d5\3\2\2\2\u07d5\u07d6"+
		"\7[\2\2\u07d6\u01c2\3\2\2\2\u07d7\u07d9\5\u01d3\u00ea\2\u07d8\u07d7\3"+
		"\2\2\2\u07d9\u07da\3\2\2\2\u07da\u07d8\3\2\2\2\u07da\u07db\3\2\2\2\u07db"+
		"\u07dc\3\2\2\2\u07dc\u07dd\t\4\2\2\u07dd\u01c4\3\2\2\2\u07de\u07e0\5\u01d3"+
		"\u00ea\2\u07df\u07de\3\2\2\2\u07e0\u07e1\3\2\2\2\u07e1\u07df\3\2\2\2\u07e1"+
		"\u07e2\3\2\2\2\u07e2\u01c6\3\2\2\2\u07e3\u07e5\5\u01d3\u00ea\2\u07e4\u07e3"+
		"\3\2\2\2\u07e5\u07e6\3\2\2\2\u07e6\u07e4\3\2\2\2\u07e6\u07e7\3\2\2\2\u07e7"+
		"\u07e8\3\2\2\2\u07e8\u07ec\7\60\2\2\u07e9\u07eb\5\u01d3\u00ea\2\u07ea"+
		"\u07e9\3\2\2\2\u07eb\u07ee\3\2\2\2\u07ec\u07ea\3\2\2\2\u07ec\u07ed\3\2"+
		"\2\2\u07ed\u07f6\3\2\2\2\u07ee\u07ec\3\2\2\2\u07ef\u07f1\7\60\2\2\u07f0"+
		"\u07f2\5\u01d3\u00ea\2\u07f1\u07f0\3\2\2\2\u07f2\u07f3\3\2\2\2\u07f3\u07f1"+
		"\3\2\2\2\u07f3\u07f4\3\2\2\2\u07f4\u07f6\3\2\2\2\u07f5\u07e4\3\2\2\2\u07f5"+
		"\u07ef\3\2\2\2\u07f6\u01c8\3\2\2\2\u07f7\u07f9\5\u01d3\u00ea\2\u07f8\u07f7"+
		"\3\2\2\2\u07f9\u07fa\3\2\2\2\u07fa\u07f8\3\2\2\2\u07fa\u07fb\3\2\2\2\u07fb"+
		"\u0803\3\2\2\2\u07fc\u0800\7\60\2\2\u07fd\u07ff\5\u01d3\u00ea\2\u07fe"+
		"\u07fd\3\2\2\2\u07ff\u0802\3\2\2\2\u0800\u07fe\3\2\2\2\u0800\u0801\3\2"+
		"\2\2\u0801\u0804\3\2\2\2\u0802\u0800\3\2\2\2\u0803\u07fc\3\2\2\2\u0803"+
		"\u0804\3\2\2\2\u0804\u0805\3\2\2\2\u0805\u0806\5\u01d1\u00e9\2\u0806\u0810"+
		"\3\2\2\2\u0807\u0809\7\60\2\2\u0808\u080a\5\u01d3\u00ea\2\u0809\u0808"+
		"\3\2\2\2\u080a\u080b\3\2\2\2\u080b\u0809\3\2\2\2\u080b\u080c\3\2\2\2\u080c"+
		"\u080d\3\2\2\2\u080d\u080e\5\u01d1\u00e9\2\u080e\u0810\3\2\2\2\u080f\u07f8"+
		"\3\2\2\2\u080f\u0807\3\2\2\2\u0810\u01ca\3\2\2\2\u0811\u0815\5\u01c5\u00e3"+
		"\2\u0812\u0815\5\u01c7\u00e4\2\u0813\u0815\5\u01c9\u00e5\2\u0814\u0811"+
		"\3\2\2\2\u0814\u0812\3\2\2\2\u0814\u0813\3\2\2\2\u0815\u0816\3\2\2\2\u0816"+
		"\u0817\7F\2\2\u0817\u01cc\3\2\2\2\u0818\u081c\5\u01d5\u00eb\2\u0819\u081c"+
		"\5\u01d3\u00ea\2\u081a\u081c\7a\2\2\u081b\u0818\3\2\2\2\u081b\u0819\3"+
		"\2\2\2\u081b\u081a\3\2\2\2\u081c\u081d\3\2\2\2\u081d\u081b\3\2\2\2\u081d"+
		"\u081e\3\2\2\2\u081e\u01ce\3\2\2\2\u081f\u0825\7b\2\2\u0820\u0824\n\5"+
		"\2\2\u0821\u0822\7b\2\2\u0822\u0824\7b\2\2\u0823\u0820\3\2\2\2\u0823\u0821"+
		"\3\2\2\2\u0824\u0827\3\2\2\2\u0825\u0823\3\2\2\2\u0825\u0826\3\2\2\2\u0826"+
		"\u0828\3\2\2\2\u0827\u0825\3\2\2\2\u0828\u0829\7b\2\2\u0829\u01d0\3\2"+
		"\2\2\u082a\u082c\7G\2\2\u082b\u082d\t\6\2\2\u082c\u082b\3\2\2\2\u082c"+
		"\u082d\3\2\2\2\u082d\u082f\3\2\2\2\u082e\u0830\5\u01d3\u00ea\2\u082f\u082e"+
		"\3\2\2\2\u0830\u0831\3\2\2\2\u0831\u082f\3\2\2\2\u0831\u0832\3\2\2\2\u0832"+
		"\u01d2\3\2\2\2\u0833\u0834\t\7\2\2\u0834\u01d4\3\2\2\2\u0835\u0836\t\b"+
		"\2\2\u0836\u01d6\3\2\2\2\u0837\u0838\7/\2\2\u0838\u0839\7/\2\2\u0839\u083d"+
		"\3\2\2\2\u083a\u083c\n\t\2\2\u083b\u083a\3\2\2\2\u083c\u083f\3\2\2\2\u083d"+
		"\u083b\3\2\2\2\u083d\u083e\3\2\2\2\u083e\u0841\3\2\2\2\u083f\u083d\3\2"+
		"\2\2\u0840\u0842\7\17\2\2\u0841\u0840\3\2\2\2\u0841\u0842\3\2\2\2\u0842"+
		"\u0844\3\2\2\2\u0843\u0845\7\f\2\2\u0844\u0843\3\2\2\2\u0844\u0845\3\2"+
		"\2\2\u0845\u0846\3\2\2\2\u0846\u0847\b\u00ec\2\2\u0847\u01d8\3\2\2\2\u0848"+
		"\u0849\7\61\2\2\u0849\u084a\7,\2\2\u084a\u084e\3\2\2\2\u084b\u084d\13"+
		"\2\2\2\u084c\u084b\3\2\2\2\u084d\u0850\3\2\2\2\u084e\u084f\3\2\2\2\u084e"+
		"\u084c\3\2\2\2\u084f\u0851\3\2\2\2\u0850\u084e\3\2\2\2\u0851\u0852\7,"+
		"\2\2\u0852\u0853\7\61\2\2\u0853\u0854\3\2\2\2\u0854\u0855\b\u00ed\2\2"+
		"\u0855\u01da\3\2\2\2\u0856\u0858\t\n\2\2\u0857\u0856\3\2\2\2\u0858\u0859"+
		"\3\2\2\2\u0859\u0857\3\2\2\2\u0859\u085a\3\2\2\2\u085a\u085b\3\2\2\2\u085b"+
		"\u085c\b\u00ee\2\2\u085c\u01dc\3\2\2\2\u085d\u085e\13\2\2\2\u085e\u01de"+
		"\3\2\2\2*\2\u025a\u027e\u0482\u0494\u049c\u05c7\u06b3\u06c5\u07b0\u07b2"+
		"\u07ba\u07bc\u07c0\u07c5\u07cc\u07d3\u07da\u07e1\u07e6\u07ec\u07f3\u07f5"+
		"\u07fa\u0800\u0803\u080b\u080f\u0814\u081b\u081d\u0823\u0825\u082c\u0831"+
		"\u083d\u0841\u0844\u084e\u0859\3\2\3\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}