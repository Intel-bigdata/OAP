// Generated from C:/Users/gefeili/Documents/gefei/IdeaProject/Spinach/sql/catalyst/src/main/antlr4/org/apache/spark/sql/catalyst/parser\SqlBase.g4 by ANTLR 4.6
package org.apache.spark.sql.catalyst.parser;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class SqlBaseParser extends Parser {
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
		SIMPLE_COMMENT=232, BRACKETED_COMMENT=233, WS=234, UNRECOGNIZED=235, DELIMITER=236;
	public static final int
		RULE_singleStatement = 0, RULE_singleExpression = 1, RULE_singleTableIdentifier = 2, 
		RULE_singleDataType = 3, RULE_statement = 4, RULE_indexCols = 5, RULE_indexCol = 6, 
		RULE_unsupportedHiveNativeCommands = 7, RULE_createTableHeader = 8, RULE_bucketSpec = 9, 
		RULE_skewSpec = 10, RULE_locationSpec = 11, RULE_query = 12, RULE_insertInto = 13, 
		RULE_partitionSpecLocation = 14, RULE_partitionSpec = 15, RULE_partitionVal = 16, 
		RULE_describeFuncName = 17, RULE_describeColName = 18, RULE_ctes = 19, 
		RULE_namedQuery = 20, RULE_tableProvider = 21, RULE_tablePropertyList = 22, 
		RULE_tableProperty = 23, RULE_tablePropertyKey = 24, RULE_constantList = 25, 
		RULE_nestedConstantList = 26, RULE_createFileFormat = 27, RULE_fileFormat = 28, 
		RULE_storageHandler = 29, RULE_resource = 30, RULE_queryNoWith = 31, RULE_queryOrganization = 32, 
		RULE_multiInsertQueryBody = 33, RULE_queryTerm = 34, RULE_queryPrimary = 35, 
		RULE_sortItem = 36, RULE_querySpecification = 37, RULE_fromClause = 38, 
		RULE_aggregation = 39, RULE_groupingSet = 40, RULE_lateralView = 41, RULE_setQuantifier = 42, 
		RULE_relation = 43, RULE_joinType = 44, RULE_joinCriteria = 45, RULE_sample = 46, 
		RULE_identifierList = 47, RULE_identifierSeq = 48, RULE_orderedIdentifierList = 49, 
		RULE_orderedIdentifier = 50, RULE_identifierCommentList = 51, RULE_identifierComment = 52, 
		RULE_relationPrimary = 53, RULE_inlineTable = 54, RULE_rowFormat = 55, 
		RULE_tableIdentifier = 56, RULE_namedExpression = 57, RULE_namedExpressionSeq = 58, 
		RULE_expression = 59, RULE_booleanExpression = 60, RULE_predicated = 61, 
		RULE_predicate = 62, RULE_valueExpression = 63, RULE_primaryExpression = 64, 
		RULE_constant = 65, RULE_comparisonOperator = 66, RULE_arithmeticOperator = 67, 
		RULE_predicateOperator = 68, RULE_booleanValue = 69, RULE_interval = 70, 
		RULE_intervalField = 71, RULE_intervalValue = 72, RULE_dataType = 73, 
		RULE_colTypeList = 74, RULE_colType = 75, RULE_whenClause = 76, RULE_windows = 77, 
		RULE_namedWindow = 78, RULE_windowSpec = 79, RULE_windowFrame = 80, RULE_frameBound = 81, 
		RULE_qualifiedName = 82, RULE_identifier = 83, RULE_strictIdentifier = 84, 
		RULE_quotedIdentifier = 85, RULE_number = 86, RULE_nonReserved = 87;
	public static final String[] ruleNames = {
		"singleStatement", "singleExpression", "singleTableIdentifier", "singleDataType", 
		"statement", "indexCols", "indexCol", "unsupportedHiveNativeCommands", 
		"createTableHeader", "bucketSpec", "skewSpec", "locationSpec", "query", 
		"insertInto", "partitionSpecLocation", "partitionSpec", "partitionVal", 
		"describeFuncName", "describeColName", "ctes", "namedQuery", "tableProvider", 
		"tablePropertyList", "tableProperty", "tablePropertyKey", "constantList", 
		"nestedConstantList", "createFileFormat", "fileFormat", "storageHandler", 
		"resource", "queryNoWith", "queryOrganization", "multiInsertQueryBody", 
		"queryTerm", "queryPrimary", "sortItem", "querySpecification", "fromClause", 
		"aggregation", "groupingSet", "lateralView", "setQuantifier", "relation", 
		"joinType", "joinCriteria", "sample", "identifierList", "identifierSeq", 
		"orderedIdentifierList", "orderedIdentifier", "identifierCommentList", 
		"identifierComment", "relationPrimary", "inlineTable", "rowFormat", "tableIdentifier", 
		"namedExpression", "namedExpressionSeq", "expression", "booleanExpression", 
		"predicated", "predicate", "valueExpression", "primaryExpression", "constant", 
		"comparisonOperator", "arithmeticOperator", "predicateOperator", "booleanValue", 
		"interval", "intervalField", "intervalValue", "dataType", "colTypeList", 
		"colType", "whenClause", "windows", "namedWindow", "windowSpec", "windowFrame", 
		"frameBound", "qualifiedName", "identifier", "strictIdentifier", "quotedIdentifier", 
		"number", "nonReserved"
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
		"BRACKETED_COMMENT", "WS", "UNRECOGNIZED", "DELIMITER"
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

	@Override
	public String getGrammarFileName() { return "SqlBase.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public SqlBaseParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}
	public static class SingleStatementContext extends ParserRuleContext {
		public StatementContext statement() {
			return getRuleContext(StatementContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public SingleStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSingleStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSingleStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSingleStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SingleStatementContext singleStatement() throws RecognitionException {
		SingleStatementContext _localctx = new SingleStatementContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_singleStatement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(176);
			statement();
			setState(177);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SingleExpressionContext extends ParserRuleContext {
		public NamedExpressionContext namedExpression() {
			return getRuleContext(NamedExpressionContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public SingleExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSingleExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSingleExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSingleExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SingleExpressionContext singleExpression() throws RecognitionException {
		SingleExpressionContext _localctx = new SingleExpressionContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_singleExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(179);
			namedExpression();
			setState(180);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SingleTableIdentifierContext extends ParserRuleContext {
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public SingleTableIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleTableIdentifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSingleTableIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSingleTableIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSingleTableIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SingleTableIdentifierContext singleTableIdentifier() throws RecognitionException {
		SingleTableIdentifierContext _localctx = new SingleTableIdentifierContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_singleTableIdentifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(182);
			tableIdentifier();
			setState(183);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SingleDataTypeContext extends ParserRuleContext {
		public DataTypeContext dataType() {
			return getRuleContext(DataTypeContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public SingleDataTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleDataType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSingleDataType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSingleDataType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSingleDataType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SingleDataTypeContext singleDataType() throws RecognitionException {
		SingleDataTypeContext _localctx = new SingleDataTypeContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_singleDataType);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(185);
			dataType();
			setState(186);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class StatementContext extends ParserRuleContext {
		public StatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_statement; }
	 
		public StatementContext() { }
		public void copyFrom(StatementContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class ExplainContext extends StatementContext {
		public TerminalNode EXPLAIN() { return getToken(SqlBaseParser.EXPLAIN, 0); }
		public StatementContext statement() {
			return getRuleContext(StatementContext.class,0);
		}
		public TerminalNode LOGICAL() { return getToken(SqlBaseParser.LOGICAL, 0); }
		public TerminalNode FORMATTED() { return getToken(SqlBaseParser.FORMATTED, 0); }
		public TerminalNode EXTENDED() { return getToken(SqlBaseParser.EXTENDED, 0); }
		public TerminalNode CODEGEN() { return getToken(SqlBaseParser.CODEGEN, 0); }
		public ExplainContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExplain(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExplain(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExplain(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SetDatabasePropertiesContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode DATABASE() { return getToken(SqlBaseParser.DATABASE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode DBPROPERTIES() { return getToken(SqlBaseParser.DBPROPERTIES, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public SetDatabasePropertiesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetDatabaseProperties(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetDatabaseProperties(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetDatabaseProperties(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SpinachDropIndexContext extends StatementContext {
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode SINDEX() { return getToken(SqlBaseParser.SINDEX, 0); }
		public TerminalNode IDENTIFIER() { return getToken(SqlBaseParser.IDENTIFIER, 0); }
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public SpinachDropIndexContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSpinachDropIndex(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSpinachDropIndex(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSpinachDropIndex(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DropDatabaseContext extends StatementContext {
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode DATABASE() { return getToken(SqlBaseParser.DATABASE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode RESTRICT() { return getToken(SqlBaseParser.RESTRICT, 0); }
		public TerminalNode CASCADE() { return getToken(SqlBaseParser.CASCADE, 0); }
		public DropDatabaseContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDropDatabase(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDropDatabase(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDropDatabase(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class CreateTableContext extends StatementContext {
		public ColTypeListContext columns;
		public ColTypeListContext partitionColumns;
		public CreateTableHeaderContext createTableHeader() {
			return getRuleContext(CreateTableHeaderContext.class,0);
		}
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode PARTITIONED() { return getToken(SqlBaseParser.PARTITIONED, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public BucketSpecContext bucketSpec() {
			return getRuleContext(BucketSpecContext.class,0);
		}
		public SkewSpecContext skewSpec() {
			return getRuleContext(SkewSpecContext.class,0);
		}
		public RowFormatContext rowFormat() {
			return getRuleContext(RowFormatContext.class,0);
		}
		public CreateFileFormatContext createFileFormat() {
			return getRuleContext(CreateFileFormatContext.class,0);
		}
		public LocationSpecContext locationSpec() {
			return getRuleContext(LocationSpecContext.class,0);
		}
		public TerminalNode TBLPROPERTIES() { return getToken(SqlBaseParser.TBLPROPERTIES, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public List<ColTypeListContext> colTypeList() {
			return getRuleContexts(ColTypeListContext.class);
		}
		public ColTypeListContext colTypeList(int i) {
			return getRuleContext(ColTypeListContext.class,i);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public CreateTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ResetConfigurationContext extends StatementContext {
		public TerminalNode RESET() { return getToken(SqlBaseParser.RESET, 0); }
		public ResetConfigurationContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterResetConfiguration(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitResetConfiguration(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitResetConfiguration(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DescribeDatabaseContext extends StatementContext {
		public TerminalNode DATABASE() { return getToken(SqlBaseParser.DATABASE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public TerminalNode DESCRIBE() { return getToken(SqlBaseParser.DESCRIBE, 0); }
		public TerminalNode EXTENDED() { return getToken(SqlBaseParser.EXTENDED, 0); }
		public DescribeDatabaseContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDescribeDatabase(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDescribeDatabase(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDescribeDatabase(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class AlterViewQueryContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public AlterViewQueryContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAlterViewQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAlterViewQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitAlterViewQuery(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DescribeTableContext extends StatementContext {
		public Token option;
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public TerminalNode DESCRIBE() { return getToken(SqlBaseParser.DESCRIBE, 0); }
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public DescribeColNameContext describeColName() {
			return getRuleContext(DescribeColNameContext.class,0);
		}
		public TerminalNode EXTENDED() { return getToken(SqlBaseParser.EXTENDED, 0); }
		public TerminalNode FORMATTED() { return getToken(SqlBaseParser.FORMATTED, 0); }
		public DescribeTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDescribeTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDescribeTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDescribeTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class UseContext extends StatementContext {
		public IdentifierContext db;
		public TerminalNode USE() { return getToken(SqlBaseParser.USE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public UseContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUse(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUse(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUse(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class CreateTempViewUsingContext extends StatementContext {
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode TEMPORARY() { return getToken(SqlBaseParser.TEMPORARY, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TableProviderContext tableProvider() {
			return getRuleContext(TableProviderContext.class,0);
		}
		public TerminalNode OR() { return getToken(SqlBaseParser.OR, 0); }
		public TerminalNode REPLACE() { return getToken(SqlBaseParser.REPLACE, 0); }
		public ColTypeListContext colTypeList() {
			return getRuleContext(ColTypeListContext.class,0);
		}
		public TerminalNode OPTIONS() { return getToken(SqlBaseParser.OPTIONS, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public CreateTempViewUsingContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateTempViewUsing(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateTempViewUsing(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateTempViewUsing(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class CreateTableLikeContext extends StatementContext {
		public TableIdentifierContext target;
		public TableIdentifierContext source;
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public List<TableIdentifierContext> tableIdentifier() {
			return getRuleContexts(TableIdentifierContext.class);
		}
		public TableIdentifierContext tableIdentifier(int i) {
			return getRuleContext(TableIdentifierContext.class,i);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public CreateTableLikeContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateTableLike(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateTableLike(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateTableLike(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class RenameTableContext extends StatementContext {
		public TableIdentifierContext from;
		public TableIdentifierContext to;
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode RENAME() { return getToken(SqlBaseParser.RENAME, 0); }
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public List<TableIdentifierContext> tableIdentifier() {
			return getRuleContexts(TableIdentifierContext.class);
		}
		public TableIdentifierContext tableIdentifier(int i) {
			return getRuleContext(TableIdentifierContext.class,i);
		}
		public RenameTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRenameTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRenameTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRenameTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class UncacheTableContext extends StatementContext {
		public TerminalNode UNCACHE() { return getToken(SqlBaseParser.UNCACHE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public UncacheTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUncacheTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUncacheTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUncacheTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SpinachCreateIndexContext extends StatementContext {
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode SINDEX() { return getToken(SqlBaseParser.SINDEX, 0); }
		public TerminalNode IDENTIFIER() { return getToken(SqlBaseParser.IDENTIFIER, 0); }
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public IndexColsContext indexCols() {
			return getRuleContext(IndexColsContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public SpinachCreateIndexContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSpinachCreateIndex(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSpinachCreateIndex(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSpinachCreateIndex(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DropFunctionContext extends StatementContext {
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode FUNCTION() { return getToken(SqlBaseParser.FUNCTION, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode TEMPORARY() { return getToken(SqlBaseParser.TEMPORARY, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public DropFunctionContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDropFunction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDropFunction(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDropFunction(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class FailNativeCommandContext extends StatementContext {
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode ROLE() { return getToken(SqlBaseParser.ROLE, 0); }
		public UnsupportedHiveNativeCommandsContext unsupportedHiveNativeCommands() {
			return getRuleContext(UnsupportedHiveNativeCommandsContext.class,0);
		}
		public FailNativeCommandContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterFailNativeCommand(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitFailNativeCommand(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitFailNativeCommand(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class LoadDataContext extends StatementContext {
		public Token path;
		public TerminalNode LOAD() { return getToken(SqlBaseParser.LOAD, 0); }
		public TerminalNode DATA() { return getToken(SqlBaseParser.DATA, 0); }
		public TerminalNode INPATH() { return getToken(SqlBaseParser.INPATH, 0); }
		public TerminalNode INTO() { return getToken(SqlBaseParser.INTO, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode LOCAL() { return getToken(SqlBaseParser.LOCAL, 0); }
		public TerminalNode OVERWRITE() { return getToken(SqlBaseParser.OVERWRITE, 0); }
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public LoadDataContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLoadData(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLoadData(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLoadData(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ShowPartitionsContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode PARTITIONS() { return getToken(SqlBaseParser.PARTITIONS, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public ShowPartitionsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowPartitions(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowPartitions(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowPartitions(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DescribeFunctionContext extends StatementContext {
		public TerminalNode FUNCTION() { return getToken(SqlBaseParser.FUNCTION, 0); }
		public DescribeFuncNameContext describeFuncName() {
			return getRuleContext(DescribeFuncNameContext.class,0);
		}
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public TerminalNode DESCRIBE() { return getToken(SqlBaseParser.DESCRIBE, 0); }
		public TerminalNode EXTENDED() { return getToken(SqlBaseParser.EXTENDED, 0); }
		public DescribeFunctionContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDescribeFunction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDescribeFunction(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDescribeFunction(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ClearCacheContext extends StatementContext {
		public TerminalNode CLEAR() { return getToken(SqlBaseParser.CLEAR, 0); }
		public TerminalNode CACHE() { return getToken(SqlBaseParser.CACHE, 0); }
		public ClearCacheContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterClearCache(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitClearCache(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitClearCache(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ShowTablesContext extends StatementContext {
		public IdentifierContext db;
		public Token pattern;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode TABLES() { return getToken(SqlBaseParser.TABLES, 0); }
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public ShowTablesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowTables(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowTables(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowTables(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class StatementDefaultContext extends StatementContext {
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public StatementDefaultContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterStatementDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitStatementDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitStatementDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class RenameTablePartitionContext extends StatementContext {
		public PartitionSpecContext from;
		public PartitionSpecContext to;
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode RENAME() { return getToken(SqlBaseParser.RENAME, 0); }
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public List<PartitionSpecContext> partitionSpec() {
			return getRuleContexts(PartitionSpecContext.class);
		}
		public PartitionSpecContext partitionSpec(int i) {
			return getRuleContext(PartitionSpecContext.class,i);
		}
		public RenameTablePartitionContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRenameTablePartition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRenameTablePartition(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRenameTablePartition(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class RefreshResourceContext extends StatementContext {
		public TerminalNode REFRESH() { return getToken(SqlBaseParser.REFRESH, 0); }
		public RefreshResourceContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRefreshResource(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRefreshResource(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRefreshResource(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class TruncateTableContext extends StatementContext {
		public TerminalNode TRUNCATE() { return getToken(SqlBaseParser.TRUNCATE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public TruncateTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTruncateTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTruncateTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTruncateTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SetTableSerDeContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode SERDE() { return getToken(SqlBaseParser.SERDE, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode SERDEPROPERTIES() { return getToken(SqlBaseParser.SERDEPROPERTIES, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public SetTableSerDeContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetTableSerDe(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetTableSerDe(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetTableSerDe(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class CreateViewContext extends StatementContext {
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode OR() { return getToken(SqlBaseParser.OR, 0); }
		public TerminalNode REPLACE() { return getToken(SqlBaseParser.REPLACE, 0); }
		public TerminalNode TEMPORARY() { return getToken(SqlBaseParser.TEMPORARY, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public IdentifierCommentListContext identifierCommentList() {
			return getRuleContext(IdentifierCommentListContext.class,0);
		}
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode PARTITIONED() { return getToken(SqlBaseParser.PARTITIONED, 0); }
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public IdentifierListContext identifierList() {
			return getRuleContext(IdentifierListContext.class,0);
		}
		public TerminalNode TBLPROPERTIES() { return getToken(SqlBaseParser.TBLPROPERTIES, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public CreateViewContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateView(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateView(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateView(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DropTablePartitionsContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public List<PartitionSpecContext> partitionSpec() {
			return getRuleContexts(PartitionSpecContext.class);
		}
		public PartitionSpecContext partitionSpec(int i) {
			return getRuleContext(PartitionSpecContext.class,i);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode PURGE() { return getToken(SqlBaseParser.PURGE, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public DropTablePartitionsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDropTablePartitions(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDropTablePartitions(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDropTablePartitions(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ShowCreateTableContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public ShowCreateTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowCreateTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowCreateTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowCreateTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SetConfigurationContext extends StatementContext {
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public SetConfigurationContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetConfiguration(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetConfiguration(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetConfiguration(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DropTableContext extends StatementContext {
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode PURGE() { return getToken(SqlBaseParser.PURGE, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public DropTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDropTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDropTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDropTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ShowColumnsContext extends StatementContext {
		public IdentifierContext db;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public List<TerminalNode> FROM() { return getTokens(SqlBaseParser.FROM); }
		public TerminalNode FROM(int i) {
			return getToken(SqlBaseParser.FROM, i);
		}
		public List<TerminalNode> IN() { return getTokens(SqlBaseParser.IN); }
		public TerminalNode IN(int i) {
			return getToken(SqlBaseParser.IN, i);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ShowColumnsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowColumns(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowColumns(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowColumns(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class CreateTableUsingContext extends StatementContext {
		public IdentifierListContext partitionColumnNames;
		public CreateTableHeaderContext createTableHeader() {
			return getRuleContext(CreateTableHeaderContext.class,0);
		}
		public TableProviderContext tableProvider() {
			return getRuleContext(TableProviderContext.class,0);
		}
		public ColTypeListContext colTypeList() {
			return getRuleContext(ColTypeListContext.class,0);
		}
		public TerminalNode OPTIONS() { return getToken(SqlBaseParser.OPTIONS, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public TerminalNode PARTITIONED() { return getToken(SqlBaseParser.PARTITIONED, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public BucketSpecContext bucketSpec() {
			return getRuleContext(BucketSpecContext.class,0);
		}
		public IdentifierListContext identifierList() {
			return getRuleContext(IdentifierListContext.class,0);
		}
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public CreateTableUsingContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateTableUsing(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateTableUsing(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateTableUsing(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ShowDatabasesContext extends StatementContext {
		public Token pattern;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode DATABASES() { return getToken(SqlBaseParser.DATABASES, 0); }
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public ShowDatabasesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowDatabases(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowDatabases(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowDatabases(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class AddTablePartitionContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode ADD() { return getToken(SqlBaseParser.ADD, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public List<PartitionSpecLocationContext> partitionSpecLocation() {
			return getRuleContexts(PartitionSpecLocationContext.class);
		}
		public PartitionSpecLocationContext partitionSpecLocation(int i) {
			return getRuleContext(PartitionSpecLocationContext.class,i);
		}
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public List<PartitionSpecContext> partitionSpec() {
			return getRuleContexts(PartitionSpecContext.class);
		}
		public PartitionSpecContext partitionSpec(int i) {
			return getRuleContext(PartitionSpecContext.class,i);
		}
		public AddTablePartitionContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAddTablePartition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAddTablePartition(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitAddTablePartition(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class RefreshTableContext extends StatementContext {
		public TerminalNode REFRESH() { return getToken(SqlBaseParser.REFRESH, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public RefreshTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRefreshTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRefreshTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRefreshTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SpinachRefreshIndicesContext extends StatementContext {
		public TerminalNode REFRESH() { return getToken(SqlBaseParser.REFRESH, 0); }
		public TerminalNode SINDEX() { return getToken(SqlBaseParser.SINDEX, 0); }
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public SpinachRefreshIndicesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSpinachRefreshIndices(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSpinachRefreshIndices(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSpinachRefreshIndices(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ManageResourceContext extends StatementContext {
		public Token op;
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode ADD() { return getToken(SqlBaseParser.ADD, 0); }
		public TerminalNode LIST() { return getToken(SqlBaseParser.LIST, 0); }
		public ManageResourceContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterManageResource(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitManageResource(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitManageResource(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class CreateDatabaseContext extends StatementContext {
		public Token comment;
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode DATABASE() { return getToken(SqlBaseParser.DATABASE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public LocationSpecContext locationSpec() {
			return getRuleContext(LocationSpecContext.class,0);
		}
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode DBPROPERTIES() { return getToken(SqlBaseParser.DBPROPERTIES, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public CreateDatabaseContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateDatabase(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateDatabase(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateDatabase(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ShowTblPropertiesContext extends StatementContext {
		public TableIdentifierContext table;
		public TablePropertyKeyContext key;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode TBLPROPERTIES() { return getToken(SqlBaseParser.TBLPROPERTIES, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TablePropertyKeyContext tablePropertyKey() {
			return getRuleContext(TablePropertyKeyContext.class,0);
		}
		public ShowTblPropertiesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowTblProperties(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowTblProperties(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowTblProperties(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class AnalyzeContext extends StatementContext {
		public TerminalNode ANALYZE() { return getToken(SqlBaseParser.ANALYZE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode COMPUTE() { return getToken(SqlBaseParser.COMPUTE, 0); }
		public TerminalNode STATISTICS() { return getToken(SqlBaseParser.STATISTICS, 0); }
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode FOR() { return getToken(SqlBaseParser.FOR, 0); }
		public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
		public IdentifierSeqContext identifierSeq() {
			return getRuleContext(IdentifierSeqContext.class,0);
		}
		public AnalyzeContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAnalyze(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAnalyze(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitAnalyze(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class UnsetTablePropertiesContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode UNSET() { return getToken(SqlBaseParser.UNSET, 0); }
		public TerminalNode TBLPROPERTIES() { return getToken(SqlBaseParser.TBLPROPERTIES, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public UnsetTablePropertiesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUnsetTableProperties(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUnsetTableProperties(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUnsetTableProperties(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SetTableLocationContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public LocationSpecContext locationSpec() {
			return getRuleContext(LocationSpecContext.class,0);
		}
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public SetTableLocationContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetTableLocation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetTableLocation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetTableLocation(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class CreateFunctionContext extends StatementContext {
		public Token className;
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode FUNCTION() { return getToken(SqlBaseParser.FUNCTION, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode TEMPORARY() { return getToken(SqlBaseParser.TEMPORARY, 0); }
		public TerminalNode USING() { return getToken(SqlBaseParser.USING, 0); }
		public List<ResourceContext> resource() {
			return getRuleContexts(ResourceContext.class);
		}
		public ResourceContext resource(int i) {
			return getRuleContext(ResourceContext.class,i);
		}
		public CreateFunctionContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateFunction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateFunction(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateFunction(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ShowFunctionsContext extends StatementContext {
		public Token pattern;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode FUNCTIONS() { return getToken(SqlBaseParser.FUNCTIONS, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public ShowFunctionsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowFunctions(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowFunctions(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowFunctions(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class CacheTableContext extends StatementContext {
		public TerminalNode CACHE() { return getToken(SqlBaseParser.CACHE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode LAZY() { return getToken(SqlBaseParser.LAZY, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public CacheTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCacheTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCacheTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCacheTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SetTablePropertiesContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode TBLPROPERTIES() { return getToken(SqlBaseParser.TBLPROPERTIES, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public SetTablePropertiesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetTableProperties(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetTableProperties(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetTableProperties(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StatementContext statement() throws RecognitionException {
		StatementContext _localctx = new StatementContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_statement);
		int _la;
		try {
			int _alt;
			setState(766);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,93,_ctx) ) {
			case 1:
				_localctx = new StatementDefaultContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(188);
				query();
				}
				break;
			case 2:
				_localctx = new UseContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(189);
				match(USE);
				setState(190);
				((UseContext)_localctx).db = identifier();
				}
				break;
			case 3:
				_localctx = new CreateDatabaseContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(191);
				match(CREATE);
				setState(192);
				match(DATABASE);
				setState(196);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,0,_ctx) ) {
				case 1:
					{
					setState(193);
					match(IF);
					setState(194);
					match(NOT);
					setState(195);
					match(EXISTS);
					}
					break;
				}
				setState(198);
				identifier();
				setState(201);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMENT) {
					{
					setState(199);
					match(COMMENT);
					setState(200);
					((CreateDatabaseContext)_localctx).comment = match(STRING);
					}
				}

				setState(204);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LOCATION) {
					{
					setState(203);
					locationSpec();
					}
				}

				setState(209);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(206);
					match(WITH);
					setState(207);
					match(DBPROPERTIES);
					setState(208);
					tablePropertyList();
					}
				}

				}
				break;
			case 4:
				_localctx = new SetDatabasePropertiesContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(211);
				match(ALTER);
				setState(212);
				match(DATABASE);
				setState(213);
				identifier();
				setState(214);
				match(SET);
				setState(215);
				match(DBPROPERTIES);
				setState(216);
				tablePropertyList();
				}
				break;
			case 5:
				_localctx = new DropDatabaseContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(218);
				match(DROP);
				setState(219);
				match(DATABASE);
				setState(222);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,4,_ctx) ) {
				case 1:
					{
					setState(220);
					match(IF);
					setState(221);
					match(EXISTS);
					}
					break;
				}
				setState(224);
				identifier();
				setState(226);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==CASCADE || _la==RESTRICT) {
					{
					setState(225);
					_la = _input.LA(1);
					if ( !(_la==CASCADE || _la==RESTRICT) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

				}
				break;
			case 6:
				_localctx = new CreateTableUsingContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(228);
				createTableHeader();
				setState(233);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__0) {
					{
					setState(229);
					match(T__0);
					setState(230);
					colTypeList();
					setState(231);
					match(T__1);
					}
				}

				setState(235);
				tableProvider();
				setState(238);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OPTIONS) {
					{
					setState(236);
					match(OPTIONS);
					setState(237);
					tablePropertyList();
					}
				}

				setState(243);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITIONED) {
					{
					setState(240);
					match(PARTITIONED);
					setState(241);
					match(BY);
					setState(242);
					((CreateTableUsingContext)_localctx).partitionColumnNames = identifierList();
					}
				}

				setState(246);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==CLUSTERED) {
					{
					setState(245);
					bucketSpec();
					}
				}

				}
				break;
			case 7:
				_localctx = new CreateTableUsingContext(_localctx);
				enterOuterAlt(_localctx, 7);
				{
				setState(248);
				createTableHeader();
				setState(249);
				tableProvider();
				setState(252);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OPTIONS) {
					{
					setState(250);
					match(OPTIONS);
					setState(251);
					tablePropertyList();
					}
				}

				setState(257);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITIONED) {
					{
					setState(254);
					match(PARTITIONED);
					setState(255);
					match(BY);
					setState(256);
					((CreateTableUsingContext)_localctx).partitionColumnNames = identifierList();
					}
				}

				setState(260);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==CLUSTERED) {
					{
					setState(259);
					bucketSpec();
					}
				}

				setState(263);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==AS) {
					{
					setState(262);
					match(AS);
					}
				}

				setState(265);
				query();
				}
				break;
			case 8:
				_localctx = new CreateTableContext(_localctx);
				enterOuterAlt(_localctx, 8);
				{
				setState(267);
				createTableHeader();
				setState(272);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,14,_ctx) ) {
				case 1:
					{
					setState(268);
					match(T__0);
					setState(269);
					((CreateTableContext)_localctx).columns = colTypeList();
					setState(270);
					match(T__1);
					}
					break;
				}
				setState(276);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMENT) {
					{
					setState(274);
					match(COMMENT);
					setState(275);
					match(STRING);
					}
				}

				setState(284);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITIONED) {
					{
					setState(278);
					match(PARTITIONED);
					setState(279);
					match(BY);
					setState(280);
					match(T__0);
					setState(281);
					((CreateTableContext)_localctx).partitionColumns = colTypeList();
					setState(282);
					match(T__1);
					}
				}

				setState(287);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==CLUSTERED) {
					{
					setState(286);
					bucketSpec();
					}
				}

				setState(290);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SKEWED) {
					{
					setState(289);
					skewSpec();
					}
				}

				setState(293);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ROW) {
					{
					setState(292);
					rowFormat();
					}
				}

				setState(296);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==STORED) {
					{
					setState(295);
					createFileFormat();
					}
				}

				setState(299);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LOCATION) {
					{
					setState(298);
					locationSpec();
					}
				}

				setState(303);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==TBLPROPERTIES) {
					{
					setState(301);
					match(TBLPROPERTIES);
					setState(302);
					tablePropertyList();
					}
				}

				setState(309);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << SELECT) | (1L << FROM) | (1L << AS))) != 0) || ((((_la - 69)) & ~0x3f) == 0 && ((1L << (_la - 69)) & ((1L << (WITH - 69)) | (1L << (VALUES - 69)) | (1L << (TABLE - 69)) | (1L << (INSERT - 69)) | (1L << (MAP - 69)))) != 0) || _la==REDUCE) {
					{
					setState(306);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==AS) {
						{
						setState(305);
						match(AS);
						}
					}

					setState(308);
					query();
					}
				}

				}
				break;
			case 9:
				_localctx = new CreateTableLikeContext(_localctx);
				enterOuterAlt(_localctx, 9);
				{
				setState(311);
				match(CREATE);
				setState(312);
				match(TABLE);
				setState(316);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,25,_ctx) ) {
				case 1:
					{
					setState(313);
					match(IF);
					setState(314);
					match(NOT);
					setState(315);
					match(EXISTS);
					}
					break;
				}
				setState(318);
				((CreateTableLikeContext)_localctx).target = tableIdentifier();
				setState(319);
				match(LIKE);
				setState(320);
				((CreateTableLikeContext)_localctx).source = tableIdentifier();
				}
				break;
			case 10:
				_localctx = new AnalyzeContext(_localctx);
				enterOuterAlt(_localctx, 10);
				{
				setState(322);
				match(ANALYZE);
				setState(323);
				match(TABLE);
				setState(324);
				tableIdentifier();
				setState(326);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(325);
					partitionSpec();
					}
				}

				setState(328);
				match(COMPUTE);
				setState(329);
				match(STATISTICS);
				setState(336);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,28,_ctx) ) {
				case 1:
					{
					setState(330);
					identifier();
					}
					break;
				case 2:
					{
					setState(331);
					match(FOR);
					setState(332);
					match(COLUMNS);
					setState(334);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << SELECT) | (1L << FROM) | (1L << ADD) | (1L << AS) | (1L << ALL) | (1L << DISTINCT) | (1L << WHERE) | (1L << GROUP) | (1L << BY) | (1L << GROUPING) | (1L << SETS) | (1L << CUBE) | (1L << ROLLUP) | (1L << ORDER) | (1L << HAVING) | (1L << LIMIT) | (1L << AT) | (1L << OR) | (1L << AND) | (1L << IN) | (1L << NOT) | (1L << NO) | (1L << EXISTS) | (1L << BETWEEN) | (1L << LIKE) | (1L << RLIKE) | (1L << IS) | (1L << NULL) | (1L << TRUE) | (1L << FALSE) | (1L << NULLS) | (1L << ASC) | (1L << DESC) | (1L << FOR) | (1L << INTERVAL) | (1L << CASE) | (1L << WHEN) | (1L << THEN) | (1L << ELSE) | (1L << END) | (1L << JOIN) | (1L << CROSS) | (1L << OUTER) | (1L << INNER) | (1L << LEFT) | (1L << SEMI) | (1L << RIGHT) | (1L << FULL) | (1L << NATURAL) | (1L << ON) | (1L << LATERAL) | (1L << WINDOW) | (1L << OVER) | (1L << PARTITION) | (1L << RANGE) | (1L << ROWS))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (UNBOUNDED - 64)) | (1L << (PRECEDING - 64)) | (1L << (FOLLOWING - 64)) | (1L << (CURRENT - 64)) | (1L << (ROW - 64)) | (1L << (WITH - 64)) | (1L << (VALUES - 64)) | (1L << (CREATE - 64)) | (1L << (TABLE - 64)) | (1L << (VIEW - 64)) | (1L << (REPLACE - 64)) | (1L << (INSERT - 64)) | (1L << (DELETE - 64)) | (1L << (INTO - 64)) | (1L << (DESCRIBE - 64)) | (1L << (EXPLAIN - 64)) | (1L << (FORMAT - 64)) | (1L << (LOGICAL - 64)) | (1L << (CODEGEN - 64)) | (1L << (CAST - 64)) | (1L << (SHOW - 64)) | (1L << (TABLES - 64)) | (1L << (COLUMNS - 64)) | (1L << (COLUMN - 64)) | (1L << (USE - 64)) | (1L << (PARTITIONS - 64)) | (1L << (FUNCTIONS - 64)) | (1L << (DROP - 64)) | (1L << (UNION - 64)) | (1L << (EXCEPT - 64)) | (1L << (INTERSECT - 64)) | (1L << (TO - 64)) | (1L << (TABLESAMPLE - 64)) | (1L << (STRATIFY - 64)) | (1L << (ALTER - 64)) | (1L << (RENAME - 64)) | (1L << (ARRAY - 64)) | (1L << (MAP - 64)) | (1L << (STRUCT - 64)) | (1L << (COMMENT - 64)) | (1L << (SET - 64)) | (1L << (RESET - 64)) | (1L << (DATA - 64)) | (1L << (START - 64)) | (1L << (TRANSACTION - 64)) | (1L << (COMMIT - 64)) | (1L << (ROLLBACK - 64)) | (1L << (MACRO - 64)) | (1L << (IF - 64)) | (1L << (DIV - 64)))) != 0) || ((((_la - 131)) & ~0x3f) == 0 && ((1L << (_la - 131)) & ((1L << (PERCENTLIT - 131)) | (1L << (BUCKET - 131)) | (1L << (OUT - 131)) | (1L << (OF - 131)) | (1L << (SORT - 131)) | (1L << (CLUSTER - 131)) | (1L << (DISTRIBUTE - 131)) | (1L << (OVERWRITE - 131)) | (1L << (TRANSFORM - 131)) | (1L << (REDUCE - 131)) | (1L << (USING - 131)) | (1L << (SERDE - 131)) | (1L << (SERDEPROPERTIES - 131)) | (1L << (RECORDREADER - 131)) | (1L << (RECORDWRITER - 131)) | (1L << (DELIMITED - 131)) | (1L << (FIELDS - 131)) | (1L << (TERMINATED - 131)) | (1L << (COLLECTION - 131)) | (1L << (ITEMS - 131)) | (1L << (KEYS - 131)) | (1L << (ESCAPED - 131)) | (1L << (LINES - 131)) | (1L << (SEPARATED - 131)) | (1L << (FUNCTION - 131)) | (1L << (EXTENDED - 131)) | (1L << (REFRESH - 131)) | (1L << (CLEAR - 131)) | (1L << (CACHE - 131)) | (1L << (UNCACHE - 131)) | (1L << (LAZY - 131)) | (1L << (FORMATTED - 131)) | (1L << (TEMPORARY - 131)) | (1L << (OPTIONS - 131)) | (1L << (UNSET - 131)) | (1L << (TBLPROPERTIES - 131)) | (1L << (DBPROPERTIES - 131)) | (1L << (BUCKETS - 131)) | (1L << (SKEWED - 131)) | (1L << (STORED - 131)) | (1L << (DIRECTORIES - 131)) | (1L << (LOCATION - 131)) | (1L << (EXCHANGE - 131)) | (1L << (ARCHIVE - 131)) | (1L << (UNARCHIVE - 131)) | (1L << (FILEFORMAT - 131)) | (1L << (TOUCH - 131)) | (1L << (COMPACT - 131)) | (1L << (CONCATENATE - 131)) | (1L << (CHANGE - 131)) | (1L << (CASCADE - 131)) | (1L << (RESTRICT - 131)) | (1L << (CLUSTERED - 131)) | (1L << (SORTED - 131)) | (1L << (PURGE - 131)) | (1L << (INPUTFORMAT - 131)) | (1L << (OUTPUTFORMAT - 131)) | (1L << (DATABASE - 131)) | (1L << (DATABASES - 131)) | (1L << (DFS - 131)) | (1L << (TRUNCATE - 131)) | (1L << (ANALYZE - 131)) | (1L << (COMPUTE - 131)) | (1L << (LIST - 131)))) != 0) || ((((_la - 195)) & ~0x3f) == 0 && ((1L << (_la - 195)) & ((1L << (STATISTICS - 195)) | (1L << (PARTITIONED - 195)) | (1L << (EXTERNAL - 195)) | (1L << (DEFINED - 195)) | (1L << (REVOKE - 195)) | (1L << (GRANT - 195)) | (1L << (LOCK - 195)) | (1L << (UNLOCK - 195)) | (1L << (MSCK - 195)) | (1L << (REPAIR - 195)) | (1L << (EXPORT - 195)) | (1L << (IMPORT - 195)) | (1L << (LOAD - 195)) | (1L << (ROLE - 195)) | (1L << (ROLES - 195)) | (1L << (COMPACTIONS - 195)) | (1L << (PRINCIPALS - 195)) | (1L << (TRANSACTIONS - 195)) | (1L << (INDEX - 195)) | (1L << (INDEXES - 195)) | (1L << (LOCKS - 195)) | (1L << (OPTION - 195)) | (1L << (ANTI - 195)) | (1L << (LOCAL - 195)) | (1L << (INPATH - 195)) | (1L << (IDENTIFIER - 195)) | (1L << (BACKQUOTED_IDENTIFIER - 195)))) != 0)) {
						{
						setState(333);
						identifierSeq();
						}
					}

					}
					break;
				}
				}
				break;
			case 11:
				_localctx = new RenameTableContext(_localctx);
				enterOuterAlt(_localctx, 11);
				{
				setState(338);
				match(ALTER);
				setState(339);
				_la = _input.LA(1);
				if ( !(_la==TABLE || _la==VIEW) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(340);
				((RenameTableContext)_localctx).from = tableIdentifier();
				setState(341);
				match(RENAME);
				setState(342);
				match(TO);
				setState(343);
				((RenameTableContext)_localctx).to = tableIdentifier();
				}
				break;
			case 12:
				_localctx = new SetTablePropertiesContext(_localctx);
				enterOuterAlt(_localctx, 12);
				{
				setState(345);
				match(ALTER);
				setState(346);
				_la = _input.LA(1);
				if ( !(_la==TABLE || _la==VIEW) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(347);
				tableIdentifier();
				setState(348);
				match(SET);
				setState(349);
				match(TBLPROPERTIES);
				setState(350);
				tablePropertyList();
				}
				break;
			case 13:
				_localctx = new UnsetTablePropertiesContext(_localctx);
				enterOuterAlt(_localctx, 13);
				{
				setState(352);
				match(ALTER);
				setState(353);
				_la = _input.LA(1);
				if ( !(_la==TABLE || _la==VIEW) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(354);
				tableIdentifier();
				setState(355);
				match(UNSET);
				setState(356);
				match(TBLPROPERTIES);
				setState(359);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IF) {
					{
					setState(357);
					match(IF);
					setState(358);
					match(EXISTS);
					}
				}

				setState(361);
				tablePropertyList();
				}
				break;
			case 14:
				_localctx = new SetTableSerDeContext(_localctx);
				enterOuterAlt(_localctx, 14);
				{
				setState(363);
				match(ALTER);
				setState(364);
				match(TABLE);
				setState(365);
				tableIdentifier();
				setState(367);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(366);
					partitionSpec();
					}
				}

				setState(369);
				match(SET);
				setState(370);
				match(SERDE);
				setState(371);
				match(STRING);
				setState(375);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(372);
					match(WITH);
					setState(373);
					match(SERDEPROPERTIES);
					setState(374);
					tablePropertyList();
					}
				}

				}
				break;
			case 15:
				_localctx = new SetTableSerDeContext(_localctx);
				enterOuterAlt(_localctx, 15);
				{
				setState(377);
				match(ALTER);
				setState(378);
				match(TABLE);
				setState(379);
				tableIdentifier();
				setState(381);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(380);
					partitionSpec();
					}
				}

				setState(383);
				match(SET);
				setState(384);
				match(SERDEPROPERTIES);
				setState(385);
				tablePropertyList();
				}
				break;
			case 16:
				_localctx = new AddTablePartitionContext(_localctx);
				enterOuterAlt(_localctx, 16);
				{
				setState(387);
				match(ALTER);
				setState(388);
				match(TABLE);
				setState(389);
				tableIdentifier();
				setState(390);
				match(ADD);
				setState(394);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IF) {
					{
					setState(391);
					match(IF);
					setState(392);
					match(NOT);
					setState(393);
					match(EXISTS);
					}
				}

				setState(397); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(396);
					partitionSpecLocation();
					}
					}
					setState(399); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==PARTITION );
				}
				break;
			case 17:
				_localctx = new AddTablePartitionContext(_localctx);
				enterOuterAlt(_localctx, 17);
				{
				setState(401);
				match(ALTER);
				setState(402);
				match(VIEW);
				setState(403);
				tableIdentifier();
				setState(404);
				match(ADD);
				setState(408);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IF) {
					{
					setState(405);
					match(IF);
					setState(406);
					match(NOT);
					setState(407);
					match(EXISTS);
					}
				}

				setState(411); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(410);
					partitionSpec();
					}
					}
					setState(413); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==PARTITION );
				}
				break;
			case 18:
				_localctx = new RenameTablePartitionContext(_localctx);
				enterOuterAlt(_localctx, 18);
				{
				setState(415);
				match(ALTER);
				setState(416);
				match(TABLE);
				setState(417);
				tableIdentifier();
				setState(418);
				((RenameTablePartitionContext)_localctx).from = partitionSpec();
				setState(419);
				match(RENAME);
				setState(420);
				match(TO);
				setState(421);
				((RenameTablePartitionContext)_localctx).to = partitionSpec();
				}
				break;
			case 19:
				_localctx = new DropTablePartitionsContext(_localctx);
				enterOuterAlt(_localctx, 19);
				{
				setState(423);
				match(ALTER);
				setState(424);
				match(TABLE);
				setState(425);
				tableIdentifier();
				setState(426);
				match(DROP);
				setState(429);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IF) {
					{
					setState(427);
					match(IF);
					setState(428);
					match(EXISTS);
					}
				}

				setState(431);
				partitionSpec();
				setState(436);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(432);
					match(T__2);
					setState(433);
					partitionSpec();
					}
					}
					setState(438);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(440);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PURGE) {
					{
					setState(439);
					match(PURGE);
					}
				}

				}
				break;
			case 20:
				_localctx = new DropTablePartitionsContext(_localctx);
				enterOuterAlt(_localctx, 20);
				{
				setState(442);
				match(ALTER);
				setState(443);
				match(VIEW);
				setState(444);
				tableIdentifier();
				setState(445);
				match(DROP);
				setState(448);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IF) {
					{
					setState(446);
					match(IF);
					setState(447);
					match(EXISTS);
					}
				}

				setState(450);
				partitionSpec();
				setState(455);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(451);
					match(T__2);
					setState(452);
					partitionSpec();
					}
					}
					setState(457);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case 21:
				_localctx = new SetTableLocationContext(_localctx);
				enterOuterAlt(_localctx, 21);
				{
				setState(458);
				match(ALTER);
				setState(459);
				match(TABLE);
				setState(460);
				tableIdentifier();
				setState(462);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(461);
					partitionSpec();
					}
				}

				setState(464);
				match(SET);
				setState(465);
				locationSpec();
				}
				break;
			case 22:
				_localctx = new DropTableContext(_localctx);
				enterOuterAlt(_localctx, 22);
				{
				setState(467);
				match(DROP);
				setState(468);
				match(TABLE);
				setState(471);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,43,_ctx) ) {
				case 1:
					{
					setState(469);
					match(IF);
					setState(470);
					match(EXISTS);
					}
					break;
				}
				setState(473);
				tableIdentifier();
				setState(475);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PURGE) {
					{
					setState(474);
					match(PURGE);
					}
				}

				}
				break;
			case 23:
				_localctx = new DropTableContext(_localctx);
				enterOuterAlt(_localctx, 23);
				{
				setState(477);
				match(DROP);
				setState(478);
				match(VIEW);
				setState(481);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,45,_ctx) ) {
				case 1:
					{
					setState(479);
					match(IF);
					setState(480);
					match(EXISTS);
					}
					break;
				}
				setState(483);
				tableIdentifier();
				}
				break;
			case 24:
				_localctx = new CreateViewContext(_localctx);
				enterOuterAlt(_localctx, 24);
				{
				setState(484);
				match(CREATE);
				setState(487);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OR) {
					{
					setState(485);
					match(OR);
					setState(486);
					match(REPLACE);
					}
				}

				setState(490);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==TEMPORARY) {
					{
					setState(489);
					match(TEMPORARY);
					}
				}

				setState(492);
				match(VIEW);
				setState(496);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,48,_ctx) ) {
				case 1:
					{
					setState(493);
					match(IF);
					setState(494);
					match(NOT);
					setState(495);
					match(EXISTS);
					}
					break;
				}
				setState(498);
				tableIdentifier();
				setState(500);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__0) {
					{
					setState(499);
					identifierCommentList();
					}
				}

				setState(504);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMENT) {
					{
					setState(502);
					match(COMMENT);
					setState(503);
					match(STRING);
					}
				}

				setState(509);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITIONED) {
					{
					setState(506);
					match(PARTITIONED);
					setState(507);
					match(ON);
					setState(508);
					identifierList();
					}
				}

				setState(513);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==TBLPROPERTIES) {
					{
					setState(511);
					match(TBLPROPERTIES);
					setState(512);
					tablePropertyList();
					}
				}

				setState(515);
				match(AS);
				setState(516);
				query();
				}
				break;
			case 25:
				_localctx = new CreateTempViewUsingContext(_localctx);
				enterOuterAlt(_localctx, 25);
				{
				setState(518);
				match(CREATE);
				setState(521);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OR) {
					{
					setState(519);
					match(OR);
					setState(520);
					match(REPLACE);
					}
				}

				setState(523);
				match(TEMPORARY);
				setState(524);
				match(VIEW);
				setState(525);
				tableIdentifier();
				setState(530);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__0) {
					{
					setState(526);
					match(T__0);
					setState(527);
					colTypeList();
					setState(528);
					match(T__1);
					}
				}

				setState(532);
				tableProvider();
				setState(535);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OPTIONS) {
					{
					setState(533);
					match(OPTIONS);
					setState(534);
					tablePropertyList();
					}
				}

				}
				break;
			case 26:
				_localctx = new AlterViewQueryContext(_localctx);
				enterOuterAlt(_localctx, 26);
				{
				setState(537);
				match(ALTER);
				setState(538);
				match(VIEW);
				setState(539);
				tableIdentifier();
				setState(541);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==AS) {
					{
					setState(540);
					match(AS);
					}
				}

				setState(543);
				query();
				}
				break;
			case 27:
				_localctx = new CreateFunctionContext(_localctx);
				enterOuterAlt(_localctx, 27);
				{
				setState(545);
				match(CREATE);
				setState(547);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==TEMPORARY) {
					{
					setState(546);
					match(TEMPORARY);
					}
				}

				setState(549);
				match(FUNCTION);
				setState(550);
				qualifiedName();
				setState(551);
				match(AS);
				setState(552);
				((CreateFunctionContext)_localctx).className = match(STRING);
				setState(562);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==USING) {
					{
					setState(553);
					match(USING);
					setState(554);
					resource();
					setState(559);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__2) {
						{
						{
						setState(555);
						match(T__2);
						setState(556);
						resource();
						}
						}
						setState(561);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				}
				break;
			case 28:
				_localctx = new DropFunctionContext(_localctx);
				enterOuterAlt(_localctx, 28);
				{
				setState(564);
				match(DROP);
				setState(566);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==TEMPORARY) {
					{
					setState(565);
					match(TEMPORARY);
					}
				}

				setState(568);
				match(FUNCTION);
				setState(571);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,61,_ctx) ) {
				case 1:
					{
					setState(569);
					match(IF);
					setState(570);
					match(EXISTS);
					}
					break;
				}
				setState(573);
				qualifiedName();
				}
				break;
			case 29:
				_localctx = new ExplainContext(_localctx);
				enterOuterAlt(_localctx, 29);
				{
				setState(574);
				match(EXPLAIN);
				setState(576);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LOGICAL || _la==CODEGEN || _la==EXTENDED || _la==FORMATTED) {
					{
					setState(575);
					_la = _input.LA(1);
					if ( !(_la==LOGICAL || _la==CODEGEN || _la==EXTENDED || _la==FORMATTED) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

				setState(578);
				statement();
				}
				break;
			case 30:
				_localctx = new ShowTablesContext(_localctx);
				enterOuterAlt(_localctx, 30);
				{
				setState(579);
				match(SHOW);
				setState(580);
				match(TABLES);
				setState(583);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FROM || _la==IN) {
					{
					setState(581);
					_la = _input.LA(1);
					if ( !(_la==FROM || _la==IN) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(582);
					((ShowTablesContext)_localctx).db = identifier();
					}
				}

				setState(589);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LIKE || _la==STRING) {
					{
					setState(586);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==LIKE) {
						{
						setState(585);
						match(LIKE);
						}
					}

					setState(588);
					((ShowTablesContext)_localctx).pattern = match(STRING);
					}
				}

				}
				break;
			case 31:
				_localctx = new ShowDatabasesContext(_localctx);
				enterOuterAlt(_localctx, 31);
				{
				setState(591);
				match(SHOW);
				setState(592);
				match(DATABASES);
				setState(595);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LIKE) {
					{
					setState(593);
					match(LIKE);
					setState(594);
					((ShowDatabasesContext)_localctx).pattern = match(STRING);
					}
				}

				}
				break;
			case 32:
				_localctx = new ShowTblPropertiesContext(_localctx);
				enterOuterAlt(_localctx, 32);
				{
				setState(597);
				match(SHOW);
				setState(598);
				match(TBLPROPERTIES);
				setState(599);
				((ShowTblPropertiesContext)_localctx).table = tableIdentifier();
				setState(604);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__0) {
					{
					setState(600);
					match(T__0);
					setState(601);
					((ShowTblPropertiesContext)_localctx).key = tablePropertyKey();
					setState(602);
					match(T__1);
					}
				}

				}
				break;
			case 33:
				_localctx = new ShowColumnsContext(_localctx);
				enterOuterAlt(_localctx, 33);
				{
				setState(606);
				match(SHOW);
				setState(607);
				match(COLUMNS);
				setState(608);
				_la = _input.LA(1);
				if ( !(_la==FROM || _la==IN) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(609);
				tableIdentifier();
				setState(612);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==FROM || _la==IN) {
					{
					setState(610);
					_la = _input.LA(1);
					if ( !(_la==FROM || _la==IN) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(611);
					((ShowColumnsContext)_localctx).db = identifier();
					}
				}

				}
				break;
			case 34:
				_localctx = new ShowPartitionsContext(_localctx);
				enterOuterAlt(_localctx, 34);
				{
				setState(614);
				match(SHOW);
				setState(615);
				match(PARTITIONS);
				setState(616);
				tableIdentifier();
				setState(618);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(617);
					partitionSpec();
					}
				}

				}
				break;
			case 35:
				_localctx = new ShowFunctionsContext(_localctx);
				enterOuterAlt(_localctx, 35);
				{
				setState(620);
				match(SHOW);
				setState(622);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,70,_ctx) ) {
				case 1:
					{
					setState(621);
					identifier();
					}
					break;
				}
				setState(624);
				match(FUNCTIONS);
				setState(632);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << SELECT) | (1L << FROM) | (1L << ADD) | (1L << AS) | (1L << ALL) | (1L << DISTINCT) | (1L << WHERE) | (1L << GROUP) | (1L << BY) | (1L << GROUPING) | (1L << SETS) | (1L << CUBE) | (1L << ROLLUP) | (1L << ORDER) | (1L << HAVING) | (1L << LIMIT) | (1L << AT) | (1L << OR) | (1L << AND) | (1L << IN) | (1L << NOT) | (1L << NO) | (1L << EXISTS) | (1L << BETWEEN) | (1L << LIKE) | (1L << RLIKE) | (1L << IS) | (1L << NULL) | (1L << TRUE) | (1L << FALSE) | (1L << NULLS) | (1L << ASC) | (1L << DESC) | (1L << FOR) | (1L << INTERVAL) | (1L << CASE) | (1L << WHEN) | (1L << THEN) | (1L << ELSE) | (1L << END) | (1L << JOIN) | (1L << CROSS) | (1L << OUTER) | (1L << INNER) | (1L << LEFT) | (1L << SEMI) | (1L << RIGHT) | (1L << FULL) | (1L << NATURAL) | (1L << ON) | (1L << LATERAL) | (1L << WINDOW) | (1L << OVER) | (1L << PARTITION) | (1L << RANGE) | (1L << ROWS))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (UNBOUNDED - 64)) | (1L << (PRECEDING - 64)) | (1L << (FOLLOWING - 64)) | (1L << (CURRENT - 64)) | (1L << (ROW - 64)) | (1L << (WITH - 64)) | (1L << (VALUES - 64)) | (1L << (CREATE - 64)) | (1L << (TABLE - 64)) | (1L << (VIEW - 64)) | (1L << (REPLACE - 64)) | (1L << (INSERT - 64)) | (1L << (DELETE - 64)) | (1L << (INTO - 64)) | (1L << (DESCRIBE - 64)) | (1L << (EXPLAIN - 64)) | (1L << (FORMAT - 64)) | (1L << (LOGICAL - 64)) | (1L << (CODEGEN - 64)) | (1L << (CAST - 64)) | (1L << (SHOW - 64)) | (1L << (TABLES - 64)) | (1L << (COLUMNS - 64)) | (1L << (COLUMN - 64)) | (1L << (USE - 64)) | (1L << (PARTITIONS - 64)) | (1L << (FUNCTIONS - 64)) | (1L << (DROP - 64)) | (1L << (UNION - 64)) | (1L << (EXCEPT - 64)) | (1L << (INTERSECT - 64)) | (1L << (TO - 64)) | (1L << (TABLESAMPLE - 64)) | (1L << (STRATIFY - 64)) | (1L << (ALTER - 64)) | (1L << (RENAME - 64)) | (1L << (ARRAY - 64)) | (1L << (MAP - 64)) | (1L << (STRUCT - 64)) | (1L << (COMMENT - 64)) | (1L << (SET - 64)) | (1L << (RESET - 64)) | (1L << (DATA - 64)) | (1L << (START - 64)) | (1L << (TRANSACTION - 64)) | (1L << (COMMIT - 64)) | (1L << (ROLLBACK - 64)) | (1L << (MACRO - 64)) | (1L << (IF - 64)) | (1L << (DIV - 64)))) != 0) || ((((_la - 131)) & ~0x3f) == 0 && ((1L << (_la - 131)) & ((1L << (PERCENTLIT - 131)) | (1L << (BUCKET - 131)) | (1L << (OUT - 131)) | (1L << (OF - 131)) | (1L << (SORT - 131)) | (1L << (CLUSTER - 131)) | (1L << (DISTRIBUTE - 131)) | (1L << (OVERWRITE - 131)) | (1L << (TRANSFORM - 131)) | (1L << (REDUCE - 131)) | (1L << (USING - 131)) | (1L << (SERDE - 131)) | (1L << (SERDEPROPERTIES - 131)) | (1L << (RECORDREADER - 131)) | (1L << (RECORDWRITER - 131)) | (1L << (DELIMITED - 131)) | (1L << (FIELDS - 131)) | (1L << (TERMINATED - 131)) | (1L << (COLLECTION - 131)) | (1L << (ITEMS - 131)) | (1L << (KEYS - 131)) | (1L << (ESCAPED - 131)) | (1L << (LINES - 131)) | (1L << (SEPARATED - 131)) | (1L << (FUNCTION - 131)) | (1L << (EXTENDED - 131)) | (1L << (REFRESH - 131)) | (1L << (CLEAR - 131)) | (1L << (CACHE - 131)) | (1L << (UNCACHE - 131)) | (1L << (LAZY - 131)) | (1L << (FORMATTED - 131)) | (1L << (TEMPORARY - 131)) | (1L << (OPTIONS - 131)) | (1L << (UNSET - 131)) | (1L << (TBLPROPERTIES - 131)) | (1L << (DBPROPERTIES - 131)) | (1L << (BUCKETS - 131)) | (1L << (SKEWED - 131)) | (1L << (STORED - 131)) | (1L << (DIRECTORIES - 131)) | (1L << (LOCATION - 131)) | (1L << (EXCHANGE - 131)) | (1L << (ARCHIVE - 131)) | (1L << (UNARCHIVE - 131)) | (1L << (FILEFORMAT - 131)) | (1L << (TOUCH - 131)) | (1L << (COMPACT - 131)) | (1L << (CONCATENATE - 131)) | (1L << (CHANGE - 131)) | (1L << (CASCADE - 131)) | (1L << (RESTRICT - 131)) | (1L << (CLUSTERED - 131)) | (1L << (SORTED - 131)) | (1L << (PURGE - 131)) | (1L << (INPUTFORMAT - 131)) | (1L << (OUTPUTFORMAT - 131)) | (1L << (DATABASE - 131)) | (1L << (DATABASES - 131)) | (1L << (DFS - 131)) | (1L << (TRUNCATE - 131)) | (1L << (ANALYZE - 131)) | (1L << (COMPUTE - 131)) | (1L << (LIST - 131)))) != 0) || ((((_la - 195)) & ~0x3f) == 0 && ((1L << (_la - 195)) & ((1L << (STATISTICS - 195)) | (1L << (PARTITIONED - 195)) | (1L << (EXTERNAL - 195)) | (1L << (DEFINED - 195)) | (1L << (REVOKE - 195)) | (1L << (GRANT - 195)) | (1L << (LOCK - 195)) | (1L << (UNLOCK - 195)) | (1L << (MSCK - 195)) | (1L << (REPAIR - 195)) | (1L << (EXPORT - 195)) | (1L << (IMPORT - 195)) | (1L << (LOAD - 195)) | (1L << (ROLE - 195)) | (1L << (ROLES - 195)) | (1L << (COMPACTIONS - 195)) | (1L << (PRINCIPALS - 195)) | (1L << (TRANSACTIONS - 195)) | (1L << (INDEX - 195)) | (1L << (INDEXES - 195)) | (1L << (LOCKS - 195)) | (1L << (OPTION - 195)) | (1L << (ANTI - 195)) | (1L << (LOCAL - 195)) | (1L << (INPATH - 195)) | (1L << (STRING - 195)) | (1L << (IDENTIFIER - 195)) | (1L << (BACKQUOTED_IDENTIFIER - 195)))) != 0)) {
					{
					setState(626);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,71,_ctx) ) {
					case 1:
						{
						setState(625);
						match(LIKE);
						}
						break;
					}
					setState(630);
					_errHandler.sync(this);
					switch (_input.LA(1)) {
					case SELECT:
					case FROM:
					case ADD:
					case AS:
					case ALL:
					case DISTINCT:
					case WHERE:
					case GROUP:
					case BY:
					case GROUPING:
					case SETS:
					case CUBE:
					case ROLLUP:
					case ORDER:
					case HAVING:
					case LIMIT:
					case AT:
					case OR:
					case AND:
					case IN:
					case NOT:
					case NO:
					case EXISTS:
					case BETWEEN:
					case LIKE:
					case RLIKE:
					case IS:
					case NULL:
					case TRUE:
					case FALSE:
					case NULLS:
					case ASC:
					case DESC:
					case FOR:
					case INTERVAL:
					case CASE:
					case WHEN:
					case THEN:
					case ELSE:
					case END:
					case JOIN:
					case CROSS:
					case OUTER:
					case INNER:
					case LEFT:
					case SEMI:
					case RIGHT:
					case FULL:
					case NATURAL:
					case ON:
					case LATERAL:
					case WINDOW:
					case OVER:
					case PARTITION:
					case RANGE:
					case ROWS:
					case UNBOUNDED:
					case PRECEDING:
					case FOLLOWING:
					case CURRENT:
					case ROW:
					case WITH:
					case VALUES:
					case CREATE:
					case TABLE:
					case VIEW:
					case REPLACE:
					case INSERT:
					case DELETE:
					case INTO:
					case DESCRIBE:
					case EXPLAIN:
					case FORMAT:
					case LOGICAL:
					case CODEGEN:
					case CAST:
					case SHOW:
					case TABLES:
					case COLUMNS:
					case COLUMN:
					case USE:
					case PARTITIONS:
					case FUNCTIONS:
					case DROP:
					case UNION:
					case EXCEPT:
					case INTERSECT:
					case TO:
					case TABLESAMPLE:
					case STRATIFY:
					case ALTER:
					case RENAME:
					case ARRAY:
					case MAP:
					case STRUCT:
					case COMMENT:
					case SET:
					case RESET:
					case DATA:
					case START:
					case TRANSACTION:
					case COMMIT:
					case ROLLBACK:
					case MACRO:
					case IF:
					case DIV:
					case PERCENTLIT:
					case BUCKET:
					case OUT:
					case OF:
					case SORT:
					case CLUSTER:
					case DISTRIBUTE:
					case OVERWRITE:
					case TRANSFORM:
					case REDUCE:
					case USING:
					case SERDE:
					case SERDEPROPERTIES:
					case RECORDREADER:
					case RECORDWRITER:
					case DELIMITED:
					case FIELDS:
					case TERMINATED:
					case COLLECTION:
					case ITEMS:
					case KEYS:
					case ESCAPED:
					case LINES:
					case SEPARATED:
					case FUNCTION:
					case EXTENDED:
					case REFRESH:
					case CLEAR:
					case CACHE:
					case UNCACHE:
					case LAZY:
					case FORMATTED:
					case TEMPORARY:
					case OPTIONS:
					case UNSET:
					case TBLPROPERTIES:
					case DBPROPERTIES:
					case BUCKETS:
					case SKEWED:
					case STORED:
					case DIRECTORIES:
					case LOCATION:
					case EXCHANGE:
					case ARCHIVE:
					case UNARCHIVE:
					case FILEFORMAT:
					case TOUCH:
					case COMPACT:
					case CONCATENATE:
					case CHANGE:
					case CASCADE:
					case RESTRICT:
					case CLUSTERED:
					case SORTED:
					case PURGE:
					case INPUTFORMAT:
					case OUTPUTFORMAT:
					case DATABASE:
					case DATABASES:
					case DFS:
					case TRUNCATE:
					case ANALYZE:
					case COMPUTE:
					case LIST:
					case STATISTICS:
					case PARTITIONED:
					case EXTERNAL:
					case DEFINED:
					case REVOKE:
					case GRANT:
					case LOCK:
					case UNLOCK:
					case MSCK:
					case REPAIR:
					case EXPORT:
					case IMPORT:
					case LOAD:
					case ROLE:
					case ROLES:
					case COMPACTIONS:
					case PRINCIPALS:
					case TRANSACTIONS:
					case INDEX:
					case INDEXES:
					case LOCKS:
					case OPTION:
					case ANTI:
					case LOCAL:
					case INPATH:
					case IDENTIFIER:
					case BACKQUOTED_IDENTIFIER:
						{
						setState(628);
						qualifiedName();
						}
						break;
					case STRING:
						{
						setState(629);
						((ShowFunctionsContext)_localctx).pattern = match(STRING);
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					}
				}

				}
				break;
			case 36:
				_localctx = new ShowCreateTableContext(_localctx);
				enterOuterAlt(_localctx, 36);
				{
				setState(634);
				match(SHOW);
				setState(635);
				match(CREATE);
				setState(636);
				match(TABLE);
				setState(637);
				tableIdentifier();
				}
				break;
			case 37:
				_localctx = new DescribeFunctionContext(_localctx);
				enterOuterAlt(_localctx, 37);
				{
				setState(638);
				_la = _input.LA(1);
				if ( !(_la==DESC || _la==DESCRIBE) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(639);
				match(FUNCTION);
				setState(641);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,74,_ctx) ) {
				case 1:
					{
					setState(640);
					match(EXTENDED);
					}
					break;
				}
				setState(643);
				describeFuncName();
				}
				break;
			case 38:
				_localctx = new DescribeDatabaseContext(_localctx);
				enterOuterAlt(_localctx, 38);
				{
				setState(644);
				_la = _input.LA(1);
				if ( !(_la==DESC || _la==DESCRIBE) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(645);
				match(DATABASE);
				setState(647);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,75,_ctx) ) {
				case 1:
					{
					setState(646);
					match(EXTENDED);
					}
					break;
				}
				setState(649);
				identifier();
				}
				break;
			case 39:
				_localctx = new DescribeTableContext(_localctx);
				enterOuterAlt(_localctx, 39);
				{
				setState(650);
				_la = _input.LA(1);
				if ( !(_la==DESC || _la==DESCRIBE) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(652);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,76,_ctx) ) {
				case 1:
					{
					setState(651);
					((DescribeTableContext)_localctx).option = _input.LT(1);
					_la = _input.LA(1);
					if ( !(_la==EXTENDED || _la==FORMATTED) ) {
						((DescribeTableContext)_localctx).option = (Token)_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
					break;
				}
				setState(654);
				tableIdentifier();
				setState(656);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,77,_ctx) ) {
				case 1:
					{
					setState(655);
					partitionSpec();
					}
					break;
				}
				setState(659);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << SELECT) | (1L << FROM) | (1L << ADD) | (1L << AS) | (1L << ALL) | (1L << DISTINCT) | (1L << WHERE) | (1L << GROUP) | (1L << BY) | (1L << GROUPING) | (1L << SETS) | (1L << CUBE) | (1L << ROLLUP) | (1L << ORDER) | (1L << HAVING) | (1L << LIMIT) | (1L << AT) | (1L << OR) | (1L << AND) | (1L << IN) | (1L << NOT) | (1L << NO) | (1L << EXISTS) | (1L << BETWEEN) | (1L << LIKE) | (1L << RLIKE) | (1L << IS) | (1L << NULL) | (1L << TRUE) | (1L << FALSE) | (1L << NULLS) | (1L << ASC) | (1L << DESC) | (1L << FOR) | (1L << INTERVAL) | (1L << CASE) | (1L << WHEN) | (1L << THEN) | (1L << ELSE) | (1L << END) | (1L << JOIN) | (1L << CROSS) | (1L << OUTER) | (1L << INNER) | (1L << LEFT) | (1L << SEMI) | (1L << RIGHT) | (1L << FULL) | (1L << NATURAL) | (1L << ON) | (1L << LATERAL) | (1L << WINDOW) | (1L << OVER) | (1L << PARTITION) | (1L << RANGE) | (1L << ROWS))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (UNBOUNDED - 64)) | (1L << (PRECEDING - 64)) | (1L << (FOLLOWING - 64)) | (1L << (CURRENT - 64)) | (1L << (ROW - 64)) | (1L << (WITH - 64)) | (1L << (VALUES - 64)) | (1L << (CREATE - 64)) | (1L << (TABLE - 64)) | (1L << (VIEW - 64)) | (1L << (REPLACE - 64)) | (1L << (INSERT - 64)) | (1L << (DELETE - 64)) | (1L << (INTO - 64)) | (1L << (DESCRIBE - 64)) | (1L << (EXPLAIN - 64)) | (1L << (FORMAT - 64)) | (1L << (LOGICAL - 64)) | (1L << (CODEGEN - 64)) | (1L << (CAST - 64)) | (1L << (SHOW - 64)) | (1L << (TABLES - 64)) | (1L << (COLUMNS - 64)) | (1L << (COLUMN - 64)) | (1L << (USE - 64)) | (1L << (PARTITIONS - 64)) | (1L << (FUNCTIONS - 64)) | (1L << (DROP - 64)) | (1L << (UNION - 64)) | (1L << (EXCEPT - 64)) | (1L << (INTERSECT - 64)) | (1L << (TO - 64)) | (1L << (TABLESAMPLE - 64)) | (1L << (STRATIFY - 64)) | (1L << (ALTER - 64)) | (1L << (RENAME - 64)) | (1L << (ARRAY - 64)) | (1L << (MAP - 64)) | (1L << (STRUCT - 64)) | (1L << (COMMENT - 64)) | (1L << (SET - 64)) | (1L << (RESET - 64)) | (1L << (DATA - 64)) | (1L << (START - 64)) | (1L << (TRANSACTION - 64)) | (1L << (COMMIT - 64)) | (1L << (ROLLBACK - 64)) | (1L << (MACRO - 64)) | (1L << (IF - 64)) | (1L << (DIV - 64)))) != 0) || ((((_la - 131)) & ~0x3f) == 0 && ((1L << (_la - 131)) & ((1L << (PERCENTLIT - 131)) | (1L << (BUCKET - 131)) | (1L << (OUT - 131)) | (1L << (OF - 131)) | (1L << (SORT - 131)) | (1L << (CLUSTER - 131)) | (1L << (DISTRIBUTE - 131)) | (1L << (OVERWRITE - 131)) | (1L << (TRANSFORM - 131)) | (1L << (REDUCE - 131)) | (1L << (USING - 131)) | (1L << (SERDE - 131)) | (1L << (SERDEPROPERTIES - 131)) | (1L << (RECORDREADER - 131)) | (1L << (RECORDWRITER - 131)) | (1L << (DELIMITED - 131)) | (1L << (FIELDS - 131)) | (1L << (TERMINATED - 131)) | (1L << (COLLECTION - 131)) | (1L << (ITEMS - 131)) | (1L << (KEYS - 131)) | (1L << (ESCAPED - 131)) | (1L << (LINES - 131)) | (1L << (SEPARATED - 131)) | (1L << (FUNCTION - 131)) | (1L << (EXTENDED - 131)) | (1L << (REFRESH - 131)) | (1L << (CLEAR - 131)) | (1L << (CACHE - 131)) | (1L << (UNCACHE - 131)) | (1L << (LAZY - 131)) | (1L << (FORMATTED - 131)) | (1L << (TEMPORARY - 131)) | (1L << (OPTIONS - 131)) | (1L << (UNSET - 131)) | (1L << (TBLPROPERTIES - 131)) | (1L << (DBPROPERTIES - 131)) | (1L << (BUCKETS - 131)) | (1L << (SKEWED - 131)) | (1L << (STORED - 131)) | (1L << (DIRECTORIES - 131)) | (1L << (LOCATION - 131)) | (1L << (EXCHANGE - 131)) | (1L << (ARCHIVE - 131)) | (1L << (UNARCHIVE - 131)) | (1L << (FILEFORMAT - 131)) | (1L << (TOUCH - 131)) | (1L << (COMPACT - 131)) | (1L << (CONCATENATE - 131)) | (1L << (CHANGE - 131)) | (1L << (CASCADE - 131)) | (1L << (RESTRICT - 131)) | (1L << (CLUSTERED - 131)) | (1L << (SORTED - 131)) | (1L << (PURGE - 131)) | (1L << (INPUTFORMAT - 131)) | (1L << (OUTPUTFORMAT - 131)) | (1L << (DATABASE - 131)) | (1L << (DATABASES - 131)) | (1L << (DFS - 131)) | (1L << (TRUNCATE - 131)) | (1L << (ANALYZE - 131)) | (1L << (COMPUTE - 131)) | (1L << (LIST - 131)))) != 0) || ((((_la - 195)) & ~0x3f) == 0 && ((1L << (_la - 195)) & ((1L << (STATISTICS - 195)) | (1L << (PARTITIONED - 195)) | (1L << (EXTERNAL - 195)) | (1L << (DEFINED - 195)) | (1L << (REVOKE - 195)) | (1L << (GRANT - 195)) | (1L << (LOCK - 195)) | (1L << (UNLOCK - 195)) | (1L << (MSCK - 195)) | (1L << (REPAIR - 195)) | (1L << (EXPORT - 195)) | (1L << (IMPORT - 195)) | (1L << (LOAD - 195)) | (1L << (ROLE - 195)) | (1L << (ROLES - 195)) | (1L << (COMPACTIONS - 195)) | (1L << (PRINCIPALS - 195)) | (1L << (TRANSACTIONS - 195)) | (1L << (INDEX - 195)) | (1L << (INDEXES - 195)) | (1L << (LOCKS - 195)) | (1L << (OPTION - 195)) | (1L << (ANTI - 195)) | (1L << (LOCAL - 195)) | (1L << (INPATH - 195)) | (1L << (IDENTIFIER - 195)) | (1L << (BACKQUOTED_IDENTIFIER - 195)))) != 0)) {
					{
					setState(658);
					describeColName();
					}
				}

				}
				break;
			case 40:
				_localctx = new RefreshTableContext(_localctx);
				enterOuterAlt(_localctx, 40);
				{
				setState(661);
				match(REFRESH);
				setState(662);
				match(TABLE);
				setState(663);
				tableIdentifier();
				}
				break;
			case 41:
				_localctx = new SpinachRefreshIndicesContext(_localctx);
				enterOuterAlt(_localctx, 41);
				{
				setState(664);
				match(REFRESH);
				setState(665);
				match(SINDEX);
				setState(666);
				match(ON);
				setState(667);
				tableIdentifier();
				}
				break;
			case 42:
				_localctx = new RefreshResourceContext(_localctx);
				enterOuterAlt(_localctx, 42);
				{
				setState(668);
				match(REFRESH);
				setState(672);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,79,_ctx);
				while ( _alt!=1 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1+1 ) {
						{
						{
						setState(669);
						matchWildcard();
						}
						} 
					}
					setState(674);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,79,_ctx);
				}
				}
				break;
			case 43:
				_localctx = new CacheTableContext(_localctx);
				enterOuterAlt(_localctx, 43);
				{
				setState(675);
				match(CACHE);
				setState(677);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LAZY) {
					{
					setState(676);
					match(LAZY);
					}
				}

				setState(679);
				match(TABLE);
				setState(680);
				tableIdentifier();
				setState(685);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << SELECT) | (1L << FROM) | (1L << AS))) != 0) || ((((_la - 69)) & ~0x3f) == 0 && ((1L << (_la - 69)) & ((1L << (WITH - 69)) | (1L << (VALUES - 69)) | (1L << (TABLE - 69)) | (1L << (INSERT - 69)) | (1L << (MAP - 69)))) != 0) || _la==REDUCE) {
					{
					setState(682);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==AS) {
						{
						setState(681);
						match(AS);
						}
					}

					setState(684);
					query();
					}
				}

				}
				break;
			case 44:
				_localctx = new UncacheTableContext(_localctx);
				enterOuterAlt(_localctx, 44);
				{
				setState(687);
				match(UNCACHE);
				setState(688);
				match(TABLE);
				setState(689);
				tableIdentifier();
				}
				break;
			case 45:
				_localctx = new ClearCacheContext(_localctx);
				enterOuterAlt(_localctx, 45);
				{
				setState(690);
				match(CLEAR);
				setState(691);
				match(CACHE);
				}
				break;
			case 46:
				_localctx = new LoadDataContext(_localctx);
				enterOuterAlt(_localctx, 46);
				{
				setState(692);
				match(LOAD);
				setState(693);
				match(DATA);
				setState(695);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LOCAL) {
					{
					setState(694);
					match(LOCAL);
					}
				}

				setState(697);
				match(INPATH);
				setState(698);
				((LoadDataContext)_localctx).path = match(STRING);
				setState(700);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OVERWRITE) {
					{
					setState(699);
					match(OVERWRITE);
					}
				}

				setState(702);
				match(INTO);
				setState(703);
				match(TABLE);
				setState(704);
				tableIdentifier();
				setState(706);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(705);
					partitionSpec();
					}
				}

				}
				break;
			case 47:
				_localctx = new TruncateTableContext(_localctx);
				enterOuterAlt(_localctx, 47);
				{
				setState(708);
				match(TRUNCATE);
				setState(709);
				match(TABLE);
				setState(710);
				tableIdentifier();
				setState(712);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(711);
					partitionSpec();
					}
				}

				}
				break;
			case 48:
				_localctx = new ManageResourceContext(_localctx);
				enterOuterAlt(_localctx, 48);
				{
				setState(714);
				((ManageResourceContext)_localctx).op = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==ADD || _la==LIST) ) {
					((ManageResourceContext)_localctx).op = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(715);
				identifier();
				setState(719);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,87,_ctx);
				while ( _alt!=1 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1+1 ) {
						{
						{
						setState(716);
						matchWildcard();
						}
						} 
					}
					setState(721);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,87,_ctx);
				}
				}
				break;
			case 49:
				_localctx = new FailNativeCommandContext(_localctx);
				enterOuterAlt(_localctx, 49);
				{
				setState(722);
				match(SET);
				setState(723);
				match(ROLE);
				setState(727);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,88,_ctx);
				while ( _alt!=1 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1+1 ) {
						{
						{
						setState(724);
						matchWildcard();
						}
						} 
					}
					setState(729);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,88,_ctx);
				}
				}
				break;
			case 50:
				_localctx = new SetConfigurationContext(_localctx);
				enterOuterAlt(_localctx, 50);
				{
				setState(730);
				match(SET);
				setState(734);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,89,_ctx);
				while ( _alt!=1 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1+1 ) {
						{
						{
						setState(731);
						matchWildcard();
						}
						} 
					}
					setState(736);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,89,_ctx);
				}
				}
				break;
			case 51:
				_localctx = new ResetConfigurationContext(_localctx);
				enterOuterAlt(_localctx, 51);
				{
				setState(737);
				match(RESET);
				}
				break;
			case 52:
				_localctx = new SpinachCreateIndexContext(_localctx);
				enterOuterAlt(_localctx, 52);
				{
				setState(738);
				match(CREATE);
				setState(739);
				match(SINDEX);
				setState(743);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IF) {
					{
					setState(740);
					match(IF);
					setState(741);
					match(NOT);
					setState(742);
					match(EXISTS);
					}
				}

				setState(745);
				match(IDENTIFIER);
				setState(746);
				match(ON);
				setState(747);
				tableIdentifier();
				setState(748);
				indexCols();
				}
				break;
			case 53:
				_localctx = new SpinachDropIndexContext(_localctx);
				enterOuterAlt(_localctx, 53);
				{
				setState(750);
				match(DROP);
				setState(751);
				match(SINDEX);
				setState(754);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IF) {
					{
					setState(752);
					match(IF);
					setState(753);
					match(EXISTS);
					}
				}

				setState(756);
				match(IDENTIFIER);
				setState(757);
				match(ON);
				setState(758);
				tableIdentifier();
				}
				break;
			case 54:
				_localctx = new FailNativeCommandContext(_localctx);
				enterOuterAlt(_localctx, 54);
				{
				setState(759);
				unsupportedHiveNativeCommands();
				setState(763);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,92,_ctx);
				while ( _alt!=1 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1+1 ) {
						{
						{
						setState(760);
						matchWildcard();
						}
						} 
					}
					setState(765);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,92,_ctx);
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IndexColsContext extends ParserRuleContext {
		public List<IndexColContext> indexCol() {
			return getRuleContexts(IndexColContext.class);
		}
		public IndexColContext indexCol(int i) {
			return getRuleContext(IndexColContext.class,i);
		}
		public IndexColsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_indexCols; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIndexCols(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIndexCols(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIndexCols(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IndexColsContext indexCols() throws RecognitionException {
		IndexColsContext _localctx = new IndexColsContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_indexCols);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(768);
			match(T__0);
			setState(769);
			indexCol();
			setState(774);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(770);
				match(T__2);
				setState(771);
				indexCol();
				}
				}
				setState(776);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(777);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IndexColContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode ASC() { return getToken(SqlBaseParser.ASC, 0); }
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public IndexColContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_indexCol; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIndexCol(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIndexCol(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIndexCol(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IndexColContext indexCol() throws RecognitionException {
		IndexColContext _localctx = new IndexColContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_indexCol);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(779);
			identifier();
			setState(781);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ASC || _la==DESC) {
				{
				setState(780);
				_la = _input.LA(1);
				if ( !(_la==ASC || _la==DESC) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class UnsupportedHiveNativeCommandsContext extends ParserRuleContext {
		public Token kw1;
		public Token kw2;
		public Token kw3;
		public Token kw4;
		public Token kw5;
		public Token kw6;
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode ROLE() { return getToken(SqlBaseParser.ROLE, 0); }
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode GRANT() { return getToken(SqlBaseParser.GRANT, 0); }
		public TerminalNode REVOKE() { return getToken(SqlBaseParser.REVOKE, 0); }
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode PRINCIPALS() { return getToken(SqlBaseParser.PRINCIPALS, 0); }
		public TerminalNode ROLES() { return getToken(SqlBaseParser.ROLES, 0); }
		public TerminalNode CURRENT() { return getToken(SqlBaseParser.CURRENT, 0); }
		public TerminalNode EXPORT() { return getToken(SqlBaseParser.EXPORT, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode IMPORT() { return getToken(SqlBaseParser.IMPORT, 0); }
		public TerminalNode COMPACTIONS() { return getToken(SqlBaseParser.COMPACTIONS, 0); }
		public TerminalNode TRANSACTIONS() { return getToken(SqlBaseParser.TRANSACTIONS, 0); }
		public TerminalNode INDEXES() { return getToken(SqlBaseParser.INDEXES, 0); }
		public TerminalNode LOCKS() { return getToken(SqlBaseParser.LOCKS, 0); }
		public TerminalNode INDEX() { return getToken(SqlBaseParser.INDEX, 0); }
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode LOCK() { return getToken(SqlBaseParser.LOCK, 0); }
		public TerminalNode DATABASE() { return getToken(SqlBaseParser.DATABASE, 0); }
		public TerminalNode UNLOCK() { return getToken(SqlBaseParser.UNLOCK, 0); }
		public TerminalNode TEMPORARY() { return getToken(SqlBaseParser.TEMPORARY, 0); }
		public TerminalNode MACRO() { return getToken(SqlBaseParser.MACRO, 0); }
		public TerminalNode MSCK() { return getToken(SqlBaseParser.MSCK, 0); }
		public TerminalNode REPAIR() { return getToken(SqlBaseParser.REPAIR, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode CLUSTERED() { return getToken(SqlBaseParser.CLUSTERED, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public TerminalNode SORTED() { return getToken(SqlBaseParser.SORTED, 0); }
		public TerminalNode SKEWED() { return getToken(SqlBaseParser.SKEWED, 0); }
		public TerminalNode STORED() { return getToken(SqlBaseParser.STORED, 0); }
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public TerminalNode DIRECTORIES() { return getToken(SqlBaseParser.DIRECTORIES, 0); }
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode LOCATION() { return getToken(SqlBaseParser.LOCATION, 0); }
		public TerminalNode EXCHANGE() { return getToken(SqlBaseParser.EXCHANGE, 0); }
		public TerminalNode PARTITION() { return getToken(SqlBaseParser.PARTITION, 0); }
		public TerminalNode ARCHIVE() { return getToken(SqlBaseParser.ARCHIVE, 0); }
		public TerminalNode UNARCHIVE() { return getToken(SqlBaseParser.UNARCHIVE, 0); }
		public TerminalNode TOUCH() { return getToken(SqlBaseParser.TOUCH, 0); }
		public TerminalNode COMPACT() { return getToken(SqlBaseParser.COMPACT, 0); }
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public TerminalNode CONCATENATE() { return getToken(SqlBaseParser.CONCATENATE, 0); }
		public TerminalNode FILEFORMAT() { return getToken(SqlBaseParser.FILEFORMAT, 0); }
		public TerminalNode ADD() { return getToken(SqlBaseParser.ADD, 0); }
		public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
		public TerminalNode CHANGE() { return getToken(SqlBaseParser.CHANGE, 0); }
		public TerminalNode REPLACE() { return getToken(SqlBaseParser.REPLACE, 0); }
		public TerminalNode START() { return getToken(SqlBaseParser.START, 0); }
		public TerminalNode TRANSACTION() { return getToken(SqlBaseParser.TRANSACTION, 0); }
		public TerminalNode COMMIT() { return getToken(SqlBaseParser.COMMIT, 0); }
		public TerminalNode ROLLBACK() { return getToken(SqlBaseParser.ROLLBACK, 0); }
		public TerminalNode DFS() { return getToken(SqlBaseParser.DFS, 0); }
		public TerminalNode DELETE() { return getToken(SqlBaseParser.DELETE, 0); }
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public UnsupportedHiveNativeCommandsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_unsupportedHiveNativeCommands; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUnsupportedHiveNativeCommands(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUnsupportedHiveNativeCommands(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUnsupportedHiveNativeCommands(this);
			else return visitor.visitChildren(this);
		}
	}

	public final UnsupportedHiveNativeCommandsContext unsupportedHiveNativeCommands() throws RecognitionException {
		UnsupportedHiveNativeCommandsContext _localctx = new UnsupportedHiveNativeCommandsContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_unsupportedHiveNativeCommands);
		int _la;
		try {
			setState(975);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,106,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(783);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(CREATE);
				setState(784);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(ROLE);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(785);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(DROP);
				setState(786);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(ROLE);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(787);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(GRANT);
				setState(789);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,96,_ctx) ) {
				case 1:
					{
					setState(788);
					((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(ROLE);
					}
					break;
				}
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(791);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(REVOKE);
				setState(793);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,97,_ctx) ) {
				case 1:
					{
					setState(792);
					((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(ROLE);
					}
					break;
				}
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(795);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(796);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(GRANT);
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(797);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(798);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(ROLE);
				setState(800);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,98,_ctx) ) {
				case 1:
					{
					setState(799);
					((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(GRANT);
					}
					break;
				}
				}
				break;
			case 7:
				enterOuterAlt(_localctx, 7);
				{
				setState(802);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(803);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(PRINCIPALS);
				}
				break;
			case 8:
				enterOuterAlt(_localctx, 8);
				{
				setState(804);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(805);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(ROLES);
				}
				break;
			case 9:
				enterOuterAlt(_localctx, 9);
				{
				setState(806);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(807);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(CURRENT);
				setState(808);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(ROLES);
				}
				break;
			case 10:
				enterOuterAlt(_localctx, 10);
				{
				setState(809);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(EXPORT);
				setState(810);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				}
				break;
			case 11:
				enterOuterAlt(_localctx, 11);
				{
				setState(811);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(IMPORT);
				setState(812);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				}
				break;
			case 12:
				enterOuterAlt(_localctx, 12);
				{
				setState(813);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(814);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(COMPACTIONS);
				}
				break;
			case 13:
				enterOuterAlt(_localctx, 13);
				{
				setState(815);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(816);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(CREATE);
				setState(817);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(TABLE);
				}
				break;
			case 14:
				enterOuterAlt(_localctx, 14);
				{
				setState(818);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(819);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TRANSACTIONS);
				}
				break;
			case 15:
				enterOuterAlt(_localctx, 15);
				{
				setState(820);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(821);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(INDEXES);
				}
				break;
			case 16:
				enterOuterAlt(_localctx, 16);
				{
				setState(822);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(823);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(LOCKS);
				}
				break;
			case 17:
				enterOuterAlt(_localctx, 17);
				{
				setState(824);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(CREATE);
				setState(825);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(INDEX);
				}
				break;
			case 18:
				enterOuterAlt(_localctx, 18);
				{
				setState(826);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(DROP);
				setState(827);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(INDEX);
				}
				break;
			case 19:
				enterOuterAlt(_localctx, 19);
				{
				setState(828);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(829);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(INDEX);
				}
				break;
			case 20:
				enterOuterAlt(_localctx, 20);
				{
				setState(830);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(LOCK);
				setState(831);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				}
				break;
			case 21:
				enterOuterAlt(_localctx, 21);
				{
				setState(832);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(LOCK);
				setState(833);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(DATABASE);
				}
				break;
			case 22:
				enterOuterAlt(_localctx, 22);
				{
				setState(834);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(UNLOCK);
				setState(835);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				}
				break;
			case 23:
				enterOuterAlt(_localctx, 23);
				{
				setState(836);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(UNLOCK);
				setState(837);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(DATABASE);
				}
				break;
			case 24:
				enterOuterAlt(_localctx, 24);
				{
				setState(838);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(CREATE);
				setState(839);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TEMPORARY);
				setState(840);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(MACRO);
				}
				break;
			case 25:
				enterOuterAlt(_localctx, 25);
				{
				setState(841);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(DROP);
				setState(842);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TEMPORARY);
				setState(843);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(MACRO);
				}
				break;
			case 26:
				enterOuterAlt(_localctx, 26);
				{
				setState(844);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(MSCK);
				setState(845);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(REPAIR);
				setState(846);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(TABLE);
				}
				break;
			case 27:
				enterOuterAlt(_localctx, 27);
				{
				setState(847);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(848);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(849);
				tableIdentifier();
				setState(850);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(NOT);
				setState(851);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(CLUSTERED);
				}
				break;
			case 28:
				enterOuterAlt(_localctx, 28);
				{
				setState(853);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(854);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(855);
				tableIdentifier();
				setState(856);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(CLUSTERED);
				setState(857);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(BY);
				}
				break;
			case 29:
				enterOuterAlt(_localctx, 29);
				{
				setState(859);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(860);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(861);
				tableIdentifier();
				setState(862);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(NOT);
				setState(863);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(SORTED);
				}
				break;
			case 30:
				enterOuterAlt(_localctx, 30);
				{
				setState(865);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(866);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(867);
				tableIdentifier();
				setState(868);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(SKEWED);
				setState(869);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(BY);
				}
				break;
			case 31:
				enterOuterAlt(_localctx, 31);
				{
				setState(871);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(872);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(873);
				tableIdentifier();
				setState(874);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(NOT);
				setState(875);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(SKEWED);
				}
				break;
			case 32:
				enterOuterAlt(_localctx, 32);
				{
				setState(877);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(878);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(879);
				tableIdentifier();
				setState(880);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(NOT);
				setState(881);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(STORED);
				setState(882);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw5 = match(AS);
				setState(883);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw6 = match(DIRECTORIES);
				}
				break;
			case 33:
				enterOuterAlt(_localctx, 33);
				{
				setState(885);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(886);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(887);
				tableIdentifier();
				setState(888);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(SET);
				setState(889);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(SKEWED);
				setState(890);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw5 = match(LOCATION);
				}
				break;
			case 34:
				enterOuterAlt(_localctx, 34);
				{
				setState(892);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(893);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(894);
				tableIdentifier();
				setState(895);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(EXCHANGE);
				setState(896);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(PARTITION);
				}
				break;
			case 35:
				enterOuterAlt(_localctx, 35);
				{
				setState(898);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(899);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(900);
				tableIdentifier();
				setState(901);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(ARCHIVE);
				setState(902);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(PARTITION);
				}
				break;
			case 36:
				enterOuterAlt(_localctx, 36);
				{
				setState(904);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(905);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(906);
				tableIdentifier();
				setState(907);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(UNARCHIVE);
				setState(908);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(PARTITION);
				}
				break;
			case 37:
				enterOuterAlt(_localctx, 37);
				{
				setState(910);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(911);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(912);
				tableIdentifier();
				setState(913);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(TOUCH);
				}
				break;
			case 38:
				enterOuterAlt(_localctx, 38);
				{
				setState(915);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(916);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(917);
				tableIdentifier();
				setState(919);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(918);
					partitionSpec();
					}
				}

				setState(921);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(COMPACT);
				}
				break;
			case 39:
				enterOuterAlt(_localctx, 39);
				{
				setState(923);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(924);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(925);
				tableIdentifier();
				setState(927);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(926);
					partitionSpec();
					}
				}

				setState(929);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(CONCATENATE);
				}
				break;
			case 40:
				enterOuterAlt(_localctx, 40);
				{
				setState(931);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(932);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(933);
				tableIdentifier();
				setState(935);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(934);
					partitionSpec();
					}
				}

				setState(937);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(SET);
				setState(938);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(FILEFORMAT);
				}
				break;
			case 41:
				enterOuterAlt(_localctx, 41);
				{
				setState(940);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(941);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(942);
				tableIdentifier();
				setState(944);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(943);
					partitionSpec();
					}
				}

				setState(946);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(ADD);
				setState(947);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(COLUMNS);
				}
				break;
			case 42:
				enterOuterAlt(_localctx, 42);
				{
				setState(949);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(950);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(951);
				tableIdentifier();
				setState(953);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(952);
					partitionSpec();
					}
				}

				setState(955);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(CHANGE);
				setState(957);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,104,_ctx) ) {
				case 1:
					{
					setState(956);
					((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(COLUMNS);
					}
					break;
				}
				}
				break;
			case 43:
				enterOuterAlt(_localctx, 43);
				{
				setState(959);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(960);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(961);
				tableIdentifier();
				setState(963);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(962);
					partitionSpec();
					}
				}

				setState(965);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(REPLACE);
				setState(966);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(COLUMNS);
				}
				break;
			case 44:
				enterOuterAlt(_localctx, 44);
				{
				setState(968);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(START);
				setState(969);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TRANSACTION);
				}
				break;
			case 45:
				enterOuterAlt(_localctx, 45);
				{
				setState(970);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(COMMIT);
				}
				break;
			case 46:
				enterOuterAlt(_localctx, 46);
				{
				setState(971);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ROLLBACK);
				}
				break;
			case 47:
				enterOuterAlt(_localctx, 47);
				{
				setState(972);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(DFS);
				}
				break;
			case 48:
				enterOuterAlt(_localctx, 48);
				{
				setState(973);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(DELETE);
				setState(974);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(FROM);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CreateTableHeaderContext extends ParserRuleContext {
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode TEMPORARY() { return getToken(SqlBaseParser.TEMPORARY, 0); }
		public TerminalNode EXTERNAL() { return getToken(SqlBaseParser.EXTERNAL, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public CreateTableHeaderContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createTableHeader; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateTableHeader(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateTableHeader(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateTableHeader(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CreateTableHeaderContext createTableHeader() throws RecognitionException {
		CreateTableHeaderContext _localctx = new CreateTableHeaderContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_createTableHeader);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(977);
			match(CREATE);
			setState(979);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==TEMPORARY) {
				{
				setState(978);
				match(TEMPORARY);
				}
			}

			setState(982);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==EXTERNAL) {
				{
				setState(981);
				match(EXTERNAL);
				}
			}

			setState(984);
			match(TABLE);
			setState(988);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,109,_ctx) ) {
			case 1:
				{
				setState(985);
				match(IF);
				setState(986);
				match(NOT);
				setState(987);
				match(EXISTS);
				}
				break;
			}
			setState(990);
			tableIdentifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class BucketSpecContext extends ParserRuleContext {
		public TerminalNode CLUSTERED() { return getToken(SqlBaseParser.CLUSTERED, 0); }
		public List<TerminalNode> BY() { return getTokens(SqlBaseParser.BY); }
		public TerminalNode BY(int i) {
			return getToken(SqlBaseParser.BY, i);
		}
		public IdentifierListContext identifierList() {
			return getRuleContext(IdentifierListContext.class,0);
		}
		public TerminalNode INTO() { return getToken(SqlBaseParser.INTO, 0); }
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public TerminalNode BUCKETS() { return getToken(SqlBaseParser.BUCKETS, 0); }
		public TerminalNode SORTED() { return getToken(SqlBaseParser.SORTED, 0); }
		public OrderedIdentifierListContext orderedIdentifierList() {
			return getRuleContext(OrderedIdentifierListContext.class,0);
		}
		public BucketSpecContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_bucketSpec; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBucketSpec(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBucketSpec(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBucketSpec(this);
			else return visitor.visitChildren(this);
		}
	}

	public final BucketSpecContext bucketSpec() throws RecognitionException {
		BucketSpecContext _localctx = new BucketSpecContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_bucketSpec);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(992);
			match(CLUSTERED);
			setState(993);
			match(BY);
			setState(994);
			identifierList();
			setState(998);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SORTED) {
				{
				setState(995);
				match(SORTED);
				setState(996);
				match(BY);
				setState(997);
				orderedIdentifierList();
				}
			}

			setState(1000);
			match(INTO);
			setState(1001);
			match(INTEGER_VALUE);
			setState(1002);
			match(BUCKETS);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SkewSpecContext extends ParserRuleContext {
		public TerminalNode SKEWED() { return getToken(SqlBaseParser.SKEWED, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public IdentifierListContext identifierList() {
			return getRuleContext(IdentifierListContext.class,0);
		}
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public ConstantListContext constantList() {
			return getRuleContext(ConstantListContext.class,0);
		}
		public NestedConstantListContext nestedConstantList() {
			return getRuleContext(NestedConstantListContext.class,0);
		}
		public TerminalNode STORED() { return getToken(SqlBaseParser.STORED, 0); }
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public TerminalNode DIRECTORIES() { return getToken(SqlBaseParser.DIRECTORIES, 0); }
		public SkewSpecContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_skewSpec; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSkewSpec(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSkewSpec(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSkewSpec(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SkewSpecContext skewSpec() throws RecognitionException {
		SkewSpecContext _localctx = new SkewSpecContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_skewSpec);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1004);
			match(SKEWED);
			setState(1005);
			match(BY);
			setState(1006);
			identifierList();
			setState(1007);
			match(ON);
			setState(1010);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,111,_ctx) ) {
			case 1:
				{
				setState(1008);
				constantList();
				}
				break;
			case 2:
				{
				setState(1009);
				nestedConstantList();
				}
				break;
			}
			setState(1015);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,112,_ctx) ) {
			case 1:
				{
				setState(1012);
				match(STORED);
				setState(1013);
				match(AS);
				setState(1014);
				match(DIRECTORIES);
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class LocationSpecContext extends ParserRuleContext {
		public TerminalNode LOCATION() { return getToken(SqlBaseParser.LOCATION, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public LocationSpecContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_locationSpec; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLocationSpec(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLocationSpec(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLocationSpec(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LocationSpecContext locationSpec() throws RecognitionException {
		LocationSpecContext _localctx = new LocationSpecContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_locationSpec);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1017);
			match(LOCATION);
			setState(1018);
			match(STRING);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QueryContext extends ParserRuleContext {
		public QueryNoWithContext queryNoWith() {
			return getRuleContext(QueryNoWithContext.class,0);
		}
		public CtesContext ctes() {
			return getRuleContext(CtesContext.class,0);
		}
		public QueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_query; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQuery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryContext query() throws RecognitionException {
		QueryContext _localctx = new QueryContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_query);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1021);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITH) {
				{
				setState(1020);
				ctes();
				}
			}

			setState(1023);
			queryNoWith();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class InsertIntoContext extends ParserRuleContext {
		public TerminalNode INSERT() { return getToken(SqlBaseParser.INSERT, 0); }
		public TerminalNode OVERWRITE() { return getToken(SqlBaseParser.OVERWRITE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode INTO() { return getToken(SqlBaseParser.INTO, 0); }
		public InsertIntoContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_insertInto; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterInsertInto(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitInsertInto(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitInsertInto(this);
			else return visitor.visitChildren(this);
		}
	}

	public final InsertIntoContext insertInto() throws RecognitionException {
		InsertIntoContext _localctx = new InsertIntoContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_insertInto);
		int _la;
		try {
			setState(1046);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,118,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1025);
				match(INSERT);
				setState(1026);
				match(OVERWRITE);
				setState(1027);
				match(TABLE);
				setState(1028);
				tableIdentifier();
				setState(1035);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(1029);
					partitionSpec();
					setState(1033);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==IF) {
						{
						setState(1030);
						match(IF);
						setState(1031);
						match(NOT);
						setState(1032);
						match(EXISTS);
						}
					}

					}
				}

				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1037);
				match(INSERT);
				setState(1038);
				match(INTO);
				setState(1040);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,116,_ctx) ) {
				case 1:
					{
					setState(1039);
					match(TABLE);
					}
					break;
				}
				setState(1042);
				tableIdentifier();
				setState(1044);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(1043);
					partitionSpec();
					}
				}

				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PartitionSpecLocationContext extends ParserRuleContext {
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public LocationSpecContext locationSpec() {
			return getRuleContext(LocationSpecContext.class,0);
		}
		public PartitionSpecLocationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_partitionSpecLocation; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPartitionSpecLocation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPartitionSpecLocation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPartitionSpecLocation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PartitionSpecLocationContext partitionSpecLocation() throws RecognitionException {
		PartitionSpecLocationContext _localctx = new PartitionSpecLocationContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_partitionSpecLocation);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1048);
			partitionSpec();
			setState(1050);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==LOCATION) {
				{
				setState(1049);
				locationSpec();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PartitionSpecContext extends ParserRuleContext {
		public TerminalNode PARTITION() { return getToken(SqlBaseParser.PARTITION, 0); }
		public List<PartitionValContext> partitionVal() {
			return getRuleContexts(PartitionValContext.class);
		}
		public PartitionValContext partitionVal(int i) {
			return getRuleContext(PartitionValContext.class,i);
		}
		public PartitionSpecContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_partitionSpec; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPartitionSpec(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPartitionSpec(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPartitionSpec(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PartitionSpecContext partitionSpec() throws RecognitionException {
		PartitionSpecContext _localctx = new PartitionSpecContext(_ctx, getState());
		enterRule(_localctx, 30, RULE_partitionSpec);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1052);
			match(PARTITION);
			setState(1053);
			match(T__0);
			setState(1054);
			partitionVal();
			setState(1059);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(1055);
				match(T__2);
				setState(1056);
				partitionVal();
				}
				}
				setState(1061);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1062);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PartitionValContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode EQ() { return getToken(SqlBaseParser.EQ, 0); }
		public ConstantContext constant() {
			return getRuleContext(ConstantContext.class,0);
		}
		public PartitionValContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_partitionVal; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPartitionVal(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPartitionVal(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPartitionVal(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PartitionValContext partitionVal() throws RecognitionException {
		PartitionValContext _localctx = new PartitionValContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_partitionVal);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1064);
			identifier();
			setState(1067);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==EQ) {
				{
				setState(1065);
				match(EQ);
				setState(1066);
				constant();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DescribeFuncNameContext extends ParserRuleContext {
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public ComparisonOperatorContext comparisonOperator() {
			return getRuleContext(ComparisonOperatorContext.class,0);
		}
		public ArithmeticOperatorContext arithmeticOperator() {
			return getRuleContext(ArithmeticOperatorContext.class,0);
		}
		public PredicateOperatorContext predicateOperator() {
			return getRuleContext(PredicateOperatorContext.class,0);
		}
		public DescribeFuncNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_describeFuncName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDescribeFuncName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDescribeFuncName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDescribeFuncName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DescribeFuncNameContext describeFuncName() throws RecognitionException {
		DescribeFuncNameContext _localctx = new DescribeFuncNameContext(_ctx, getState());
		enterRule(_localctx, 34, RULE_describeFuncName);
		try {
			setState(1074);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,122,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1069);
				qualifiedName();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1070);
				match(STRING);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(1071);
				comparisonOperator();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(1072);
				arithmeticOperator();
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(1073);
				predicateOperator();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DescribeColNameContext extends ParserRuleContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public List<TerminalNode> STRING() { return getTokens(SqlBaseParser.STRING); }
		public TerminalNode STRING(int i) {
			return getToken(SqlBaseParser.STRING, i);
		}
		public DescribeColNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_describeColName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDescribeColName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDescribeColName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDescribeColName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DescribeColNameContext describeColName() throws RecognitionException {
		DescribeColNameContext _localctx = new DescribeColNameContext(_ctx, getState());
		enterRule(_localctx, 36, RULE_describeColName);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1076);
			identifier();
			setState(1084);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(1077);
				match(T__3);
				setState(1080);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case SELECT:
				case FROM:
				case ADD:
				case AS:
				case ALL:
				case DISTINCT:
				case WHERE:
				case GROUP:
				case BY:
				case GROUPING:
				case SETS:
				case CUBE:
				case ROLLUP:
				case ORDER:
				case HAVING:
				case LIMIT:
				case AT:
				case OR:
				case AND:
				case IN:
				case NOT:
				case NO:
				case EXISTS:
				case BETWEEN:
				case LIKE:
				case RLIKE:
				case IS:
				case NULL:
				case TRUE:
				case FALSE:
				case NULLS:
				case ASC:
				case DESC:
				case FOR:
				case INTERVAL:
				case CASE:
				case WHEN:
				case THEN:
				case ELSE:
				case END:
				case JOIN:
				case CROSS:
				case OUTER:
				case INNER:
				case LEFT:
				case SEMI:
				case RIGHT:
				case FULL:
				case NATURAL:
				case ON:
				case LATERAL:
				case WINDOW:
				case OVER:
				case PARTITION:
				case RANGE:
				case ROWS:
				case UNBOUNDED:
				case PRECEDING:
				case FOLLOWING:
				case CURRENT:
				case ROW:
				case WITH:
				case VALUES:
				case CREATE:
				case TABLE:
				case VIEW:
				case REPLACE:
				case INSERT:
				case DELETE:
				case INTO:
				case DESCRIBE:
				case EXPLAIN:
				case FORMAT:
				case LOGICAL:
				case CODEGEN:
				case CAST:
				case SHOW:
				case TABLES:
				case COLUMNS:
				case COLUMN:
				case USE:
				case PARTITIONS:
				case FUNCTIONS:
				case DROP:
				case UNION:
				case EXCEPT:
				case INTERSECT:
				case TO:
				case TABLESAMPLE:
				case STRATIFY:
				case ALTER:
				case RENAME:
				case ARRAY:
				case MAP:
				case STRUCT:
				case COMMENT:
				case SET:
				case RESET:
				case DATA:
				case START:
				case TRANSACTION:
				case COMMIT:
				case ROLLBACK:
				case MACRO:
				case IF:
				case DIV:
				case PERCENTLIT:
				case BUCKET:
				case OUT:
				case OF:
				case SORT:
				case CLUSTER:
				case DISTRIBUTE:
				case OVERWRITE:
				case TRANSFORM:
				case REDUCE:
				case USING:
				case SERDE:
				case SERDEPROPERTIES:
				case RECORDREADER:
				case RECORDWRITER:
				case DELIMITED:
				case FIELDS:
				case TERMINATED:
				case COLLECTION:
				case ITEMS:
				case KEYS:
				case ESCAPED:
				case LINES:
				case SEPARATED:
				case FUNCTION:
				case EXTENDED:
				case REFRESH:
				case CLEAR:
				case CACHE:
				case UNCACHE:
				case LAZY:
				case FORMATTED:
				case TEMPORARY:
				case OPTIONS:
				case UNSET:
				case TBLPROPERTIES:
				case DBPROPERTIES:
				case BUCKETS:
				case SKEWED:
				case STORED:
				case DIRECTORIES:
				case LOCATION:
				case EXCHANGE:
				case ARCHIVE:
				case UNARCHIVE:
				case FILEFORMAT:
				case TOUCH:
				case COMPACT:
				case CONCATENATE:
				case CHANGE:
				case CASCADE:
				case RESTRICT:
				case CLUSTERED:
				case SORTED:
				case PURGE:
				case INPUTFORMAT:
				case OUTPUTFORMAT:
				case DATABASE:
				case DATABASES:
				case DFS:
				case TRUNCATE:
				case ANALYZE:
				case COMPUTE:
				case LIST:
				case STATISTICS:
				case PARTITIONED:
				case EXTERNAL:
				case DEFINED:
				case REVOKE:
				case GRANT:
				case LOCK:
				case UNLOCK:
				case MSCK:
				case REPAIR:
				case EXPORT:
				case IMPORT:
				case LOAD:
				case ROLE:
				case ROLES:
				case COMPACTIONS:
				case PRINCIPALS:
				case TRANSACTIONS:
				case INDEX:
				case INDEXES:
				case LOCKS:
				case OPTION:
				case ANTI:
				case LOCAL:
				case INPATH:
				case IDENTIFIER:
				case BACKQUOTED_IDENTIFIER:
					{
					setState(1078);
					identifier();
					}
					break;
				case STRING:
					{
					setState(1079);
					match(STRING);
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				}
				setState(1086);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CtesContext extends ParserRuleContext {
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public List<NamedQueryContext> namedQuery() {
			return getRuleContexts(NamedQueryContext.class);
		}
		public NamedQueryContext namedQuery(int i) {
			return getRuleContext(NamedQueryContext.class,i);
		}
		public CtesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_ctes; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCtes(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCtes(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCtes(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CtesContext ctes() throws RecognitionException {
		CtesContext _localctx = new CtesContext(_ctx, getState());
		enterRule(_localctx, 38, RULE_ctes);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1087);
			match(WITH);
			setState(1088);
			namedQuery();
			setState(1093);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(1089);
				match(T__2);
				setState(1090);
				namedQuery();
				}
				}
				setState(1095);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NamedQueryContext extends ParserRuleContext {
		public IdentifierContext name;
		public QueryNoWithContext queryNoWith() {
			return getRuleContext(QueryNoWithContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public NamedQueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_namedQuery; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNamedQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNamedQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNamedQuery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NamedQueryContext namedQuery() throws RecognitionException {
		NamedQueryContext _localctx = new NamedQueryContext(_ctx, getState());
		enterRule(_localctx, 40, RULE_namedQuery);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1096);
			((NamedQueryContext)_localctx).name = identifier();
			setState(1098);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==AS) {
				{
				setState(1097);
				match(AS);
				}
			}

			setState(1100);
			match(T__0);
			setState(1101);
			queryNoWith();
			setState(1102);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TableProviderContext extends ParserRuleContext {
		public TerminalNode USING() { return getToken(SqlBaseParser.USING, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TableProviderContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tableProvider; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableProvider(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableProvider(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTableProvider(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TableProviderContext tableProvider() throws RecognitionException {
		TableProviderContext _localctx = new TableProviderContext(_ctx, getState());
		enterRule(_localctx, 42, RULE_tableProvider);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1104);
			match(USING);
			setState(1105);
			qualifiedName();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TablePropertyListContext extends ParserRuleContext {
		public List<TablePropertyContext> tableProperty() {
			return getRuleContexts(TablePropertyContext.class);
		}
		public TablePropertyContext tableProperty(int i) {
			return getRuleContext(TablePropertyContext.class,i);
		}
		public TablePropertyListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tablePropertyList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTablePropertyList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTablePropertyList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTablePropertyList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TablePropertyListContext tablePropertyList() throws RecognitionException {
		TablePropertyListContext _localctx = new TablePropertyListContext(_ctx, getState());
		enterRule(_localctx, 44, RULE_tablePropertyList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1107);
			match(T__0);
			setState(1108);
			tableProperty();
			setState(1113);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(1109);
				match(T__2);
				setState(1110);
				tableProperty();
				}
				}
				setState(1115);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1116);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TablePropertyContext extends ParserRuleContext {
		public TablePropertyKeyContext key;
		public Token value;
		public TablePropertyKeyContext tablePropertyKey() {
			return getRuleContext(TablePropertyKeyContext.class,0);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode EQ() { return getToken(SqlBaseParser.EQ, 0); }
		public TablePropertyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tableProperty; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableProperty(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableProperty(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTableProperty(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TablePropertyContext tableProperty() throws RecognitionException {
		TablePropertyContext _localctx = new TablePropertyContext(_ctx, getState());
		enterRule(_localctx, 46, RULE_tableProperty);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1118);
			((TablePropertyContext)_localctx).key = tablePropertyKey();
			setState(1123);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==EQ || _la==STRING) {
				{
				setState(1120);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==EQ) {
					{
					setState(1119);
					match(EQ);
					}
				}

				setState(1122);
				((TablePropertyContext)_localctx).value = match(STRING);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TablePropertyKeyContext extends ParserRuleContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TablePropertyKeyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tablePropertyKey; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTablePropertyKey(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTablePropertyKey(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTablePropertyKey(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TablePropertyKeyContext tablePropertyKey() throws RecognitionException {
		TablePropertyKeyContext _localctx = new TablePropertyKeyContext(_ctx, getState());
		enterRule(_localctx, 48, RULE_tablePropertyKey);
		int _la;
		try {
			setState(1134);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case SELECT:
			case FROM:
			case ADD:
			case AS:
			case ALL:
			case DISTINCT:
			case WHERE:
			case GROUP:
			case BY:
			case GROUPING:
			case SETS:
			case CUBE:
			case ROLLUP:
			case ORDER:
			case HAVING:
			case LIMIT:
			case AT:
			case OR:
			case AND:
			case IN:
			case NOT:
			case NO:
			case EXISTS:
			case BETWEEN:
			case LIKE:
			case RLIKE:
			case IS:
			case NULL:
			case TRUE:
			case FALSE:
			case NULLS:
			case ASC:
			case DESC:
			case FOR:
			case INTERVAL:
			case CASE:
			case WHEN:
			case THEN:
			case ELSE:
			case END:
			case JOIN:
			case CROSS:
			case OUTER:
			case INNER:
			case LEFT:
			case SEMI:
			case RIGHT:
			case FULL:
			case NATURAL:
			case ON:
			case LATERAL:
			case WINDOW:
			case OVER:
			case PARTITION:
			case RANGE:
			case ROWS:
			case UNBOUNDED:
			case PRECEDING:
			case FOLLOWING:
			case CURRENT:
			case ROW:
			case WITH:
			case VALUES:
			case CREATE:
			case TABLE:
			case VIEW:
			case REPLACE:
			case INSERT:
			case DELETE:
			case INTO:
			case DESCRIBE:
			case EXPLAIN:
			case FORMAT:
			case LOGICAL:
			case CODEGEN:
			case CAST:
			case SHOW:
			case TABLES:
			case COLUMNS:
			case COLUMN:
			case USE:
			case PARTITIONS:
			case FUNCTIONS:
			case DROP:
			case UNION:
			case EXCEPT:
			case INTERSECT:
			case TO:
			case TABLESAMPLE:
			case STRATIFY:
			case ALTER:
			case RENAME:
			case ARRAY:
			case MAP:
			case STRUCT:
			case COMMENT:
			case SET:
			case RESET:
			case DATA:
			case START:
			case TRANSACTION:
			case COMMIT:
			case ROLLBACK:
			case MACRO:
			case IF:
			case DIV:
			case PERCENTLIT:
			case BUCKET:
			case OUT:
			case OF:
			case SORT:
			case CLUSTER:
			case DISTRIBUTE:
			case OVERWRITE:
			case TRANSFORM:
			case REDUCE:
			case USING:
			case SERDE:
			case SERDEPROPERTIES:
			case RECORDREADER:
			case RECORDWRITER:
			case DELIMITED:
			case FIELDS:
			case TERMINATED:
			case COLLECTION:
			case ITEMS:
			case KEYS:
			case ESCAPED:
			case LINES:
			case SEPARATED:
			case FUNCTION:
			case EXTENDED:
			case REFRESH:
			case CLEAR:
			case CACHE:
			case UNCACHE:
			case LAZY:
			case FORMATTED:
			case TEMPORARY:
			case OPTIONS:
			case UNSET:
			case TBLPROPERTIES:
			case DBPROPERTIES:
			case BUCKETS:
			case SKEWED:
			case STORED:
			case DIRECTORIES:
			case LOCATION:
			case EXCHANGE:
			case ARCHIVE:
			case UNARCHIVE:
			case FILEFORMAT:
			case TOUCH:
			case COMPACT:
			case CONCATENATE:
			case CHANGE:
			case CASCADE:
			case RESTRICT:
			case CLUSTERED:
			case SORTED:
			case PURGE:
			case INPUTFORMAT:
			case OUTPUTFORMAT:
			case DATABASE:
			case DATABASES:
			case DFS:
			case TRUNCATE:
			case ANALYZE:
			case COMPUTE:
			case LIST:
			case STATISTICS:
			case PARTITIONED:
			case EXTERNAL:
			case DEFINED:
			case REVOKE:
			case GRANT:
			case LOCK:
			case UNLOCK:
			case MSCK:
			case REPAIR:
			case EXPORT:
			case IMPORT:
			case LOAD:
			case ROLE:
			case ROLES:
			case COMPACTIONS:
			case PRINCIPALS:
			case TRANSACTIONS:
			case INDEX:
			case INDEXES:
			case LOCKS:
			case OPTION:
			case ANTI:
			case LOCAL:
			case INPATH:
			case IDENTIFIER:
			case BACKQUOTED_IDENTIFIER:
				enterOuterAlt(_localctx, 1);
				{
				setState(1125);
				identifier();
				setState(1130);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(1126);
					match(T__3);
					setState(1127);
					identifier();
					}
					}
					setState(1132);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case STRING:
				enterOuterAlt(_localctx, 2);
				{
				setState(1133);
				match(STRING);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ConstantListContext extends ParserRuleContext {
		public List<ConstantContext> constant() {
			return getRuleContexts(ConstantContext.class);
		}
		public ConstantContext constant(int i) {
			return getRuleContext(ConstantContext.class,i);
		}
		public ConstantListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_constantList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterConstantList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitConstantList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitConstantList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ConstantListContext constantList() throws RecognitionException {
		ConstantListContext _localctx = new ConstantListContext(_ctx, getState());
		enterRule(_localctx, 50, RULE_constantList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1136);
			match(T__0);
			setState(1137);
			constant();
			setState(1142);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(1138);
				match(T__2);
				setState(1139);
				constant();
				}
				}
				setState(1144);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1145);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NestedConstantListContext extends ParserRuleContext {
		public List<ConstantListContext> constantList() {
			return getRuleContexts(ConstantListContext.class);
		}
		public ConstantListContext constantList(int i) {
			return getRuleContext(ConstantListContext.class,i);
		}
		public NestedConstantListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nestedConstantList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNestedConstantList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNestedConstantList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNestedConstantList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NestedConstantListContext nestedConstantList() throws RecognitionException {
		NestedConstantListContext _localctx = new NestedConstantListContext(_ctx, getState());
		enterRule(_localctx, 52, RULE_nestedConstantList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1147);
			match(T__0);
			setState(1148);
			constantList();
			setState(1153);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(1149);
				match(T__2);
				setState(1150);
				constantList();
				}
				}
				setState(1155);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1156);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CreateFileFormatContext extends ParserRuleContext {
		public TerminalNode STORED() { return getToken(SqlBaseParser.STORED, 0); }
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public FileFormatContext fileFormat() {
			return getRuleContext(FileFormatContext.class,0);
		}
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public StorageHandlerContext storageHandler() {
			return getRuleContext(StorageHandlerContext.class,0);
		}
		public CreateFileFormatContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createFileFormat; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateFileFormat(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateFileFormat(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateFileFormat(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CreateFileFormatContext createFileFormat() throws RecognitionException {
		CreateFileFormatContext _localctx = new CreateFileFormatContext(_ctx, getState());
		enterRule(_localctx, 54, RULE_createFileFormat);
		try {
			setState(1164);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,134,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1158);
				match(STORED);
				setState(1159);
				match(AS);
				setState(1160);
				fileFormat();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1161);
				match(STORED);
				setState(1162);
				match(BY);
				setState(1163);
				storageHandler();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FileFormatContext extends ParserRuleContext {
		public FileFormatContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_fileFormat; }
	 
		public FileFormatContext() { }
		public void copyFrom(FileFormatContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class TableFileFormatContext extends FileFormatContext {
		public Token inFmt;
		public Token outFmt;
		public TerminalNode INPUTFORMAT() { return getToken(SqlBaseParser.INPUTFORMAT, 0); }
		public TerminalNode OUTPUTFORMAT() { return getToken(SqlBaseParser.OUTPUTFORMAT, 0); }
		public List<TerminalNode> STRING() { return getTokens(SqlBaseParser.STRING); }
		public TerminalNode STRING(int i) {
			return getToken(SqlBaseParser.STRING, i);
		}
		public TableFileFormatContext(FileFormatContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableFileFormat(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableFileFormat(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTableFileFormat(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class GenericFileFormatContext extends FileFormatContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public GenericFileFormatContext(FileFormatContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterGenericFileFormat(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitGenericFileFormat(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitGenericFileFormat(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FileFormatContext fileFormat() throws RecognitionException {
		FileFormatContext _localctx = new FileFormatContext(_ctx, getState());
		enterRule(_localctx, 56, RULE_fileFormat);
		try {
			setState(1171);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,135,_ctx) ) {
			case 1:
				_localctx = new TableFileFormatContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1166);
				match(INPUTFORMAT);
				setState(1167);
				((TableFileFormatContext)_localctx).inFmt = match(STRING);
				setState(1168);
				match(OUTPUTFORMAT);
				setState(1169);
				((TableFileFormatContext)_localctx).outFmt = match(STRING);
				}
				break;
			case 2:
				_localctx = new GenericFileFormatContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1170);
				identifier();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class StorageHandlerContext extends ParserRuleContext {
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode SERDEPROPERTIES() { return getToken(SqlBaseParser.SERDEPROPERTIES, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public StorageHandlerContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_storageHandler; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterStorageHandler(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitStorageHandler(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitStorageHandler(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StorageHandlerContext storageHandler() throws RecognitionException {
		StorageHandlerContext _localctx = new StorageHandlerContext(_ctx, getState());
		enterRule(_localctx, 58, RULE_storageHandler);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1173);
			match(STRING);
			setState(1177);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,136,_ctx) ) {
			case 1:
				{
				setState(1174);
				match(WITH);
				setState(1175);
				match(SERDEPROPERTIES);
				setState(1176);
				tablePropertyList();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ResourceContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public ResourceContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_resource; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterResource(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitResource(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitResource(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ResourceContext resource() throws RecognitionException {
		ResourceContext _localctx = new ResourceContext(_ctx, getState());
		enterRule(_localctx, 60, RULE_resource);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1179);
			identifier();
			setState(1180);
			match(STRING);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QueryNoWithContext extends ParserRuleContext {
		public QueryNoWithContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryNoWith; }
	 
		public QueryNoWithContext() { }
		public void copyFrom(QueryNoWithContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class SingleInsertQueryContext extends QueryNoWithContext {
		public QueryTermContext queryTerm() {
			return getRuleContext(QueryTermContext.class,0);
		}
		public QueryOrganizationContext queryOrganization() {
			return getRuleContext(QueryOrganizationContext.class,0);
		}
		public InsertIntoContext insertInto() {
			return getRuleContext(InsertIntoContext.class,0);
		}
		public SingleInsertQueryContext(QueryNoWithContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSingleInsertQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSingleInsertQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSingleInsertQuery(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class MultiInsertQueryContext extends QueryNoWithContext {
		public FromClauseContext fromClause() {
			return getRuleContext(FromClauseContext.class,0);
		}
		public List<MultiInsertQueryBodyContext> multiInsertQueryBody() {
			return getRuleContexts(MultiInsertQueryBodyContext.class);
		}
		public MultiInsertQueryBodyContext multiInsertQueryBody(int i) {
			return getRuleContext(MultiInsertQueryBodyContext.class,i);
		}
		public MultiInsertQueryContext(QueryNoWithContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterMultiInsertQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitMultiInsertQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitMultiInsertQuery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryNoWithContext queryNoWith() throws RecognitionException {
		QueryNoWithContext _localctx = new QueryNoWithContext(_ctx, getState());
		enterRule(_localctx, 62, RULE_queryNoWith);
		int _la;
		try {
			setState(1194);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,139,_ctx) ) {
			case 1:
				_localctx = new SingleInsertQueryContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1183);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==INSERT) {
					{
					setState(1182);
					insertInto();
					}
				}

				setState(1185);
				queryTerm(0);
				setState(1186);
				queryOrganization();
				}
				break;
			case 2:
				_localctx = new MultiInsertQueryContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1188);
				fromClause();
				setState(1190); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(1189);
					multiInsertQueryBody();
					}
					}
					setState(1192); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==SELECT || _la==FROM || _la==INSERT || _la==MAP || _la==REDUCE );
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QueryOrganizationContext extends ParserRuleContext {
		public SortItemContext sortItem;
		public List<SortItemContext> order = new ArrayList<SortItemContext>();
		public ExpressionContext expression;
		public List<ExpressionContext> clusterBy = new ArrayList<ExpressionContext>();
		public List<ExpressionContext> distributeBy = new ArrayList<ExpressionContext>();
		public List<SortItemContext> sort = new ArrayList<SortItemContext>();
		public ExpressionContext limit;
		public TerminalNode ORDER() { return getToken(SqlBaseParser.ORDER, 0); }
		public List<TerminalNode> BY() { return getTokens(SqlBaseParser.BY); }
		public TerminalNode BY(int i) {
			return getToken(SqlBaseParser.BY, i);
		}
		public TerminalNode CLUSTER() { return getToken(SqlBaseParser.CLUSTER, 0); }
		public TerminalNode DISTRIBUTE() { return getToken(SqlBaseParser.DISTRIBUTE, 0); }
		public TerminalNode SORT() { return getToken(SqlBaseParser.SORT, 0); }
		public WindowsContext windows() {
			return getRuleContext(WindowsContext.class,0);
		}
		public TerminalNode LIMIT() { return getToken(SqlBaseParser.LIMIT, 0); }
		public List<SortItemContext> sortItem() {
			return getRuleContexts(SortItemContext.class);
		}
		public SortItemContext sortItem(int i) {
			return getRuleContext(SortItemContext.class,i);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public QueryOrganizationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryOrganization; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQueryOrganization(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQueryOrganization(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQueryOrganization(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryOrganizationContext queryOrganization() throws RecognitionException {
		QueryOrganizationContext _localctx = new QueryOrganizationContext(_ctx, getState());
		enterRule(_localctx, 64, RULE_queryOrganization);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1206);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ORDER) {
				{
				setState(1196);
				match(ORDER);
				setState(1197);
				match(BY);
				setState(1198);
				((QueryOrganizationContext)_localctx).sortItem = sortItem();
				((QueryOrganizationContext)_localctx).order.add(((QueryOrganizationContext)_localctx).sortItem);
				setState(1203);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(1199);
					match(T__2);
					setState(1200);
					((QueryOrganizationContext)_localctx).sortItem = sortItem();
					((QueryOrganizationContext)_localctx).order.add(((QueryOrganizationContext)_localctx).sortItem);
					}
					}
					setState(1205);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(1218);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==CLUSTER) {
				{
				setState(1208);
				match(CLUSTER);
				setState(1209);
				match(BY);
				setState(1210);
				((QueryOrganizationContext)_localctx).expression = expression();
				((QueryOrganizationContext)_localctx).clusterBy.add(((QueryOrganizationContext)_localctx).expression);
				setState(1215);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(1211);
					match(T__2);
					setState(1212);
					((QueryOrganizationContext)_localctx).expression = expression();
					((QueryOrganizationContext)_localctx).clusterBy.add(((QueryOrganizationContext)_localctx).expression);
					}
					}
					setState(1217);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(1230);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==DISTRIBUTE) {
				{
				setState(1220);
				match(DISTRIBUTE);
				setState(1221);
				match(BY);
				setState(1222);
				((QueryOrganizationContext)_localctx).expression = expression();
				((QueryOrganizationContext)_localctx).distributeBy.add(((QueryOrganizationContext)_localctx).expression);
				setState(1227);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(1223);
					match(T__2);
					setState(1224);
					((QueryOrganizationContext)_localctx).expression = expression();
					((QueryOrganizationContext)_localctx).distributeBy.add(((QueryOrganizationContext)_localctx).expression);
					}
					}
					setState(1229);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(1242);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SORT) {
				{
				setState(1232);
				match(SORT);
				setState(1233);
				match(BY);
				setState(1234);
				((QueryOrganizationContext)_localctx).sortItem = sortItem();
				((QueryOrganizationContext)_localctx).sort.add(((QueryOrganizationContext)_localctx).sortItem);
				setState(1239);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(1235);
					match(T__2);
					setState(1236);
					((QueryOrganizationContext)_localctx).sortItem = sortItem();
					((QueryOrganizationContext)_localctx).sort.add(((QueryOrganizationContext)_localctx).sortItem);
					}
					}
					setState(1241);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(1245);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WINDOW) {
				{
				setState(1244);
				windows();
				}
			}

			setState(1249);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==LIMIT) {
				{
				setState(1247);
				match(LIMIT);
				setState(1248);
				((QueryOrganizationContext)_localctx).limit = expression();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class MultiInsertQueryBodyContext extends ParserRuleContext {
		public QuerySpecificationContext querySpecification() {
			return getRuleContext(QuerySpecificationContext.class,0);
		}
		public QueryOrganizationContext queryOrganization() {
			return getRuleContext(QueryOrganizationContext.class,0);
		}
		public InsertIntoContext insertInto() {
			return getRuleContext(InsertIntoContext.class,0);
		}
		public MultiInsertQueryBodyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_multiInsertQueryBody; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterMultiInsertQueryBody(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitMultiInsertQueryBody(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitMultiInsertQueryBody(this);
			else return visitor.visitChildren(this);
		}
	}

	public final MultiInsertQueryBodyContext multiInsertQueryBody() throws RecognitionException {
		MultiInsertQueryBodyContext _localctx = new MultiInsertQueryBodyContext(_ctx, getState());
		enterRule(_localctx, 66, RULE_multiInsertQueryBody);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1252);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==INSERT) {
				{
				setState(1251);
				insertInto();
				}
			}

			setState(1254);
			querySpecification();
			setState(1255);
			queryOrganization();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QueryTermContext extends ParserRuleContext {
		public QueryTermContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryTerm; }
	 
		public QueryTermContext() { }
		public void copyFrom(QueryTermContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class QueryTermDefaultContext extends QueryTermContext {
		public QueryPrimaryContext queryPrimary() {
			return getRuleContext(QueryPrimaryContext.class,0);
		}
		public QueryTermDefaultContext(QueryTermContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQueryTermDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQueryTermDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQueryTermDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SetOperationContext extends QueryTermContext {
		public QueryTermContext left;
		public Token operator;
		public QueryTermContext right;
		public List<QueryTermContext> queryTerm() {
			return getRuleContexts(QueryTermContext.class);
		}
		public QueryTermContext queryTerm(int i) {
			return getRuleContext(QueryTermContext.class,i);
		}
		public TerminalNode INTERSECT() { return getToken(SqlBaseParser.INTERSECT, 0); }
		public TerminalNode UNION() { return getToken(SqlBaseParser.UNION, 0); }
		public TerminalNode EXCEPT() { return getToken(SqlBaseParser.EXCEPT, 0); }
		public SetQuantifierContext setQuantifier() {
			return getRuleContext(SetQuantifierContext.class,0);
		}
		public SetOperationContext(QueryTermContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetOperation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetOperation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetOperation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryTermContext queryTerm() throws RecognitionException {
		return queryTerm(0);
	}

	private QueryTermContext queryTerm(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		QueryTermContext _localctx = new QueryTermContext(_ctx, _parentState);
		QueryTermContext _prevctx = _localctx;
		int _startState = 68;
		enterRecursionRule(_localctx, 68, RULE_queryTerm, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			{
			_localctx = new QueryTermDefaultContext(_localctx);
			_ctx = _localctx;
			_prevctx = _localctx;

			setState(1258);
			queryPrimary();
			}
			_ctx.stop = _input.LT(-1);
			setState(1268);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,152,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					{
					_localctx = new SetOperationContext(new QueryTermContext(_parentctx, _parentState));
					((SetOperationContext)_localctx).left = _prevctx;
					pushNewRecursionContext(_localctx, _startState, RULE_queryTerm);
					setState(1260);
					if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
					setState(1261);
					((SetOperationContext)_localctx).operator = _input.LT(1);
					_la = _input.LA(1);
					if ( !(((((_la - 92)) & ~0x3f) == 0 && ((1L << (_la - 92)) & ((1L << (UNION - 92)) | (1L << (EXCEPT - 92)) | (1L << (INTERSECT - 92)))) != 0)) ) {
						((SetOperationContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(1263);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==ALL || _la==DISTINCT) {
						{
						setState(1262);
						setQuantifier();
						}
					}

					setState(1265);
					((SetOperationContext)_localctx).right = queryTerm(2);
					}
					} 
				}
				setState(1270);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,152,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class QueryPrimaryContext extends ParserRuleContext {
		public QueryPrimaryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryPrimary; }
	 
		public QueryPrimaryContext() { }
		public void copyFrom(QueryPrimaryContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class SubqueryContext extends QueryPrimaryContext {
		public QueryNoWithContext queryNoWith() {
			return getRuleContext(QueryNoWithContext.class,0);
		}
		public SubqueryContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSubquery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSubquery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSubquery(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class QueryPrimaryDefaultContext extends QueryPrimaryContext {
		public QuerySpecificationContext querySpecification() {
			return getRuleContext(QuerySpecificationContext.class,0);
		}
		public QueryPrimaryDefaultContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQueryPrimaryDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQueryPrimaryDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQueryPrimaryDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class InlineTableDefault1Context extends QueryPrimaryContext {
		public InlineTableContext inlineTable() {
			return getRuleContext(InlineTableContext.class,0);
		}
		public InlineTableDefault1Context(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterInlineTableDefault1(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitInlineTableDefault1(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitInlineTableDefault1(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class TableContext extends QueryPrimaryContext {
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TableContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTable(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryPrimaryContext queryPrimary() throws RecognitionException {
		QueryPrimaryContext _localctx = new QueryPrimaryContext(_ctx, getState());
		enterRule(_localctx, 70, RULE_queryPrimary);
		try {
			setState(1279);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case SELECT:
			case FROM:
			case MAP:
			case REDUCE:
				_localctx = new QueryPrimaryDefaultContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1271);
				querySpecification();
				}
				break;
			case TABLE:
				_localctx = new TableContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1272);
				match(TABLE);
				setState(1273);
				tableIdentifier();
				}
				break;
			case VALUES:
				_localctx = new InlineTableDefault1Context(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1274);
				inlineTable();
				}
				break;
			case T__0:
				_localctx = new SubqueryContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(1275);
				match(T__0);
				setState(1276);
				queryNoWith();
				setState(1277);
				match(T__1);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SortItemContext extends ParserRuleContext {
		public Token ordering;
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode ASC() { return getToken(SqlBaseParser.ASC, 0); }
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public SortItemContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sortItem; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSortItem(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSortItem(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSortItem(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SortItemContext sortItem() throws RecognitionException {
		SortItemContext _localctx = new SortItemContext(_ctx, getState());
		enterRule(_localctx, 72, RULE_sortItem);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1281);
			expression();
			setState(1283);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ASC || _la==DESC) {
				{
				setState(1282);
				((SortItemContext)_localctx).ordering = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==ASC || _la==DESC) ) {
					((SortItemContext)_localctx).ordering = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QuerySpecificationContext extends ParserRuleContext {
		public Token kind;
		public RowFormatContext inRowFormat;
		public Token recordWriter;
		public Token script;
		public RowFormatContext outRowFormat;
		public Token recordReader;
		public BooleanExpressionContext where;
		public BooleanExpressionContext having;
		public TerminalNode USING() { return getToken(SqlBaseParser.USING, 0); }
		public List<TerminalNode> STRING() { return getTokens(SqlBaseParser.STRING); }
		public TerminalNode STRING(int i) {
			return getToken(SqlBaseParser.STRING, i);
		}
		public TerminalNode RECORDWRITER() { return getToken(SqlBaseParser.RECORDWRITER, 0); }
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public TerminalNode RECORDREADER() { return getToken(SqlBaseParser.RECORDREADER, 0); }
		public FromClauseContext fromClause() {
			return getRuleContext(FromClauseContext.class,0);
		}
		public TerminalNode WHERE() { return getToken(SqlBaseParser.WHERE, 0); }
		public TerminalNode SELECT() { return getToken(SqlBaseParser.SELECT, 0); }
		public NamedExpressionSeqContext namedExpressionSeq() {
			return getRuleContext(NamedExpressionSeqContext.class,0);
		}
		public List<RowFormatContext> rowFormat() {
			return getRuleContexts(RowFormatContext.class);
		}
		public RowFormatContext rowFormat(int i) {
			return getRuleContext(RowFormatContext.class,i);
		}
		public List<BooleanExpressionContext> booleanExpression() {
			return getRuleContexts(BooleanExpressionContext.class);
		}
		public BooleanExpressionContext booleanExpression(int i) {
			return getRuleContext(BooleanExpressionContext.class,i);
		}
		public TerminalNode TRANSFORM() { return getToken(SqlBaseParser.TRANSFORM, 0); }
		public TerminalNode MAP() { return getToken(SqlBaseParser.MAP, 0); }
		public TerminalNode REDUCE() { return getToken(SqlBaseParser.REDUCE, 0); }
		public IdentifierSeqContext identifierSeq() {
			return getRuleContext(IdentifierSeqContext.class,0);
		}
		public ColTypeListContext colTypeList() {
			return getRuleContext(ColTypeListContext.class,0);
		}
		public List<LateralViewContext> lateralView() {
			return getRuleContexts(LateralViewContext.class);
		}
		public LateralViewContext lateralView(int i) {
			return getRuleContext(LateralViewContext.class,i);
		}
		public AggregationContext aggregation() {
			return getRuleContext(AggregationContext.class,0);
		}
		public TerminalNode HAVING() { return getToken(SqlBaseParser.HAVING, 0); }
		public WindowsContext windows() {
			return getRuleContext(WindowsContext.class,0);
		}
		public SetQuantifierContext setQuantifier() {
			return getRuleContext(SetQuantifierContext.class,0);
		}
		public QuerySpecificationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_querySpecification; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQuerySpecification(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQuerySpecification(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQuerySpecification(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QuerySpecificationContext querySpecification() throws RecognitionException {
		QuerySpecificationContext _localctx = new QuerySpecificationContext(_ctx, getState());
		enterRule(_localctx, 74, RULE_querySpecification);
		int _la;
		try {
			int _alt;
			setState(1372);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,175,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				{
				{
				setState(1295);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case SELECT:
					{
					setState(1285);
					match(SELECT);
					setState(1286);
					((QuerySpecificationContext)_localctx).kind = match(TRANSFORM);
					setState(1287);
					match(T__0);
					setState(1288);
					namedExpressionSeq();
					setState(1289);
					match(T__1);
					}
					break;
				case MAP:
					{
					setState(1291);
					((QuerySpecificationContext)_localctx).kind = match(MAP);
					setState(1292);
					namedExpressionSeq();
					}
					break;
				case REDUCE:
					{
					setState(1293);
					((QuerySpecificationContext)_localctx).kind = match(REDUCE);
					setState(1294);
					namedExpressionSeq();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				setState(1298);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ROW) {
					{
					setState(1297);
					((QuerySpecificationContext)_localctx).inRowFormat = rowFormat();
					}
				}

				setState(1302);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==RECORDWRITER) {
					{
					setState(1300);
					match(RECORDWRITER);
					setState(1301);
					((QuerySpecificationContext)_localctx).recordWriter = match(STRING);
					}
				}

				setState(1304);
				match(USING);
				setState(1305);
				((QuerySpecificationContext)_localctx).script = match(STRING);
				setState(1318);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,160,_ctx) ) {
				case 1:
					{
					setState(1306);
					match(AS);
					setState(1316);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,159,_ctx) ) {
					case 1:
						{
						setState(1307);
						identifierSeq();
						}
						break;
					case 2:
						{
						setState(1308);
						colTypeList();
						}
						break;
					case 3:
						{
						{
						setState(1309);
						match(T__0);
						setState(1312);
						_errHandler.sync(this);
						switch ( getInterpreter().adaptivePredict(_input,158,_ctx) ) {
						case 1:
							{
							setState(1310);
							identifierSeq();
							}
							break;
						case 2:
							{
							setState(1311);
							colTypeList();
							}
							break;
						}
						setState(1314);
						match(T__1);
						}
						}
						break;
					}
					}
					break;
				}
				setState(1321);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,161,_ctx) ) {
				case 1:
					{
					setState(1320);
					((QuerySpecificationContext)_localctx).outRowFormat = rowFormat();
					}
					break;
				}
				setState(1325);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,162,_ctx) ) {
				case 1:
					{
					setState(1323);
					match(RECORDREADER);
					setState(1324);
					((QuerySpecificationContext)_localctx).recordReader = match(STRING);
					}
					break;
				}
				setState(1328);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,163,_ctx) ) {
				case 1:
					{
					setState(1327);
					fromClause();
					}
					break;
				}
				setState(1332);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,164,_ctx) ) {
				case 1:
					{
					setState(1330);
					match(WHERE);
					setState(1331);
					((QuerySpecificationContext)_localctx).where = booleanExpression(0);
					}
					break;
				}
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(1350);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case SELECT:
					{
					setState(1334);
					((QuerySpecificationContext)_localctx).kind = match(SELECT);
					setState(1336);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,165,_ctx) ) {
					case 1:
						{
						setState(1335);
						setQuantifier();
						}
						break;
					}
					setState(1338);
					namedExpressionSeq();
					setState(1340);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,166,_ctx) ) {
					case 1:
						{
						setState(1339);
						fromClause();
						}
						break;
					}
					}
					break;
				case FROM:
					{
					setState(1342);
					fromClause();
					setState(1348);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,168,_ctx) ) {
					case 1:
						{
						setState(1343);
						((QuerySpecificationContext)_localctx).kind = match(SELECT);
						setState(1345);
						_errHandler.sync(this);
						switch ( getInterpreter().adaptivePredict(_input,167,_ctx) ) {
						case 1:
							{
							setState(1344);
							setQuantifier();
							}
							break;
						}
						setState(1347);
						namedExpressionSeq();
						}
						break;
					}
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(1355);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,170,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(1352);
						lateralView();
						}
						} 
					}
					setState(1357);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,170,_ctx);
				}
				setState(1360);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,171,_ctx) ) {
				case 1:
					{
					setState(1358);
					match(WHERE);
					setState(1359);
					((QuerySpecificationContext)_localctx).where = booleanExpression(0);
					}
					break;
				}
				setState(1363);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,172,_ctx) ) {
				case 1:
					{
					setState(1362);
					aggregation();
					}
					break;
				}
				setState(1367);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,173,_ctx) ) {
				case 1:
					{
					setState(1365);
					match(HAVING);
					setState(1366);
					((QuerySpecificationContext)_localctx).having = booleanExpression(0);
					}
					break;
				}
				setState(1370);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,174,_ctx) ) {
				case 1:
					{
					setState(1369);
					windows();
					}
					break;
				}
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FromClauseContext extends ParserRuleContext {
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public List<RelationContext> relation() {
			return getRuleContexts(RelationContext.class);
		}
		public RelationContext relation(int i) {
			return getRuleContext(RelationContext.class,i);
		}
		public List<LateralViewContext> lateralView() {
			return getRuleContexts(LateralViewContext.class);
		}
		public LateralViewContext lateralView(int i) {
			return getRuleContext(LateralViewContext.class,i);
		}
		public FromClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_fromClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterFromClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitFromClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitFromClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FromClauseContext fromClause() throws RecognitionException {
		FromClauseContext _localctx = new FromClauseContext(_ctx, getState());
		enterRule(_localctx, 76, RULE_fromClause);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1374);
			match(FROM);
			setState(1375);
			relation(0);
			setState(1380);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,176,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1376);
					match(T__2);
					setState(1377);
					relation(0);
					}
					} 
				}
				setState(1382);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,176,_ctx);
			}
			setState(1386);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,177,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1383);
					lateralView();
					}
					} 
				}
				setState(1388);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,177,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class AggregationContext extends ParserRuleContext {
		public ExpressionContext expression;
		public List<ExpressionContext> groupingExpressions = new ArrayList<ExpressionContext>();
		public Token kind;
		public TerminalNode GROUP() { return getToken(SqlBaseParser.GROUP, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode SETS() { return getToken(SqlBaseParser.SETS, 0); }
		public List<GroupingSetContext> groupingSet() {
			return getRuleContexts(GroupingSetContext.class);
		}
		public GroupingSetContext groupingSet(int i) {
			return getRuleContext(GroupingSetContext.class,i);
		}
		public TerminalNode ROLLUP() { return getToken(SqlBaseParser.ROLLUP, 0); }
		public TerminalNode CUBE() { return getToken(SqlBaseParser.CUBE, 0); }
		public TerminalNode GROUPING() { return getToken(SqlBaseParser.GROUPING, 0); }
		public AggregationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_aggregation; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAggregation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAggregation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitAggregation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AggregationContext aggregation() throws RecognitionException {
		AggregationContext _localctx = new AggregationContext(_ctx, getState());
		enterRule(_localctx, 78, RULE_aggregation);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1389);
			match(GROUP);
			setState(1390);
			match(BY);
			setState(1391);
			((AggregationContext)_localctx).expression = expression();
			((AggregationContext)_localctx).groupingExpressions.add(((AggregationContext)_localctx).expression);
			setState(1396);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,178,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1392);
					match(T__2);
					setState(1393);
					((AggregationContext)_localctx).expression = expression();
					((AggregationContext)_localctx).groupingExpressions.add(((AggregationContext)_localctx).expression);
					}
					} 
				}
				setState(1398);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,178,_ctx);
			}
			setState(1416);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,180,_ctx) ) {
			case 1:
				{
				setState(1399);
				match(WITH);
				setState(1400);
				((AggregationContext)_localctx).kind = match(ROLLUP);
				}
				break;
			case 2:
				{
				setState(1401);
				match(WITH);
				setState(1402);
				((AggregationContext)_localctx).kind = match(CUBE);
				}
				break;
			case 3:
				{
				setState(1403);
				((AggregationContext)_localctx).kind = match(GROUPING);
				setState(1404);
				match(SETS);
				setState(1405);
				match(T__0);
				setState(1406);
				groupingSet();
				setState(1411);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(1407);
					match(T__2);
					setState(1408);
					groupingSet();
					}
					}
					setState(1413);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1414);
				match(T__1);
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class GroupingSetContext extends ParserRuleContext {
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public GroupingSetContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groupingSet; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterGroupingSet(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitGroupingSet(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitGroupingSet(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GroupingSetContext groupingSet() throws RecognitionException {
		GroupingSetContext _localctx = new GroupingSetContext(_ctx, getState());
		enterRule(_localctx, 80, RULE_groupingSet);
		int _la;
		try {
			setState(1431);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,183,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1418);
				match(T__0);
				setState(1427);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << SELECT) | (1L << FROM) | (1L << ADD) | (1L << AS) | (1L << ALL) | (1L << DISTINCT) | (1L << WHERE) | (1L << GROUP) | (1L << BY) | (1L << GROUPING) | (1L << SETS) | (1L << CUBE) | (1L << ROLLUP) | (1L << ORDER) | (1L << HAVING) | (1L << LIMIT) | (1L << AT) | (1L << OR) | (1L << AND) | (1L << IN) | (1L << NOT) | (1L << NO) | (1L << EXISTS) | (1L << BETWEEN) | (1L << LIKE) | (1L << RLIKE) | (1L << IS) | (1L << NULL) | (1L << TRUE) | (1L << FALSE) | (1L << NULLS) | (1L << ASC) | (1L << DESC) | (1L << FOR) | (1L << INTERVAL) | (1L << CASE) | (1L << WHEN) | (1L << THEN) | (1L << ELSE) | (1L << END) | (1L << JOIN) | (1L << CROSS) | (1L << OUTER) | (1L << INNER) | (1L << LEFT) | (1L << SEMI) | (1L << RIGHT) | (1L << FULL) | (1L << NATURAL) | (1L << ON) | (1L << LATERAL) | (1L << WINDOW) | (1L << OVER) | (1L << PARTITION) | (1L << RANGE) | (1L << ROWS))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (UNBOUNDED - 64)) | (1L << (PRECEDING - 64)) | (1L << (FOLLOWING - 64)) | (1L << (CURRENT - 64)) | (1L << (ROW - 64)) | (1L << (WITH - 64)) | (1L << (VALUES - 64)) | (1L << (CREATE - 64)) | (1L << (TABLE - 64)) | (1L << (VIEW - 64)) | (1L << (REPLACE - 64)) | (1L << (INSERT - 64)) | (1L << (DELETE - 64)) | (1L << (INTO - 64)) | (1L << (DESCRIBE - 64)) | (1L << (EXPLAIN - 64)) | (1L << (FORMAT - 64)) | (1L << (LOGICAL - 64)) | (1L << (CODEGEN - 64)) | (1L << (CAST - 64)) | (1L << (SHOW - 64)) | (1L << (TABLES - 64)) | (1L << (COLUMNS - 64)) | (1L << (COLUMN - 64)) | (1L << (USE - 64)) | (1L << (PARTITIONS - 64)) | (1L << (FUNCTIONS - 64)) | (1L << (DROP - 64)) | (1L << (UNION - 64)) | (1L << (EXCEPT - 64)) | (1L << (INTERSECT - 64)) | (1L << (TO - 64)) | (1L << (TABLESAMPLE - 64)) | (1L << (STRATIFY - 64)) | (1L << (ALTER - 64)) | (1L << (RENAME - 64)) | (1L << (ARRAY - 64)) | (1L << (MAP - 64)) | (1L << (STRUCT - 64)) | (1L << (COMMENT - 64)) | (1L << (SET - 64)) | (1L << (RESET - 64)) | (1L << (DATA - 64)) | (1L << (START - 64)) | (1L << (TRANSACTION - 64)) | (1L << (COMMIT - 64)) | (1L << (ROLLBACK - 64)) | (1L << (MACRO - 64)) | (1L << (IF - 64)) | (1L << (PLUS - 64)) | (1L << (MINUS - 64)) | (1L << (ASTERISK - 64)) | (1L << (DIV - 64)) | (1L << (TILDE - 64)))) != 0) || ((((_la - 131)) & ~0x3f) == 0 && ((1L << (_la - 131)) & ((1L << (PERCENTLIT - 131)) | (1L << (BUCKET - 131)) | (1L << (OUT - 131)) | (1L << (OF - 131)) | (1L << (SORT - 131)) | (1L << (CLUSTER - 131)) | (1L << (DISTRIBUTE - 131)) | (1L << (OVERWRITE - 131)) | (1L << (TRANSFORM - 131)) | (1L << (REDUCE - 131)) | (1L << (USING - 131)) | (1L << (SERDE - 131)) | (1L << (SERDEPROPERTIES - 131)) | (1L << (RECORDREADER - 131)) | (1L << (RECORDWRITER - 131)) | (1L << (DELIMITED - 131)) | (1L << (FIELDS - 131)) | (1L << (TERMINATED - 131)) | (1L << (COLLECTION - 131)) | (1L << (ITEMS - 131)) | (1L << (KEYS - 131)) | (1L << (ESCAPED - 131)) | (1L << (LINES - 131)) | (1L << (SEPARATED - 131)) | (1L << (FUNCTION - 131)) | (1L << (EXTENDED - 131)) | (1L << (REFRESH - 131)) | (1L << (CLEAR - 131)) | (1L << (CACHE - 131)) | (1L << (UNCACHE - 131)) | (1L << (LAZY - 131)) | (1L << (FORMATTED - 131)) | (1L << (TEMPORARY - 131)) | (1L << (OPTIONS - 131)) | (1L << (UNSET - 131)) | (1L << (TBLPROPERTIES - 131)) | (1L << (DBPROPERTIES - 131)) | (1L << (BUCKETS - 131)) | (1L << (SKEWED - 131)) | (1L << (STORED - 131)) | (1L << (DIRECTORIES - 131)) | (1L << (LOCATION - 131)) | (1L << (EXCHANGE - 131)) | (1L << (ARCHIVE - 131)) | (1L << (UNARCHIVE - 131)) | (1L << (FILEFORMAT - 131)) | (1L << (TOUCH - 131)) | (1L << (COMPACT - 131)) | (1L << (CONCATENATE - 131)) | (1L << (CHANGE - 131)) | (1L << (CASCADE - 131)) | (1L << (RESTRICT - 131)) | (1L << (CLUSTERED - 131)) | (1L << (SORTED - 131)) | (1L << (PURGE - 131)) | (1L << (INPUTFORMAT - 131)) | (1L << (OUTPUTFORMAT - 131)) | (1L << (DATABASE - 131)) | (1L << (DATABASES - 131)) | (1L << (DFS - 131)) | (1L << (TRUNCATE - 131)) | (1L << (ANALYZE - 131)) | (1L << (COMPUTE - 131)) | (1L << (LIST - 131)))) != 0) || ((((_la - 195)) & ~0x3f) == 0 && ((1L << (_la - 195)) & ((1L << (STATISTICS - 195)) | (1L << (PARTITIONED - 195)) | (1L << (EXTERNAL - 195)) | (1L << (DEFINED - 195)) | (1L << (REVOKE - 195)) | (1L << (GRANT - 195)) | (1L << (LOCK - 195)) | (1L << (UNLOCK - 195)) | (1L << (MSCK - 195)) | (1L << (REPAIR - 195)) | (1L << (EXPORT - 195)) | (1L << (IMPORT - 195)) | (1L << (LOAD - 195)) | (1L << (ROLE - 195)) | (1L << (ROLES - 195)) | (1L << (COMPACTIONS - 195)) | (1L << (PRINCIPALS - 195)) | (1L << (TRANSACTIONS - 195)) | (1L << (INDEX - 195)) | (1L << (INDEXES - 195)) | (1L << (LOCKS - 195)) | (1L << (OPTION - 195)) | (1L << (ANTI - 195)) | (1L << (LOCAL - 195)) | (1L << (INPATH - 195)) | (1L << (STRING - 195)) | (1L << (BIGINT_LITERAL - 195)) | (1L << (SMALLINT_LITERAL - 195)) | (1L << (TINYINT_LITERAL - 195)) | (1L << (INTEGER_VALUE - 195)) | (1L << (DECIMAL_VALUE - 195)) | (1L << (SCIENTIFIC_DECIMAL_VALUE - 195)) | (1L << (DOUBLE_LITERAL - 195)) | (1L << (IDENTIFIER - 195)) | (1L << (BACKQUOTED_IDENTIFIER - 195)))) != 0)) {
					{
					setState(1419);
					expression();
					setState(1424);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__2) {
						{
						{
						setState(1420);
						match(T__2);
						setState(1421);
						expression();
						}
						}
						setState(1426);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(1429);
				match(T__1);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1430);
				expression();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class LateralViewContext extends ParserRuleContext {
		public IdentifierContext tblName;
		public IdentifierContext identifier;
		public List<IdentifierContext> colName = new ArrayList<IdentifierContext>();
		public TerminalNode LATERAL() { return getToken(SqlBaseParser.LATERAL, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public TerminalNode OUTER() { return getToken(SqlBaseParser.OUTER, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public LateralViewContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_lateralView; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLateralView(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLateralView(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLateralView(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LateralViewContext lateralView() throws RecognitionException {
		LateralViewContext _localctx = new LateralViewContext(_ctx, getState());
		enterRule(_localctx, 82, RULE_lateralView);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1433);
			match(LATERAL);
			setState(1434);
			match(VIEW);
			setState(1436);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,184,_ctx) ) {
			case 1:
				{
				setState(1435);
				match(OUTER);
				}
				break;
			}
			setState(1438);
			qualifiedName();
			setState(1439);
			match(T__0);
			setState(1448);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << SELECT) | (1L << FROM) | (1L << ADD) | (1L << AS) | (1L << ALL) | (1L << DISTINCT) | (1L << WHERE) | (1L << GROUP) | (1L << BY) | (1L << GROUPING) | (1L << SETS) | (1L << CUBE) | (1L << ROLLUP) | (1L << ORDER) | (1L << HAVING) | (1L << LIMIT) | (1L << AT) | (1L << OR) | (1L << AND) | (1L << IN) | (1L << NOT) | (1L << NO) | (1L << EXISTS) | (1L << BETWEEN) | (1L << LIKE) | (1L << RLIKE) | (1L << IS) | (1L << NULL) | (1L << TRUE) | (1L << FALSE) | (1L << NULLS) | (1L << ASC) | (1L << DESC) | (1L << FOR) | (1L << INTERVAL) | (1L << CASE) | (1L << WHEN) | (1L << THEN) | (1L << ELSE) | (1L << END) | (1L << JOIN) | (1L << CROSS) | (1L << OUTER) | (1L << INNER) | (1L << LEFT) | (1L << SEMI) | (1L << RIGHT) | (1L << FULL) | (1L << NATURAL) | (1L << ON) | (1L << LATERAL) | (1L << WINDOW) | (1L << OVER) | (1L << PARTITION) | (1L << RANGE) | (1L << ROWS))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (UNBOUNDED - 64)) | (1L << (PRECEDING - 64)) | (1L << (FOLLOWING - 64)) | (1L << (CURRENT - 64)) | (1L << (ROW - 64)) | (1L << (WITH - 64)) | (1L << (VALUES - 64)) | (1L << (CREATE - 64)) | (1L << (TABLE - 64)) | (1L << (VIEW - 64)) | (1L << (REPLACE - 64)) | (1L << (INSERT - 64)) | (1L << (DELETE - 64)) | (1L << (INTO - 64)) | (1L << (DESCRIBE - 64)) | (1L << (EXPLAIN - 64)) | (1L << (FORMAT - 64)) | (1L << (LOGICAL - 64)) | (1L << (CODEGEN - 64)) | (1L << (CAST - 64)) | (1L << (SHOW - 64)) | (1L << (TABLES - 64)) | (1L << (COLUMNS - 64)) | (1L << (COLUMN - 64)) | (1L << (USE - 64)) | (1L << (PARTITIONS - 64)) | (1L << (FUNCTIONS - 64)) | (1L << (DROP - 64)) | (1L << (UNION - 64)) | (1L << (EXCEPT - 64)) | (1L << (INTERSECT - 64)) | (1L << (TO - 64)) | (1L << (TABLESAMPLE - 64)) | (1L << (STRATIFY - 64)) | (1L << (ALTER - 64)) | (1L << (RENAME - 64)) | (1L << (ARRAY - 64)) | (1L << (MAP - 64)) | (1L << (STRUCT - 64)) | (1L << (COMMENT - 64)) | (1L << (SET - 64)) | (1L << (RESET - 64)) | (1L << (DATA - 64)) | (1L << (START - 64)) | (1L << (TRANSACTION - 64)) | (1L << (COMMIT - 64)) | (1L << (ROLLBACK - 64)) | (1L << (MACRO - 64)) | (1L << (IF - 64)) | (1L << (PLUS - 64)) | (1L << (MINUS - 64)) | (1L << (ASTERISK - 64)) | (1L << (DIV - 64)) | (1L << (TILDE - 64)))) != 0) || ((((_la - 131)) & ~0x3f) == 0 && ((1L << (_la - 131)) & ((1L << (PERCENTLIT - 131)) | (1L << (BUCKET - 131)) | (1L << (OUT - 131)) | (1L << (OF - 131)) | (1L << (SORT - 131)) | (1L << (CLUSTER - 131)) | (1L << (DISTRIBUTE - 131)) | (1L << (OVERWRITE - 131)) | (1L << (TRANSFORM - 131)) | (1L << (REDUCE - 131)) | (1L << (USING - 131)) | (1L << (SERDE - 131)) | (1L << (SERDEPROPERTIES - 131)) | (1L << (RECORDREADER - 131)) | (1L << (RECORDWRITER - 131)) | (1L << (DELIMITED - 131)) | (1L << (FIELDS - 131)) | (1L << (TERMINATED - 131)) | (1L << (COLLECTION - 131)) | (1L << (ITEMS - 131)) | (1L << (KEYS - 131)) | (1L << (ESCAPED - 131)) | (1L << (LINES - 131)) | (1L << (SEPARATED - 131)) | (1L << (FUNCTION - 131)) | (1L << (EXTENDED - 131)) | (1L << (REFRESH - 131)) | (1L << (CLEAR - 131)) | (1L << (CACHE - 131)) | (1L << (UNCACHE - 131)) | (1L << (LAZY - 131)) | (1L << (FORMATTED - 131)) | (1L << (TEMPORARY - 131)) | (1L << (OPTIONS - 131)) | (1L << (UNSET - 131)) | (1L << (TBLPROPERTIES - 131)) | (1L << (DBPROPERTIES - 131)) | (1L << (BUCKETS - 131)) | (1L << (SKEWED - 131)) | (1L << (STORED - 131)) | (1L << (DIRECTORIES - 131)) | (1L << (LOCATION - 131)) | (1L << (EXCHANGE - 131)) | (1L << (ARCHIVE - 131)) | (1L << (UNARCHIVE - 131)) | (1L << (FILEFORMAT - 131)) | (1L << (TOUCH - 131)) | (1L << (COMPACT - 131)) | (1L << (CONCATENATE - 131)) | (1L << (CHANGE - 131)) | (1L << (CASCADE - 131)) | (1L << (RESTRICT - 131)) | (1L << (CLUSTERED - 131)) | (1L << (SORTED - 131)) | (1L << (PURGE - 131)) | (1L << (INPUTFORMAT - 131)) | (1L << (OUTPUTFORMAT - 131)) | (1L << (DATABASE - 131)) | (1L << (DATABASES - 131)) | (1L << (DFS - 131)) | (1L << (TRUNCATE - 131)) | (1L << (ANALYZE - 131)) | (1L << (COMPUTE - 131)) | (1L << (LIST - 131)))) != 0) || ((((_la - 195)) & ~0x3f) == 0 && ((1L << (_la - 195)) & ((1L << (STATISTICS - 195)) | (1L << (PARTITIONED - 195)) | (1L << (EXTERNAL - 195)) | (1L << (DEFINED - 195)) | (1L << (REVOKE - 195)) | (1L << (GRANT - 195)) | (1L << (LOCK - 195)) | (1L << (UNLOCK - 195)) | (1L << (MSCK - 195)) | (1L << (REPAIR - 195)) | (1L << (EXPORT - 195)) | (1L << (IMPORT - 195)) | (1L << (LOAD - 195)) | (1L << (ROLE - 195)) | (1L << (ROLES - 195)) | (1L << (COMPACTIONS - 195)) | (1L << (PRINCIPALS - 195)) | (1L << (TRANSACTIONS - 195)) | (1L << (INDEX - 195)) | (1L << (INDEXES - 195)) | (1L << (LOCKS - 195)) | (1L << (OPTION - 195)) | (1L << (ANTI - 195)) | (1L << (LOCAL - 195)) | (1L << (INPATH - 195)) | (1L << (STRING - 195)) | (1L << (BIGINT_LITERAL - 195)) | (1L << (SMALLINT_LITERAL - 195)) | (1L << (TINYINT_LITERAL - 195)) | (1L << (INTEGER_VALUE - 195)) | (1L << (DECIMAL_VALUE - 195)) | (1L << (SCIENTIFIC_DECIMAL_VALUE - 195)) | (1L << (DOUBLE_LITERAL - 195)) | (1L << (IDENTIFIER - 195)) | (1L << (BACKQUOTED_IDENTIFIER - 195)))) != 0)) {
				{
				setState(1440);
				expression();
				setState(1445);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(1441);
					match(T__2);
					setState(1442);
					expression();
					}
					}
					setState(1447);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(1450);
			match(T__1);
			setState(1451);
			((LateralViewContext)_localctx).tblName = identifier();
			setState(1463);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,189,_ctx) ) {
			case 1:
				{
				setState(1453);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,187,_ctx) ) {
				case 1:
					{
					setState(1452);
					match(AS);
					}
					break;
				}
				setState(1455);
				((LateralViewContext)_localctx).identifier = identifier();
				((LateralViewContext)_localctx).colName.add(((LateralViewContext)_localctx).identifier);
				setState(1460);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,188,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(1456);
						match(T__2);
						setState(1457);
						((LateralViewContext)_localctx).identifier = identifier();
						((LateralViewContext)_localctx).colName.add(((LateralViewContext)_localctx).identifier);
						}
						} 
					}
					setState(1462);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,188,_ctx);
				}
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SetQuantifierContext extends ParserRuleContext {
		public TerminalNode DISTINCT() { return getToken(SqlBaseParser.DISTINCT, 0); }
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public SetQuantifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_setQuantifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetQuantifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetQuantifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetQuantifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SetQuantifierContext setQuantifier() throws RecognitionException {
		SetQuantifierContext _localctx = new SetQuantifierContext(_ctx, getState());
		enterRule(_localctx, 84, RULE_setQuantifier);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1465);
			_la = _input.LA(1);
			if ( !(_la==ALL || _la==DISTINCT) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RelationContext extends ParserRuleContext {
		public RelationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_relation; }
	 
		public RelationContext() { }
		public void copyFrom(RelationContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class RelationDefaultContext extends RelationContext {
		public RelationPrimaryContext relationPrimary() {
			return getRuleContext(RelationPrimaryContext.class,0);
		}
		public RelationDefaultContext(RelationContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRelationDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRelationDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRelationDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class JoinRelationContext extends RelationContext {
		public RelationContext left;
		public RelationContext right;
		public List<RelationContext> relation() {
			return getRuleContexts(RelationContext.class);
		}
		public RelationContext relation(int i) {
			return getRuleContext(RelationContext.class,i);
		}
		public TerminalNode JOIN() { return getToken(SqlBaseParser.JOIN, 0); }
		public TerminalNode NATURAL() { return getToken(SqlBaseParser.NATURAL, 0); }
		public JoinTypeContext joinType() {
			return getRuleContext(JoinTypeContext.class,0);
		}
		public TerminalNode CROSS() { return getToken(SqlBaseParser.CROSS, 0); }
		public JoinCriteriaContext joinCriteria() {
			return getRuleContext(JoinCriteriaContext.class,0);
		}
		public JoinRelationContext(RelationContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJoinRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJoinRelation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJoinRelation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RelationContext relation() throws RecognitionException {
		return relation(0);
	}

	private RelationContext relation(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		RelationContext _localctx = new RelationContext(_ctx, _parentState);
		RelationContext _prevctx = _localctx;
		int _startState = 86;
		enterRecursionRule(_localctx, 86, RULE_relation, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			{
			_localctx = new RelationDefaultContext(_localctx);
			_ctx = _localctx;
			_prevctx = _localctx;

			setState(1468);
			relationPrimary();
			}
			_ctx.stop = _input.LT(-1);
			setState(1489);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,193,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					{
					_localctx = new JoinRelationContext(new RelationContext(_parentctx, _parentState));
					((JoinRelationContext)_localctx).left = _prevctx;
					pushNewRecursionContext(_localctx, _startState, RULE_relation);
					setState(1470);
					if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
					setState(1485);
					_errHandler.sync(this);
					switch (_input.LA(1)) {
					case JOIN:
					case CROSS:
					case INNER:
					case LEFT:
					case RIGHT:
					case FULL:
					case ANTI:
						{
						setState(1473);
						_errHandler.sync(this);
						switch (_input.LA(1)) {
						case CROSS:
							{
							setState(1471);
							match(CROSS);
							}
							break;
						case JOIN:
						case INNER:
						case LEFT:
						case RIGHT:
						case FULL:
						case ANTI:
							{
							setState(1472);
							joinType();
							}
							break;
						default:
							throw new NoViableAltException(this);
						}
						setState(1475);
						match(JOIN);
						setState(1476);
						((JoinRelationContext)_localctx).right = relation(0);
						setState(1478);
						_errHandler.sync(this);
						switch ( getInterpreter().adaptivePredict(_input,191,_ctx) ) {
						case 1:
							{
							setState(1477);
							joinCriteria();
							}
							break;
						}
						}
						break;
					case NATURAL:
						{
						setState(1480);
						match(NATURAL);
						setState(1481);
						joinType();
						setState(1482);
						match(JOIN);
						setState(1483);
						((JoinRelationContext)_localctx).right = relation(0);
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					}
					} 
				}
				setState(1491);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,193,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class JoinTypeContext extends ParserRuleContext {
		public TerminalNode INNER() { return getToken(SqlBaseParser.INNER, 0); }
		public TerminalNode LEFT() { return getToken(SqlBaseParser.LEFT, 0); }
		public TerminalNode OUTER() { return getToken(SqlBaseParser.OUTER, 0); }
		public TerminalNode SEMI() { return getToken(SqlBaseParser.SEMI, 0); }
		public TerminalNode RIGHT() { return getToken(SqlBaseParser.RIGHT, 0); }
		public TerminalNode FULL() { return getToken(SqlBaseParser.FULL, 0); }
		public TerminalNode ANTI() { return getToken(SqlBaseParser.ANTI, 0); }
		public JoinTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJoinType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJoinType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJoinType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JoinTypeContext joinType() throws RecognitionException {
		JoinTypeContext _localctx = new JoinTypeContext(_ctx, getState());
		enterRule(_localctx, 88, RULE_joinType);
		int _la;
		try {
			setState(1513);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,199,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1493);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==INNER) {
					{
					setState(1492);
					match(INNER);
					}
				}

				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1495);
				match(LEFT);
				setState(1497);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OUTER) {
					{
					setState(1496);
					match(OUTER);
					}
				}

				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(1499);
				match(LEFT);
				setState(1500);
				match(SEMI);
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(1501);
				match(RIGHT);
				setState(1503);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OUTER) {
					{
					setState(1502);
					match(OUTER);
					}
				}

				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(1505);
				match(FULL);
				setState(1507);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OUTER) {
					{
					setState(1506);
					match(OUTER);
					}
				}

				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(1510);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LEFT) {
					{
					setState(1509);
					match(LEFT);
					}
				}

				setState(1512);
				match(ANTI);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class JoinCriteriaContext extends ParserRuleContext {
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public TerminalNode USING() { return getToken(SqlBaseParser.USING, 0); }
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public JoinCriteriaContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinCriteria; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJoinCriteria(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJoinCriteria(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJoinCriteria(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JoinCriteriaContext joinCriteria() throws RecognitionException {
		JoinCriteriaContext _localctx = new JoinCriteriaContext(_ctx, getState());
		enterRule(_localctx, 90, RULE_joinCriteria);
		int _la;
		try {
			setState(1529);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ON:
				enterOuterAlt(_localctx, 1);
				{
				setState(1515);
				match(ON);
				setState(1516);
				booleanExpression(0);
				}
				break;
			case USING:
				enterOuterAlt(_localctx, 2);
				{
				setState(1517);
				match(USING);
				setState(1518);
				match(T__0);
				setState(1519);
				identifier();
				setState(1524);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(1520);
					match(T__2);
					setState(1521);
					identifier();
					}
					}
					setState(1526);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1527);
				match(T__1);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SampleContext extends ParserRuleContext {
		public Token percentage;
		public Token sampleType;
		public Token numerator;
		public Token denominator;
		public TerminalNode TABLESAMPLE() { return getToken(SqlBaseParser.TABLESAMPLE, 0); }
		public TerminalNode BYTELENGTH_LITERAL() { return getToken(SqlBaseParser.BYTELENGTH_LITERAL, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode OUT() { return getToken(SqlBaseParser.OUT, 0); }
		public TerminalNode OF() { return getToken(SqlBaseParser.OF, 0); }
		public TerminalNode PERCENTLIT() { return getToken(SqlBaseParser.PERCENTLIT, 0); }
		public TerminalNode ROWS() { return getToken(SqlBaseParser.ROWS, 0); }
		public TerminalNode BUCKET() { return getToken(SqlBaseParser.BUCKET, 0); }
		public List<TerminalNode> INTEGER_VALUE() { return getTokens(SqlBaseParser.INTEGER_VALUE); }
		public TerminalNode INTEGER_VALUE(int i) {
			return getToken(SqlBaseParser.INTEGER_VALUE, i);
		}
		public TerminalNode DECIMAL_VALUE() { return getToken(SqlBaseParser.DECIMAL_VALUE, 0); }
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public SampleContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sample; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSample(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSample(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSample(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SampleContext sample() throws RecognitionException {
		SampleContext _localctx = new SampleContext(_ctx, getState());
		enterRule(_localctx, 92, RULE_sample);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1531);
			match(TABLESAMPLE);
			setState(1532);
			match(T__0);
			setState(1554);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,204,_ctx) ) {
			case 1:
				{
				{
				setState(1533);
				((SampleContext)_localctx).percentage = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==INTEGER_VALUE || _la==DECIMAL_VALUE) ) {
					((SampleContext)_localctx).percentage = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(1534);
				((SampleContext)_localctx).sampleType = match(PERCENTLIT);
				}
				}
				break;
			case 2:
				{
				{
				setState(1535);
				expression();
				setState(1536);
				((SampleContext)_localctx).sampleType = match(ROWS);
				}
				}
				break;
			case 3:
				{
				setState(1538);
				((SampleContext)_localctx).sampleType = match(BYTELENGTH_LITERAL);
				}
				break;
			case 4:
				{
				{
				setState(1539);
				((SampleContext)_localctx).sampleType = match(BUCKET);
				setState(1540);
				((SampleContext)_localctx).numerator = match(INTEGER_VALUE);
				setState(1541);
				match(OUT);
				setState(1542);
				match(OF);
				setState(1543);
				((SampleContext)_localctx).denominator = match(INTEGER_VALUE);
				setState(1552);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ON) {
					{
					setState(1544);
					match(ON);
					setState(1550);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,202,_ctx) ) {
					case 1:
						{
						setState(1545);
						identifier();
						}
						break;
					case 2:
						{
						setState(1546);
						qualifiedName();
						setState(1547);
						match(T__0);
						setState(1548);
						match(T__1);
						}
						break;
					}
					}
				}

				}
				}
				break;
			}
			setState(1556);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IdentifierListContext extends ParserRuleContext {
		public IdentifierSeqContext identifierSeq() {
			return getRuleContext(IdentifierSeqContext.class,0);
		}
		public IdentifierListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifierList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIdentifierList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIdentifierList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIdentifierList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IdentifierListContext identifierList() throws RecognitionException {
		IdentifierListContext _localctx = new IdentifierListContext(_ctx, getState());
		enterRule(_localctx, 94, RULE_identifierList);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1558);
			match(T__0);
			setState(1559);
			identifierSeq();
			setState(1560);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IdentifierSeqContext extends ParserRuleContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public IdentifierSeqContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifierSeq; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIdentifierSeq(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIdentifierSeq(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIdentifierSeq(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IdentifierSeqContext identifierSeq() throws RecognitionException {
		IdentifierSeqContext _localctx = new IdentifierSeqContext(_ctx, getState());
		enterRule(_localctx, 96, RULE_identifierSeq);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1562);
			identifier();
			setState(1567);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,205,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1563);
					match(T__2);
					setState(1564);
					identifier();
					}
					} 
				}
				setState(1569);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,205,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OrderedIdentifierListContext extends ParserRuleContext {
		public List<OrderedIdentifierContext> orderedIdentifier() {
			return getRuleContexts(OrderedIdentifierContext.class);
		}
		public OrderedIdentifierContext orderedIdentifier(int i) {
			return getRuleContext(OrderedIdentifierContext.class,i);
		}
		public OrderedIdentifierListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_orderedIdentifierList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterOrderedIdentifierList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitOrderedIdentifierList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitOrderedIdentifierList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OrderedIdentifierListContext orderedIdentifierList() throws RecognitionException {
		OrderedIdentifierListContext _localctx = new OrderedIdentifierListContext(_ctx, getState());
		enterRule(_localctx, 98, RULE_orderedIdentifierList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1570);
			match(T__0);
			setState(1571);
			orderedIdentifier();
			setState(1576);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(1572);
				match(T__2);
				setState(1573);
				orderedIdentifier();
				}
				}
				setState(1578);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1579);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OrderedIdentifierContext extends ParserRuleContext {
		public Token ordering;
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode ASC() { return getToken(SqlBaseParser.ASC, 0); }
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public OrderedIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_orderedIdentifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterOrderedIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitOrderedIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitOrderedIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OrderedIdentifierContext orderedIdentifier() throws RecognitionException {
		OrderedIdentifierContext _localctx = new OrderedIdentifierContext(_ctx, getState());
		enterRule(_localctx, 100, RULE_orderedIdentifier);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1581);
			identifier();
			setState(1583);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ASC || _la==DESC) {
				{
				setState(1582);
				((OrderedIdentifierContext)_localctx).ordering = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==ASC || _la==DESC) ) {
					((OrderedIdentifierContext)_localctx).ordering = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IdentifierCommentListContext extends ParserRuleContext {
		public List<IdentifierCommentContext> identifierComment() {
			return getRuleContexts(IdentifierCommentContext.class);
		}
		public IdentifierCommentContext identifierComment(int i) {
			return getRuleContext(IdentifierCommentContext.class,i);
		}
		public IdentifierCommentListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifierCommentList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIdentifierCommentList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIdentifierCommentList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIdentifierCommentList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IdentifierCommentListContext identifierCommentList() throws RecognitionException {
		IdentifierCommentListContext _localctx = new IdentifierCommentListContext(_ctx, getState());
		enterRule(_localctx, 102, RULE_identifierCommentList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1585);
			match(T__0);
			setState(1586);
			identifierComment();
			setState(1591);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(1587);
				match(T__2);
				setState(1588);
				identifierComment();
				}
				}
				setState(1593);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1594);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IdentifierCommentContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public IdentifierCommentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifierComment; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIdentifierComment(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIdentifierComment(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIdentifierComment(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IdentifierCommentContext identifierComment() throws RecognitionException {
		IdentifierCommentContext _localctx = new IdentifierCommentContext(_ctx, getState());
		enterRule(_localctx, 104, RULE_identifierComment);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1596);
			identifier();
			setState(1599);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==COMMENT) {
				{
				setState(1597);
				match(COMMENT);
				setState(1598);
				match(STRING);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RelationPrimaryContext extends ParserRuleContext {
		public RelationPrimaryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_relationPrimary; }
	 
		public RelationPrimaryContext() { }
		public void copyFrom(RelationPrimaryContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class InlineTableDefault2Context extends RelationPrimaryContext {
		public InlineTableContext inlineTable() {
			return getRuleContext(InlineTableContext.class,0);
		}
		public InlineTableDefault2Context(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterInlineTableDefault2(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitInlineTableDefault2(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitInlineTableDefault2(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class AliasedRelationContext extends RelationPrimaryContext {
		public RelationContext relation() {
			return getRuleContext(RelationContext.class,0);
		}
		public SampleContext sample() {
			return getRuleContext(SampleContext.class,0);
		}
		public StrictIdentifierContext strictIdentifier() {
			return getRuleContext(StrictIdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public AliasedRelationContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAliasedRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAliasedRelation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitAliasedRelation(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class AliasedQueryContext extends RelationPrimaryContext {
		public QueryNoWithContext queryNoWith() {
			return getRuleContext(QueryNoWithContext.class,0);
		}
		public SampleContext sample() {
			return getRuleContext(SampleContext.class,0);
		}
		public StrictIdentifierContext strictIdentifier() {
			return getRuleContext(StrictIdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public AliasedQueryContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAliasedQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAliasedQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitAliasedQuery(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class TableNameContext extends RelationPrimaryContext {
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public SampleContext sample() {
			return getRuleContext(SampleContext.class,0);
		}
		public StrictIdentifierContext strictIdentifier() {
			return getRuleContext(StrictIdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public TableNameContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTableName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RelationPrimaryContext relationPrimary() throws RecognitionException {
		RelationPrimaryContext _localctx = new RelationPrimaryContext(_ctx, getState());
		enterRule(_localctx, 106, RULE_relationPrimary);
		try {
			setState(1636);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,219,_ctx) ) {
			case 1:
				_localctx = new TableNameContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1601);
				tableIdentifier();
				setState(1603);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,210,_ctx) ) {
				case 1:
					{
					setState(1602);
					sample();
					}
					break;
				}
				setState(1609);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,212,_ctx) ) {
				case 1:
					{
					setState(1606);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,211,_ctx) ) {
					case 1:
						{
						setState(1605);
						match(AS);
						}
						break;
					}
					setState(1608);
					strictIdentifier();
					}
					break;
				}
				}
				break;
			case 2:
				_localctx = new AliasedQueryContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1611);
				match(T__0);
				setState(1612);
				queryNoWith();
				setState(1613);
				match(T__1);
				setState(1615);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,213,_ctx) ) {
				case 1:
					{
					setState(1614);
					sample();
					}
					break;
				}
				setState(1621);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,215,_ctx) ) {
				case 1:
					{
					setState(1618);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,214,_ctx) ) {
					case 1:
						{
						setState(1617);
						match(AS);
						}
						break;
					}
					setState(1620);
					strictIdentifier();
					}
					break;
				}
				}
				break;
			case 3:
				_localctx = new AliasedRelationContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1623);
				match(T__0);
				setState(1624);
				relation(0);
				setState(1625);
				match(T__1);
				setState(1627);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,216,_ctx) ) {
				case 1:
					{
					setState(1626);
					sample();
					}
					break;
				}
				setState(1633);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,218,_ctx) ) {
				case 1:
					{
					setState(1630);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,217,_ctx) ) {
					case 1:
						{
						setState(1629);
						match(AS);
						}
						break;
					}
					setState(1632);
					strictIdentifier();
					}
					break;
				}
				}
				break;
			case 4:
				_localctx = new InlineTableDefault2Context(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(1635);
				inlineTable();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class InlineTableContext extends ParserRuleContext {
		public TerminalNode VALUES() { return getToken(SqlBaseParser.VALUES, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public IdentifierListContext identifierList() {
			return getRuleContext(IdentifierListContext.class,0);
		}
		public InlineTableContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_inlineTable; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterInlineTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitInlineTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitInlineTable(this);
			else return visitor.visitChildren(this);
		}
	}

	public final InlineTableContext inlineTable() throws RecognitionException {
		InlineTableContext _localctx = new InlineTableContext(_ctx, getState());
		enterRule(_localctx, 108, RULE_inlineTable);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1638);
			match(VALUES);
			setState(1639);
			expression();
			setState(1644);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,220,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1640);
					match(T__2);
					setState(1641);
					expression();
					}
					} 
				}
				setState(1646);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,220,_ctx);
			}
			setState(1654);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,223,_ctx) ) {
			case 1:
				{
				setState(1648);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,221,_ctx) ) {
				case 1:
					{
					setState(1647);
					match(AS);
					}
					break;
				}
				setState(1650);
				identifier();
				setState(1652);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,222,_ctx) ) {
				case 1:
					{
					setState(1651);
					identifierList();
					}
					break;
				}
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RowFormatContext extends ParserRuleContext {
		public RowFormatContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_rowFormat; }
	 
		public RowFormatContext() { }
		public void copyFrom(RowFormatContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class RowFormatSerdeContext extends RowFormatContext {
		public Token name;
		public TablePropertyListContext props;
		public TerminalNode ROW() { return getToken(SqlBaseParser.ROW, 0); }
		public TerminalNode FORMAT() { return getToken(SqlBaseParser.FORMAT, 0); }
		public TerminalNode SERDE() { return getToken(SqlBaseParser.SERDE, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode SERDEPROPERTIES() { return getToken(SqlBaseParser.SERDEPROPERTIES, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public RowFormatSerdeContext(RowFormatContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRowFormatSerde(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRowFormatSerde(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRowFormatSerde(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class RowFormatDelimitedContext extends RowFormatContext {
		public Token fieldsTerminatedBy;
		public Token escapedBy;
		public Token collectionItemsTerminatedBy;
		public Token keysTerminatedBy;
		public Token linesSeparatedBy;
		public Token nullDefinedAs;
		public TerminalNode ROW() { return getToken(SqlBaseParser.ROW, 0); }
		public TerminalNode FORMAT() { return getToken(SqlBaseParser.FORMAT, 0); }
		public TerminalNode DELIMITED() { return getToken(SqlBaseParser.DELIMITED, 0); }
		public TerminalNode FIELDS() { return getToken(SqlBaseParser.FIELDS, 0); }
		public List<TerminalNode> TERMINATED() { return getTokens(SqlBaseParser.TERMINATED); }
		public TerminalNode TERMINATED(int i) {
			return getToken(SqlBaseParser.TERMINATED, i);
		}
		public List<TerminalNode> BY() { return getTokens(SqlBaseParser.BY); }
		public TerminalNode BY(int i) {
			return getToken(SqlBaseParser.BY, i);
		}
		public TerminalNode COLLECTION() { return getToken(SqlBaseParser.COLLECTION, 0); }
		public TerminalNode ITEMS() { return getToken(SqlBaseParser.ITEMS, 0); }
		public TerminalNode MAP() { return getToken(SqlBaseParser.MAP, 0); }
		public TerminalNode KEYS() { return getToken(SqlBaseParser.KEYS, 0); }
		public TerminalNode LINES() { return getToken(SqlBaseParser.LINES, 0); }
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public TerminalNode DEFINED() { return getToken(SqlBaseParser.DEFINED, 0); }
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public List<TerminalNode> STRING() { return getTokens(SqlBaseParser.STRING); }
		public TerminalNode STRING(int i) {
			return getToken(SqlBaseParser.STRING, i);
		}
		public TerminalNode ESCAPED() { return getToken(SqlBaseParser.ESCAPED, 0); }
		public RowFormatDelimitedContext(RowFormatContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRowFormatDelimited(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRowFormatDelimited(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRowFormatDelimited(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RowFormatContext rowFormat() throws RecognitionException {
		RowFormatContext _localctx = new RowFormatContext(_ctx, getState());
		enterRule(_localctx, 110, RULE_rowFormat);
		try {
			setState(1705);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,231,_ctx) ) {
			case 1:
				_localctx = new RowFormatSerdeContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1656);
				match(ROW);
				setState(1657);
				match(FORMAT);
				setState(1658);
				match(SERDE);
				setState(1659);
				((RowFormatSerdeContext)_localctx).name = match(STRING);
				setState(1663);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,224,_ctx) ) {
				case 1:
					{
					setState(1660);
					match(WITH);
					setState(1661);
					match(SERDEPROPERTIES);
					setState(1662);
					((RowFormatSerdeContext)_localctx).props = tablePropertyList();
					}
					break;
				}
				}
				break;
			case 2:
				_localctx = new RowFormatDelimitedContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1665);
				match(ROW);
				setState(1666);
				match(FORMAT);
				setState(1667);
				match(DELIMITED);
				setState(1677);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,226,_ctx) ) {
				case 1:
					{
					setState(1668);
					match(FIELDS);
					setState(1669);
					match(TERMINATED);
					setState(1670);
					match(BY);
					setState(1671);
					((RowFormatDelimitedContext)_localctx).fieldsTerminatedBy = match(STRING);
					setState(1675);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,225,_ctx) ) {
					case 1:
						{
						setState(1672);
						match(ESCAPED);
						setState(1673);
						match(BY);
						setState(1674);
						((RowFormatDelimitedContext)_localctx).escapedBy = match(STRING);
						}
						break;
					}
					}
					break;
				}
				setState(1684);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,227,_ctx) ) {
				case 1:
					{
					setState(1679);
					match(COLLECTION);
					setState(1680);
					match(ITEMS);
					setState(1681);
					match(TERMINATED);
					setState(1682);
					match(BY);
					setState(1683);
					((RowFormatDelimitedContext)_localctx).collectionItemsTerminatedBy = match(STRING);
					}
					break;
				}
				setState(1691);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,228,_ctx) ) {
				case 1:
					{
					setState(1686);
					match(MAP);
					setState(1687);
					match(KEYS);
					setState(1688);
					match(TERMINATED);
					setState(1689);
					match(BY);
					setState(1690);
					((RowFormatDelimitedContext)_localctx).keysTerminatedBy = match(STRING);
					}
					break;
				}
				setState(1697);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,229,_ctx) ) {
				case 1:
					{
					setState(1693);
					match(LINES);
					setState(1694);
					match(TERMINATED);
					setState(1695);
					match(BY);
					setState(1696);
					((RowFormatDelimitedContext)_localctx).linesSeparatedBy = match(STRING);
					}
					break;
				}
				setState(1703);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,230,_ctx) ) {
				case 1:
					{
					setState(1699);
					match(NULL);
					setState(1700);
					match(DEFINED);
					setState(1701);
					match(AS);
					setState(1702);
					((RowFormatDelimitedContext)_localctx).nullDefinedAs = match(STRING);
					}
					break;
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TableIdentifierContext extends ParserRuleContext {
		public IdentifierContext db;
		public IdentifierContext table;
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public TableIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tableIdentifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTableIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TableIdentifierContext tableIdentifier() throws RecognitionException {
		TableIdentifierContext _localctx = new TableIdentifierContext(_ctx, getState());
		enterRule(_localctx, 112, RULE_tableIdentifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1710);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,232,_ctx) ) {
			case 1:
				{
				setState(1707);
				((TableIdentifierContext)_localctx).db = identifier();
				setState(1708);
				match(T__3);
				}
				break;
			}
			setState(1712);
			((TableIdentifierContext)_localctx).table = identifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NamedExpressionContext extends ParserRuleContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public IdentifierListContext identifierList() {
			return getRuleContext(IdentifierListContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public NamedExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_namedExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNamedExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNamedExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNamedExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NamedExpressionContext namedExpression() throws RecognitionException {
		NamedExpressionContext _localctx = new NamedExpressionContext(_ctx, getState());
		enterRule(_localctx, 114, RULE_namedExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1714);
			expression();
			setState(1722);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,235,_ctx) ) {
			case 1:
				{
				setState(1716);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,233,_ctx) ) {
				case 1:
					{
					setState(1715);
					match(AS);
					}
					break;
				}
				setState(1720);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case SELECT:
				case FROM:
				case ADD:
				case AS:
				case ALL:
				case DISTINCT:
				case WHERE:
				case GROUP:
				case BY:
				case GROUPING:
				case SETS:
				case CUBE:
				case ROLLUP:
				case ORDER:
				case HAVING:
				case LIMIT:
				case AT:
				case OR:
				case AND:
				case IN:
				case NOT:
				case NO:
				case EXISTS:
				case BETWEEN:
				case LIKE:
				case RLIKE:
				case IS:
				case NULL:
				case TRUE:
				case FALSE:
				case NULLS:
				case ASC:
				case DESC:
				case FOR:
				case INTERVAL:
				case CASE:
				case WHEN:
				case THEN:
				case ELSE:
				case END:
				case JOIN:
				case CROSS:
				case OUTER:
				case INNER:
				case LEFT:
				case SEMI:
				case RIGHT:
				case FULL:
				case NATURAL:
				case ON:
				case LATERAL:
				case WINDOW:
				case OVER:
				case PARTITION:
				case RANGE:
				case ROWS:
				case UNBOUNDED:
				case PRECEDING:
				case FOLLOWING:
				case CURRENT:
				case ROW:
				case WITH:
				case VALUES:
				case CREATE:
				case TABLE:
				case VIEW:
				case REPLACE:
				case INSERT:
				case DELETE:
				case INTO:
				case DESCRIBE:
				case EXPLAIN:
				case FORMAT:
				case LOGICAL:
				case CODEGEN:
				case CAST:
				case SHOW:
				case TABLES:
				case COLUMNS:
				case COLUMN:
				case USE:
				case PARTITIONS:
				case FUNCTIONS:
				case DROP:
				case UNION:
				case EXCEPT:
				case INTERSECT:
				case TO:
				case TABLESAMPLE:
				case STRATIFY:
				case ALTER:
				case RENAME:
				case ARRAY:
				case MAP:
				case STRUCT:
				case COMMENT:
				case SET:
				case RESET:
				case DATA:
				case START:
				case TRANSACTION:
				case COMMIT:
				case ROLLBACK:
				case MACRO:
				case IF:
				case DIV:
				case PERCENTLIT:
				case BUCKET:
				case OUT:
				case OF:
				case SORT:
				case CLUSTER:
				case DISTRIBUTE:
				case OVERWRITE:
				case TRANSFORM:
				case REDUCE:
				case USING:
				case SERDE:
				case SERDEPROPERTIES:
				case RECORDREADER:
				case RECORDWRITER:
				case DELIMITED:
				case FIELDS:
				case TERMINATED:
				case COLLECTION:
				case ITEMS:
				case KEYS:
				case ESCAPED:
				case LINES:
				case SEPARATED:
				case FUNCTION:
				case EXTENDED:
				case REFRESH:
				case CLEAR:
				case CACHE:
				case UNCACHE:
				case LAZY:
				case FORMATTED:
				case TEMPORARY:
				case OPTIONS:
				case UNSET:
				case TBLPROPERTIES:
				case DBPROPERTIES:
				case BUCKETS:
				case SKEWED:
				case STORED:
				case DIRECTORIES:
				case LOCATION:
				case EXCHANGE:
				case ARCHIVE:
				case UNARCHIVE:
				case FILEFORMAT:
				case TOUCH:
				case COMPACT:
				case CONCATENATE:
				case CHANGE:
				case CASCADE:
				case RESTRICT:
				case CLUSTERED:
				case SORTED:
				case PURGE:
				case INPUTFORMAT:
				case OUTPUTFORMAT:
				case DATABASE:
				case DATABASES:
				case DFS:
				case TRUNCATE:
				case ANALYZE:
				case COMPUTE:
				case LIST:
				case STATISTICS:
				case PARTITIONED:
				case EXTERNAL:
				case DEFINED:
				case REVOKE:
				case GRANT:
				case LOCK:
				case UNLOCK:
				case MSCK:
				case REPAIR:
				case EXPORT:
				case IMPORT:
				case LOAD:
				case ROLE:
				case ROLES:
				case COMPACTIONS:
				case PRINCIPALS:
				case TRANSACTIONS:
				case INDEX:
				case INDEXES:
				case LOCKS:
				case OPTION:
				case ANTI:
				case LOCAL:
				case INPATH:
				case IDENTIFIER:
				case BACKQUOTED_IDENTIFIER:
					{
					setState(1718);
					identifier();
					}
					break;
				case T__0:
					{
					setState(1719);
					identifierList();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NamedExpressionSeqContext extends ParserRuleContext {
		public List<NamedExpressionContext> namedExpression() {
			return getRuleContexts(NamedExpressionContext.class);
		}
		public NamedExpressionContext namedExpression(int i) {
			return getRuleContext(NamedExpressionContext.class,i);
		}
		public NamedExpressionSeqContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_namedExpressionSeq; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNamedExpressionSeq(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNamedExpressionSeq(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNamedExpressionSeq(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NamedExpressionSeqContext namedExpressionSeq() throws RecognitionException {
		NamedExpressionSeqContext _localctx = new NamedExpressionSeqContext(_ctx, getState());
		enterRule(_localctx, 116, RULE_namedExpressionSeq);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1724);
			namedExpression();
			setState(1729);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,236,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1725);
					match(T__2);
					setState(1726);
					namedExpression();
					}
					} 
				}
				setState(1731);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,236,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ExpressionContext extends ParserRuleContext {
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public ExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExpressionContext expression() throws RecognitionException {
		ExpressionContext _localctx = new ExpressionContext(_ctx, getState());
		enterRule(_localctx, 118, RULE_expression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1732);
			booleanExpression(0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class BooleanExpressionContext extends ParserRuleContext {
		public BooleanExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_booleanExpression; }
	 
		public BooleanExpressionContext() { }
		public void copyFrom(BooleanExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class LogicalNotContext extends BooleanExpressionContext {
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public LogicalNotContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLogicalNot(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLogicalNot(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLogicalNot(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class BooleanDefaultContext extends BooleanExpressionContext {
		public PredicatedContext predicated() {
			return getRuleContext(PredicatedContext.class,0);
		}
		public BooleanDefaultContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBooleanDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBooleanDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBooleanDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ExistsContext extends BooleanExpressionContext {
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public ExistsContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExists(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExists(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExists(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class LogicalBinaryContext extends BooleanExpressionContext {
		public BooleanExpressionContext left;
		public Token operator;
		public BooleanExpressionContext right;
		public List<BooleanExpressionContext> booleanExpression() {
			return getRuleContexts(BooleanExpressionContext.class);
		}
		public BooleanExpressionContext booleanExpression(int i) {
			return getRuleContext(BooleanExpressionContext.class,i);
		}
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public TerminalNode OR() { return getToken(SqlBaseParser.OR, 0); }
		public LogicalBinaryContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLogicalBinary(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLogicalBinary(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLogicalBinary(this);
			else return visitor.visitChildren(this);
		}
	}

	public final BooleanExpressionContext booleanExpression() throws RecognitionException {
		return booleanExpression(0);
	}

	private BooleanExpressionContext booleanExpression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		BooleanExpressionContext _localctx = new BooleanExpressionContext(_ctx, _parentState);
		BooleanExpressionContext _prevctx = _localctx;
		int _startState = 120;
		enterRecursionRule(_localctx, 120, RULE_booleanExpression, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1743);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,237,_ctx) ) {
			case 1:
				{
				_localctx = new LogicalNotContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(1735);
				match(NOT);
				setState(1736);
				booleanExpression(5);
				}
				break;
			case 2:
				{
				_localctx = new BooleanDefaultContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1737);
				predicated();
				}
				break;
			case 3:
				{
				_localctx = new ExistsContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1738);
				match(EXISTS);
				setState(1739);
				match(T__0);
				setState(1740);
				query();
				setState(1741);
				match(T__1);
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(1753);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,239,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(1751);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,238,_ctx) ) {
					case 1:
						{
						_localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
						((LogicalBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
						setState(1745);
						if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
						setState(1746);
						((LogicalBinaryContext)_localctx).operator = match(AND);
						setState(1747);
						((LogicalBinaryContext)_localctx).right = booleanExpression(4);
						}
						break;
					case 2:
						{
						_localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
						((LogicalBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
						setState(1748);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(1749);
						((LogicalBinaryContext)_localctx).operator = match(OR);
						setState(1750);
						((LogicalBinaryContext)_localctx).right = booleanExpression(3);
						}
						break;
					}
					} 
				}
				setState(1755);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,239,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class PredicatedContext extends ParserRuleContext {
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public PredicateContext predicate() {
			return getRuleContext(PredicateContext.class,0);
		}
		public PredicatedContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_predicated; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPredicated(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPredicated(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPredicated(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PredicatedContext predicated() throws RecognitionException {
		PredicatedContext _localctx = new PredicatedContext(_ctx, getState());
		enterRule(_localctx, 122, RULE_predicated);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1756);
			valueExpression(0);
			setState(1758);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,240,_ctx) ) {
			case 1:
				{
				setState(1757);
				predicate();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PredicateContext extends ParserRuleContext {
		public Token kind;
		public ValueExpressionContext lower;
		public ValueExpressionContext upper;
		public ValueExpressionContext pattern;
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public TerminalNode BETWEEN() { return getToken(SqlBaseParser.BETWEEN, 0); }
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode RLIKE() { return getToken(SqlBaseParser.RLIKE, 0); }
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public TerminalNode IS() { return getToken(SqlBaseParser.IS, 0); }
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public PredicateContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_predicate; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPredicate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPredicate(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPredicate(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PredicateContext predicate() throws RecognitionException {
		PredicateContext _localctx = new PredicateContext(_ctx, getState());
		enterRule(_localctx, 124, RULE_predicate);
		int _la;
		try {
			setState(1801);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,247,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1761);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(1760);
					match(NOT);
					}
				}

				setState(1763);
				((PredicateContext)_localctx).kind = match(BETWEEN);
				setState(1764);
				((PredicateContext)_localctx).lower = valueExpression(0);
				setState(1765);
				match(AND);
				setState(1766);
				((PredicateContext)_localctx).upper = valueExpression(0);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1769);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(1768);
					match(NOT);
					}
				}

				setState(1771);
				((PredicateContext)_localctx).kind = match(IN);
				setState(1772);
				match(T__0);
				setState(1773);
				expression();
				setState(1778);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(1774);
					match(T__2);
					setState(1775);
					expression();
					}
					}
					setState(1780);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1781);
				match(T__1);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(1784);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(1783);
					match(NOT);
					}
				}

				setState(1786);
				((PredicateContext)_localctx).kind = match(IN);
				setState(1787);
				match(T__0);
				setState(1788);
				query();
				setState(1789);
				match(T__1);
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(1792);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(1791);
					match(NOT);
					}
				}

				setState(1794);
				((PredicateContext)_localctx).kind = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==LIKE || _la==RLIKE) ) {
					((PredicateContext)_localctx).kind = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(1795);
				((PredicateContext)_localctx).pattern = valueExpression(0);
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(1796);
				match(IS);
				setState(1798);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(1797);
					match(NOT);
					}
				}

				setState(1800);
				((PredicateContext)_localctx).kind = match(NULL);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ValueExpressionContext extends ParserRuleContext {
		public ValueExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_valueExpression; }
	 
		public ValueExpressionContext() { }
		public void copyFrom(ValueExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class ValueExpressionDefaultContext extends ValueExpressionContext {
		public PrimaryExpressionContext primaryExpression() {
			return getRuleContext(PrimaryExpressionContext.class,0);
		}
		public ValueExpressionDefaultContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterValueExpressionDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitValueExpressionDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitValueExpressionDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ComparisonContext extends ValueExpressionContext {
		public ValueExpressionContext left;
		public ValueExpressionContext right;
		public ComparisonOperatorContext comparisonOperator() {
			return getRuleContext(ComparisonOperatorContext.class,0);
		}
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public ComparisonContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterComparison(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitComparison(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitComparison(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ArithmeticBinaryContext extends ValueExpressionContext {
		public ValueExpressionContext left;
		public Token operator;
		public ValueExpressionContext right;
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode ASTERISK() { return getToken(SqlBaseParser.ASTERISK, 0); }
		public TerminalNode SLASH() { return getToken(SqlBaseParser.SLASH, 0); }
		public TerminalNode PERCENT() { return getToken(SqlBaseParser.PERCENT, 0); }
		public TerminalNode DIV() { return getToken(SqlBaseParser.DIV, 0); }
		public TerminalNode PLUS() { return getToken(SqlBaseParser.PLUS, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public TerminalNode AMPERSAND() { return getToken(SqlBaseParser.AMPERSAND, 0); }
		public TerminalNode HAT() { return getToken(SqlBaseParser.HAT, 0); }
		public TerminalNode PIPE() { return getToken(SqlBaseParser.PIPE, 0); }
		public ArithmeticBinaryContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterArithmeticBinary(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitArithmeticBinary(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitArithmeticBinary(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ArithmeticUnaryContext extends ValueExpressionContext {
		public Token operator;
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public TerminalNode PLUS() { return getToken(SqlBaseParser.PLUS, 0); }
		public TerminalNode TILDE() { return getToken(SqlBaseParser.TILDE, 0); }
		public ArithmeticUnaryContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterArithmeticUnary(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitArithmeticUnary(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitArithmeticUnary(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ValueExpressionContext valueExpression() throws RecognitionException {
		return valueExpression(0);
	}

	private ValueExpressionContext valueExpression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		ValueExpressionContext _localctx = new ValueExpressionContext(_ctx, _parentState);
		ValueExpressionContext _prevctx = _localctx;
		int _startState = 126;
		enterRecursionRule(_localctx, 126, RULE_valueExpression, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1807);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__0:
			case SELECT:
			case FROM:
			case ADD:
			case AS:
			case ALL:
			case DISTINCT:
			case WHERE:
			case GROUP:
			case BY:
			case GROUPING:
			case SETS:
			case CUBE:
			case ROLLUP:
			case ORDER:
			case HAVING:
			case LIMIT:
			case AT:
			case OR:
			case AND:
			case IN:
			case NOT:
			case NO:
			case EXISTS:
			case BETWEEN:
			case LIKE:
			case RLIKE:
			case IS:
			case NULL:
			case TRUE:
			case FALSE:
			case NULLS:
			case ASC:
			case DESC:
			case FOR:
			case INTERVAL:
			case CASE:
			case WHEN:
			case THEN:
			case ELSE:
			case END:
			case JOIN:
			case CROSS:
			case OUTER:
			case INNER:
			case LEFT:
			case SEMI:
			case RIGHT:
			case FULL:
			case NATURAL:
			case ON:
			case LATERAL:
			case WINDOW:
			case OVER:
			case PARTITION:
			case RANGE:
			case ROWS:
			case UNBOUNDED:
			case PRECEDING:
			case FOLLOWING:
			case CURRENT:
			case ROW:
			case WITH:
			case VALUES:
			case CREATE:
			case TABLE:
			case VIEW:
			case REPLACE:
			case INSERT:
			case DELETE:
			case INTO:
			case DESCRIBE:
			case EXPLAIN:
			case FORMAT:
			case LOGICAL:
			case CODEGEN:
			case CAST:
			case SHOW:
			case TABLES:
			case COLUMNS:
			case COLUMN:
			case USE:
			case PARTITIONS:
			case FUNCTIONS:
			case DROP:
			case UNION:
			case EXCEPT:
			case INTERSECT:
			case TO:
			case TABLESAMPLE:
			case STRATIFY:
			case ALTER:
			case RENAME:
			case ARRAY:
			case MAP:
			case STRUCT:
			case COMMENT:
			case SET:
			case RESET:
			case DATA:
			case START:
			case TRANSACTION:
			case COMMIT:
			case ROLLBACK:
			case MACRO:
			case IF:
			case ASTERISK:
			case DIV:
			case PERCENTLIT:
			case BUCKET:
			case OUT:
			case OF:
			case SORT:
			case CLUSTER:
			case DISTRIBUTE:
			case OVERWRITE:
			case TRANSFORM:
			case REDUCE:
			case USING:
			case SERDE:
			case SERDEPROPERTIES:
			case RECORDREADER:
			case RECORDWRITER:
			case DELIMITED:
			case FIELDS:
			case TERMINATED:
			case COLLECTION:
			case ITEMS:
			case KEYS:
			case ESCAPED:
			case LINES:
			case SEPARATED:
			case FUNCTION:
			case EXTENDED:
			case REFRESH:
			case CLEAR:
			case CACHE:
			case UNCACHE:
			case LAZY:
			case FORMATTED:
			case TEMPORARY:
			case OPTIONS:
			case UNSET:
			case TBLPROPERTIES:
			case DBPROPERTIES:
			case BUCKETS:
			case SKEWED:
			case STORED:
			case DIRECTORIES:
			case LOCATION:
			case EXCHANGE:
			case ARCHIVE:
			case UNARCHIVE:
			case FILEFORMAT:
			case TOUCH:
			case COMPACT:
			case CONCATENATE:
			case CHANGE:
			case CASCADE:
			case RESTRICT:
			case CLUSTERED:
			case SORTED:
			case PURGE:
			case INPUTFORMAT:
			case OUTPUTFORMAT:
			case DATABASE:
			case DATABASES:
			case DFS:
			case TRUNCATE:
			case ANALYZE:
			case COMPUTE:
			case LIST:
			case STATISTICS:
			case PARTITIONED:
			case EXTERNAL:
			case DEFINED:
			case REVOKE:
			case GRANT:
			case LOCK:
			case UNLOCK:
			case MSCK:
			case REPAIR:
			case EXPORT:
			case IMPORT:
			case LOAD:
			case ROLE:
			case ROLES:
			case COMPACTIONS:
			case PRINCIPALS:
			case TRANSACTIONS:
			case INDEX:
			case INDEXES:
			case LOCKS:
			case OPTION:
			case ANTI:
			case LOCAL:
			case INPATH:
			case STRING:
			case BIGINT_LITERAL:
			case SMALLINT_LITERAL:
			case TINYINT_LITERAL:
			case INTEGER_VALUE:
			case DECIMAL_VALUE:
			case SCIENTIFIC_DECIMAL_VALUE:
			case DOUBLE_LITERAL:
			case IDENTIFIER:
			case BACKQUOTED_IDENTIFIER:
				{
				_localctx = new ValueExpressionDefaultContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(1804);
				primaryExpression(0);
				}
				break;
			case PLUS:
			case MINUS:
			case TILDE:
				{
				_localctx = new ArithmeticUnaryContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1805);
				((ArithmeticUnaryContext)_localctx).operator = _input.LT(1);
				_la = _input.LA(1);
				if ( !(((((_la - 121)) & ~0x3f) == 0 && ((1L << (_la - 121)) & ((1L << (PLUS - 121)) | (1L << (MINUS - 121)) | (1L << (TILDE - 121)))) != 0)) ) {
					((ArithmeticUnaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(1806);
				valueExpression(7);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			_ctx.stop = _input.LT(-1);
			setState(1830);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,250,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(1828);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,249,_ctx) ) {
					case 1:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(1809);
						if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
						setState(1810);
						((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
						_la = _input.LA(1);
						if ( !(((((_la - 123)) & ~0x3f) == 0 && ((1L << (_la - 123)) & ((1L << (ASTERISK - 123)) | (1L << (SLASH - 123)) | (1L << (PERCENT - 123)) | (1L << (DIV - 123)))) != 0)) ) {
							((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(1811);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(7);
						}
						break;
					case 2:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(1812);
						if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
						setState(1813);
						((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
						_la = _input.LA(1);
						if ( !(_la==PLUS || _la==MINUS) ) {
							((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(1814);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(6);
						}
						break;
					case 3:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(1815);
						if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
						setState(1816);
						((ArithmeticBinaryContext)_localctx).operator = match(AMPERSAND);
						setState(1817);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(5);
						}
						break;
					case 4:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(1818);
						if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
						setState(1819);
						((ArithmeticBinaryContext)_localctx).operator = match(HAT);
						setState(1820);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(4);
						}
						break;
					case 5:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(1821);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(1822);
						((ArithmeticBinaryContext)_localctx).operator = match(PIPE);
						setState(1823);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(3);
						}
						break;
					case 6:
						{
						_localctx = new ComparisonContext(new ValueExpressionContext(_parentctx, _parentState));
						((ComparisonContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(1824);
						if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
						setState(1825);
						comparisonOperator();
						setState(1826);
						((ComparisonContext)_localctx).right = valueExpression(2);
						}
						break;
					}
					} 
				}
				setState(1832);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,250,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class PrimaryExpressionContext extends ParserRuleContext {
		public PrimaryExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_primaryExpression; }
	 
		public PrimaryExpressionContext() { }
		public void copyFrom(PrimaryExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class DereferenceContext extends PrimaryExpressionContext {
		public PrimaryExpressionContext base;
		public IdentifierContext fieldName;
		public PrimaryExpressionContext primaryExpression() {
			return getRuleContext(PrimaryExpressionContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DereferenceContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDereference(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDereference(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDereference(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SimpleCaseContext extends PrimaryExpressionContext {
		public ExpressionContext elseExpression;
		public TerminalNode CASE() { return getToken(SqlBaseParser.CASE, 0); }
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public TerminalNode END() { return getToken(SqlBaseParser.END, 0); }
		public List<WhenClauseContext> whenClause() {
			return getRuleContexts(WhenClauseContext.class);
		}
		public WhenClauseContext whenClause(int i) {
			return getRuleContext(WhenClauseContext.class,i);
		}
		public TerminalNode ELSE() { return getToken(SqlBaseParser.ELSE, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public SimpleCaseContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSimpleCase(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSimpleCase(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSimpleCase(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class CastContext extends PrimaryExpressionContext {
		public TerminalNode CAST() { return getToken(SqlBaseParser.CAST, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public DataTypeContext dataType() {
			return getRuleContext(DataTypeContext.class,0);
		}
		public CastContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCast(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCast(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCast(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ConstantDefaultContext extends PrimaryExpressionContext {
		public ConstantContext constant() {
			return getRuleContext(ConstantContext.class,0);
		}
		public ConstantDefaultContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterConstantDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitConstantDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitConstantDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ColumnReferenceContext extends PrimaryExpressionContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ColumnReferenceContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterColumnReference(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitColumnReference(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitColumnReference(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class RowConstructorContext extends PrimaryExpressionContext {
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public RowConstructorContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRowConstructor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRowConstructor(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRowConstructor(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ParenthesizedExpressionContext extends PrimaryExpressionContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public ParenthesizedExpressionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterParenthesizedExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitParenthesizedExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitParenthesizedExpression(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class StarContext extends PrimaryExpressionContext {
		public TerminalNode ASTERISK() { return getToken(SqlBaseParser.ASTERISK, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public StarContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterStar(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitStar(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitStar(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SubscriptContext extends PrimaryExpressionContext {
		public PrimaryExpressionContext value;
		public ValueExpressionContext index;
		public PrimaryExpressionContext primaryExpression() {
			return getRuleContext(PrimaryExpressionContext.class,0);
		}
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public SubscriptContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSubscript(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSubscript(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSubscript(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class FunctionCallContext extends PrimaryExpressionContext {
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode OVER() { return getToken(SqlBaseParser.OVER, 0); }
		public WindowSpecContext windowSpec() {
			return getRuleContext(WindowSpecContext.class,0);
		}
		public SetQuantifierContext setQuantifier() {
			return getRuleContext(SetQuantifierContext.class,0);
		}
		public FunctionCallContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterFunctionCall(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitFunctionCall(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitFunctionCall(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SearchedCaseContext extends PrimaryExpressionContext {
		public ExpressionContext elseExpression;
		public TerminalNode CASE() { return getToken(SqlBaseParser.CASE, 0); }
		public TerminalNode END() { return getToken(SqlBaseParser.END, 0); }
		public List<WhenClauseContext> whenClause() {
			return getRuleContexts(WhenClauseContext.class);
		}
		public WhenClauseContext whenClause(int i) {
			return getRuleContext(WhenClauseContext.class,i);
		}
		public TerminalNode ELSE() { return getToken(SqlBaseParser.ELSE, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public SearchedCaseContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSearchedCase(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSearchedCase(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSearchedCase(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SubqueryExpressionContext extends PrimaryExpressionContext {
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public SubqueryExpressionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSubqueryExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSubqueryExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSubqueryExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PrimaryExpressionContext primaryExpression() throws RecognitionException {
		return primaryExpression(0);
	}

	private PrimaryExpressionContext primaryExpression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		PrimaryExpressionContext _localctx = new PrimaryExpressionContext(_ctx, _parentState);
		PrimaryExpressionContext _prevctx = _localctx;
		int _startState = 128;
		enterRecursionRule(_localctx, 128, RULE_primaryExpression, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1911);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,260,_ctx) ) {
			case 1:
				{
				_localctx = new ConstantDefaultContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(1834);
				constant();
				}
				break;
			case 2:
				{
				_localctx = new StarContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1835);
				match(ASTERISK);
				}
				break;
			case 3:
				{
				_localctx = new StarContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1836);
				qualifiedName();
				setState(1837);
				match(T__3);
				setState(1838);
				match(ASTERISK);
				}
				break;
			case 4:
				{
				_localctx = new RowConstructorContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1840);
				match(T__0);
				setState(1841);
				expression();
				setState(1844); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(1842);
					match(T__2);
					setState(1843);
					expression();
					}
					}
					setState(1846); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==T__2 );
				setState(1848);
				match(T__1);
				}
				break;
			case 5:
				{
				_localctx = new FunctionCallContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1850);
				qualifiedName();
				setState(1851);
				match(T__0);
				setState(1863);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << SELECT) | (1L << FROM) | (1L << ADD) | (1L << AS) | (1L << ALL) | (1L << DISTINCT) | (1L << WHERE) | (1L << GROUP) | (1L << BY) | (1L << GROUPING) | (1L << SETS) | (1L << CUBE) | (1L << ROLLUP) | (1L << ORDER) | (1L << HAVING) | (1L << LIMIT) | (1L << AT) | (1L << OR) | (1L << AND) | (1L << IN) | (1L << NOT) | (1L << NO) | (1L << EXISTS) | (1L << BETWEEN) | (1L << LIKE) | (1L << RLIKE) | (1L << IS) | (1L << NULL) | (1L << TRUE) | (1L << FALSE) | (1L << NULLS) | (1L << ASC) | (1L << DESC) | (1L << FOR) | (1L << INTERVAL) | (1L << CASE) | (1L << WHEN) | (1L << THEN) | (1L << ELSE) | (1L << END) | (1L << JOIN) | (1L << CROSS) | (1L << OUTER) | (1L << INNER) | (1L << LEFT) | (1L << SEMI) | (1L << RIGHT) | (1L << FULL) | (1L << NATURAL) | (1L << ON) | (1L << LATERAL) | (1L << WINDOW) | (1L << OVER) | (1L << PARTITION) | (1L << RANGE) | (1L << ROWS))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (UNBOUNDED - 64)) | (1L << (PRECEDING - 64)) | (1L << (FOLLOWING - 64)) | (1L << (CURRENT - 64)) | (1L << (ROW - 64)) | (1L << (WITH - 64)) | (1L << (VALUES - 64)) | (1L << (CREATE - 64)) | (1L << (TABLE - 64)) | (1L << (VIEW - 64)) | (1L << (REPLACE - 64)) | (1L << (INSERT - 64)) | (1L << (DELETE - 64)) | (1L << (INTO - 64)) | (1L << (DESCRIBE - 64)) | (1L << (EXPLAIN - 64)) | (1L << (FORMAT - 64)) | (1L << (LOGICAL - 64)) | (1L << (CODEGEN - 64)) | (1L << (CAST - 64)) | (1L << (SHOW - 64)) | (1L << (TABLES - 64)) | (1L << (COLUMNS - 64)) | (1L << (COLUMN - 64)) | (1L << (USE - 64)) | (1L << (PARTITIONS - 64)) | (1L << (FUNCTIONS - 64)) | (1L << (DROP - 64)) | (1L << (UNION - 64)) | (1L << (EXCEPT - 64)) | (1L << (INTERSECT - 64)) | (1L << (TO - 64)) | (1L << (TABLESAMPLE - 64)) | (1L << (STRATIFY - 64)) | (1L << (ALTER - 64)) | (1L << (RENAME - 64)) | (1L << (ARRAY - 64)) | (1L << (MAP - 64)) | (1L << (STRUCT - 64)) | (1L << (COMMENT - 64)) | (1L << (SET - 64)) | (1L << (RESET - 64)) | (1L << (DATA - 64)) | (1L << (START - 64)) | (1L << (TRANSACTION - 64)) | (1L << (COMMIT - 64)) | (1L << (ROLLBACK - 64)) | (1L << (MACRO - 64)) | (1L << (IF - 64)) | (1L << (PLUS - 64)) | (1L << (MINUS - 64)) | (1L << (ASTERISK - 64)) | (1L << (DIV - 64)) | (1L << (TILDE - 64)))) != 0) || ((((_la - 131)) & ~0x3f) == 0 && ((1L << (_la - 131)) & ((1L << (PERCENTLIT - 131)) | (1L << (BUCKET - 131)) | (1L << (OUT - 131)) | (1L << (OF - 131)) | (1L << (SORT - 131)) | (1L << (CLUSTER - 131)) | (1L << (DISTRIBUTE - 131)) | (1L << (OVERWRITE - 131)) | (1L << (TRANSFORM - 131)) | (1L << (REDUCE - 131)) | (1L << (USING - 131)) | (1L << (SERDE - 131)) | (1L << (SERDEPROPERTIES - 131)) | (1L << (RECORDREADER - 131)) | (1L << (RECORDWRITER - 131)) | (1L << (DELIMITED - 131)) | (1L << (FIELDS - 131)) | (1L << (TERMINATED - 131)) | (1L << (COLLECTION - 131)) | (1L << (ITEMS - 131)) | (1L << (KEYS - 131)) | (1L << (ESCAPED - 131)) | (1L << (LINES - 131)) | (1L << (SEPARATED - 131)) | (1L << (FUNCTION - 131)) | (1L << (EXTENDED - 131)) | (1L << (REFRESH - 131)) | (1L << (CLEAR - 131)) | (1L << (CACHE - 131)) | (1L << (UNCACHE - 131)) | (1L << (LAZY - 131)) | (1L << (FORMATTED - 131)) | (1L << (TEMPORARY - 131)) | (1L << (OPTIONS - 131)) | (1L << (UNSET - 131)) | (1L << (TBLPROPERTIES - 131)) | (1L << (DBPROPERTIES - 131)) | (1L << (BUCKETS - 131)) | (1L << (SKEWED - 131)) | (1L << (STORED - 131)) | (1L << (DIRECTORIES - 131)) | (1L << (LOCATION - 131)) | (1L << (EXCHANGE - 131)) | (1L << (ARCHIVE - 131)) | (1L << (UNARCHIVE - 131)) | (1L << (FILEFORMAT - 131)) | (1L << (TOUCH - 131)) | (1L << (COMPACT - 131)) | (1L << (CONCATENATE - 131)) | (1L << (CHANGE - 131)) | (1L << (CASCADE - 131)) | (1L << (RESTRICT - 131)) | (1L << (CLUSTERED - 131)) | (1L << (SORTED - 131)) | (1L << (PURGE - 131)) | (1L << (INPUTFORMAT - 131)) | (1L << (OUTPUTFORMAT - 131)) | (1L << (DATABASE - 131)) | (1L << (DATABASES - 131)) | (1L << (DFS - 131)) | (1L << (TRUNCATE - 131)) | (1L << (ANALYZE - 131)) | (1L << (COMPUTE - 131)) | (1L << (LIST - 131)))) != 0) || ((((_la - 195)) & ~0x3f) == 0 && ((1L << (_la - 195)) & ((1L << (STATISTICS - 195)) | (1L << (PARTITIONED - 195)) | (1L << (EXTERNAL - 195)) | (1L << (DEFINED - 195)) | (1L << (REVOKE - 195)) | (1L << (GRANT - 195)) | (1L << (LOCK - 195)) | (1L << (UNLOCK - 195)) | (1L << (MSCK - 195)) | (1L << (REPAIR - 195)) | (1L << (EXPORT - 195)) | (1L << (IMPORT - 195)) | (1L << (LOAD - 195)) | (1L << (ROLE - 195)) | (1L << (ROLES - 195)) | (1L << (COMPACTIONS - 195)) | (1L << (PRINCIPALS - 195)) | (1L << (TRANSACTIONS - 195)) | (1L << (INDEX - 195)) | (1L << (INDEXES - 195)) | (1L << (LOCKS - 195)) | (1L << (OPTION - 195)) | (1L << (ANTI - 195)) | (1L << (LOCAL - 195)) | (1L << (INPATH - 195)) | (1L << (STRING - 195)) | (1L << (BIGINT_LITERAL - 195)) | (1L << (SMALLINT_LITERAL - 195)) | (1L << (TINYINT_LITERAL - 195)) | (1L << (INTEGER_VALUE - 195)) | (1L << (DECIMAL_VALUE - 195)) | (1L << (SCIENTIFIC_DECIMAL_VALUE - 195)) | (1L << (DOUBLE_LITERAL - 195)) | (1L << (IDENTIFIER - 195)) | (1L << (BACKQUOTED_IDENTIFIER - 195)))) != 0)) {
					{
					setState(1853);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,252,_ctx) ) {
					case 1:
						{
						setState(1852);
						setQuantifier();
						}
						break;
					}
					setState(1855);
					expression();
					setState(1860);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__2) {
						{
						{
						setState(1856);
						match(T__2);
						setState(1857);
						expression();
						}
						}
						setState(1862);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(1865);
				match(T__1);
				setState(1868);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,255,_ctx) ) {
				case 1:
					{
					setState(1866);
					match(OVER);
					setState(1867);
					windowSpec();
					}
					break;
				}
				}
				break;
			case 6:
				{
				_localctx = new SubqueryExpressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1870);
				match(T__0);
				setState(1871);
				query();
				setState(1872);
				match(T__1);
				}
				break;
			case 7:
				{
				_localctx = new SimpleCaseContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1874);
				match(CASE);
				setState(1875);
				valueExpression(0);
				setState(1877); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(1876);
					whenClause();
					}
					}
					setState(1879); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==WHEN );
				setState(1883);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ELSE) {
					{
					setState(1881);
					match(ELSE);
					setState(1882);
					((SimpleCaseContext)_localctx).elseExpression = expression();
					}
				}

				setState(1885);
				match(END);
				}
				break;
			case 8:
				{
				_localctx = new SearchedCaseContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1887);
				match(CASE);
				setState(1889); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(1888);
					whenClause();
					}
					}
					setState(1891); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==WHEN );
				setState(1895);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ELSE) {
					{
					setState(1893);
					match(ELSE);
					setState(1894);
					((SearchedCaseContext)_localctx).elseExpression = expression();
					}
				}

				setState(1897);
				match(END);
				}
				break;
			case 9:
				{
				_localctx = new CastContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1899);
				match(CAST);
				setState(1900);
				match(T__0);
				setState(1901);
				expression();
				setState(1902);
				match(AS);
				setState(1903);
				dataType();
				setState(1904);
				match(T__1);
				}
				break;
			case 10:
				{
				_localctx = new ColumnReferenceContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1906);
				identifier();
				}
				break;
			case 11:
				{
				_localctx = new ParenthesizedExpressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1907);
				match(T__0);
				setState(1908);
				expression();
				setState(1909);
				match(T__1);
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(1923);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,262,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(1921);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,261,_ctx) ) {
					case 1:
						{
						_localctx = new SubscriptContext(new PrimaryExpressionContext(_parentctx, _parentState));
						((SubscriptContext)_localctx).value = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_primaryExpression);
						setState(1913);
						if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
						setState(1914);
						match(T__4);
						setState(1915);
						((SubscriptContext)_localctx).index = valueExpression(0);
						setState(1916);
						match(T__5);
						}
						break;
					case 2:
						{
						_localctx = new DereferenceContext(new PrimaryExpressionContext(_parentctx, _parentState));
						((DereferenceContext)_localctx).base = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_primaryExpression);
						setState(1918);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(1919);
						match(T__3);
						setState(1920);
						((DereferenceContext)_localctx).fieldName = identifier();
						}
						break;
					}
					} 
				}
				setState(1925);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,262,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class ConstantContext extends ParserRuleContext {
		public ConstantContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_constant; }
	 
		public ConstantContext() { }
		public void copyFrom(ConstantContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class NullLiteralContext extends ConstantContext {
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public NullLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNullLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNullLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNullLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class StringLiteralContext extends ConstantContext {
		public List<TerminalNode> STRING() { return getTokens(SqlBaseParser.STRING); }
		public TerminalNode STRING(int i) {
			return getToken(SqlBaseParser.STRING, i);
		}
		public StringLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterStringLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitStringLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitStringLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class TypeConstructorContext extends ConstantContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TypeConstructorContext(ConstantContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTypeConstructor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTypeConstructor(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTypeConstructor(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class IntervalLiteralContext extends ConstantContext {
		public IntervalContext interval() {
			return getRuleContext(IntervalContext.class,0);
		}
		public IntervalLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIntervalLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIntervalLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIntervalLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class NumericLiteralContext extends ConstantContext {
		public NumberContext number() {
			return getRuleContext(NumberContext.class,0);
		}
		public NumericLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNumericLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNumericLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNumericLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class BooleanLiteralContext extends ConstantContext {
		public BooleanValueContext booleanValue() {
			return getRuleContext(BooleanValueContext.class,0);
		}
		public BooleanLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBooleanLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBooleanLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBooleanLiteral(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ConstantContext constant() throws RecognitionException {
		ConstantContext _localctx = new ConstantContext(_ctx, getState());
		enterRule(_localctx, 130, RULE_constant);
		try {
			int _alt;
			setState(1938);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,264,_ctx) ) {
			case 1:
				_localctx = new NullLiteralContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1926);
				match(NULL);
				}
				break;
			case 2:
				_localctx = new IntervalLiteralContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1927);
				interval();
				}
				break;
			case 3:
				_localctx = new TypeConstructorContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1928);
				identifier();
				setState(1929);
				match(STRING);
				}
				break;
			case 4:
				_localctx = new NumericLiteralContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(1931);
				number();
				}
				break;
			case 5:
				_localctx = new BooleanLiteralContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(1932);
				booleanValue();
				}
				break;
			case 6:
				_localctx = new StringLiteralContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(1934); 
				_errHandler.sync(this);
				_alt = 1;
				do {
					switch (_alt) {
					case 1:
						{
						{
						setState(1933);
						match(STRING);
						}
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					setState(1936); 
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,263,_ctx);
				} while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ComparisonOperatorContext extends ParserRuleContext {
		public TerminalNode EQ() { return getToken(SqlBaseParser.EQ, 0); }
		public TerminalNode NEQ() { return getToken(SqlBaseParser.NEQ, 0); }
		public TerminalNode NEQJ() { return getToken(SqlBaseParser.NEQJ, 0); }
		public TerminalNode LT() { return getToken(SqlBaseParser.LT, 0); }
		public TerminalNode LTE() { return getToken(SqlBaseParser.LTE, 0); }
		public TerminalNode GT() { return getToken(SqlBaseParser.GT, 0); }
		public TerminalNode GTE() { return getToken(SqlBaseParser.GTE, 0); }
		public TerminalNode NSEQ() { return getToken(SqlBaseParser.NSEQ, 0); }
		public ComparisonOperatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_comparisonOperator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterComparisonOperator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitComparisonOperator(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitComparisonOperator(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ComparisonOperatorContext comparisonOperator() throws RecognitionException {
		ComparisonOperatorContext _localctx = new ComparisonOperatorContext(_ctx, getState());
		enterRule(_localctx, 132, RULE_comparisonOperator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1940);
			_la = _input.LA(1);
			if ( !(((((_la - 113)) & ~0x3f) == 0 && ((1L << (_la - 113)) & ((1L << (EQ - 113)) | (1L << (NSEQ - 113)) | (1L << (NEQ - 113)) | (1L << (NEQJ - 113)) | (1L << (LT - 113)) | (1L << (LTE - 113)) | (1L << (GT - 113)) | (1L << (GTE - 113)))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ArithmeticOperatorContext extends ParserRuleContext {
		public TerminalNode PLUS() { return getToken(SqlBaseParser.PLUS, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public TerminalNode ASTERISK() { return getToken(SqlBaseParser.ASTERISK, 0); }
		public TerminalNode SLASH() { return getToken(SqlBaseParser.SLASH, 0); }
		public TerminalNode PERCENT() { return getToken(SqlBaseParser.PERCENT, 0); }
		public TerminalNode DIV() { return getToken(SqlBaseParser.DIV, 0); }
		public TerminalNode TILDE() { return getToken(SqlBaseParser.TILDE, 0); }
		public TerminalNode AMPERSAND() { return getToken(SqlBaseParser.AMPERSAND, 0); }
		public TerminalNode PIPE() { return getToken(SqlBaseParser.PIPE, 0); }
		public TerminalNode HAT() { return getToken(SqlBaseParser.HAT, 0); }
		public ArithmeticOperatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_arithmeticOperator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterArithmeticOperator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitArithmeticOperator(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitArithmeticOperator(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ArithmeticOperatorContext arithmeticOperator() throws RecognitionException {
		ArithmeticOperatorContext _localctx = new ArithmeticOperatorContext(_ctx, getState());
		enterRule(_localctx, 134, RULE_arithmeticOperator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1942);
			_la = _input.LA(1);
			if ( !(((((_la - 121)) & ~0x3f) == 0 && ((1L << (_la - 121)) & ((1L << (PLUS - 121)) | (1L << (MINUS - 121)) | (1L << (ASTERISK - 121)) | (1L << (SLASH - 121)) | (1L << (PERCENT - 121)) | (1L << (DIV - 121)) | (1L << (TILDE - 121)) | (1L << (AMPERSAND - 121)) | (1L << (PIPE - 121)) | (1L << (HAT - 121)))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PredicateOperatorContext extends ParserRuleContext {
		public TerminalNode OR() { return getToken(SqlBaseParser.OR, 0); }
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public PredicateOperatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_predicateOperator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPredicateOperator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPredicateOperator(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPredicateOperator(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PredicateOperatorContext predicateOperator() throws RecognitionException {
		PredicateOperatorContext _localctx = new PredicateOperatorContext(_ctx, getState());
		enterRule(_localctx, 136, RULE_predicateOperator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1944);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << OR) | (1L << AND) | (1L << IN) | (1L << NOT))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class BooleanValueContext extends ParserRuleContext {
		public TerminalNode TRUE() { return getToken(SqlBaseParser.TRUE, 0); }
		public TerminalNode FALSE() { return getToken(SqlBaseParser.FALSE, 0); }
		public BooleanValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_booleanValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBooleanValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBooleanValue(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBooleanValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final BooleanValueContext booleanValue() throws RecognitionException {
		BooleanValueContext _localctx = new BooleanValueContext(_ctx, getState());
		enterRule(_localctx, 138, RULE_booleanValue);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1946);
			_la = _input.LA(1);
			if ( !(_la==TRUE || _la==FALSE) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IntervalContext extends ParserRuleContext {
		public TerminalNode INTERVAL() { return getToken(SqlBaseParser.INTERVAL, 0); }
		public List<IntervalFieldContext> intervalField() {
			return getRuleContexts(IntervalFieldContext.class);
		}
		public IntervalFieldContext intervalField(int i) {
			return getRuleContext(IntervalFieldContext.class,i);
		}
		public IntervalContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_interval; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterInterval(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitInterval(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitInterval(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IntervalContext interval() throws RecognitionException {
		IntervalContext _localctx = new IntervalContext(_ctx, getState());
		enterRule(_localctx, 140, RULE_interval);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1948);
			match(INTERVAL);
			setState(1952);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,265,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1949);
					intervalField();
					}
					} 
				}
				setState(1954);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,265,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IntervalFieldContext extends ParserRuleContext {
		public IntervalValueContext value;
		public IdentifierContext unit;
		public IdentifierContext to;
		public IntervalValueContext intervalValue() {
			return getRuleContext(IntervalValueContext.class,0);
		}
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public IntervalFieldContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_intervalField; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIntervalField(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIntervalField(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIntervalField(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IntervalFieldContext intervalField() throws RecognitionException {
		IntervalFieldContext _localctx = new IntervalFieldContext(_ctx, getState());
		enterRule(_localctx, 142, RULE_intervalField);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1955);
			((IntervalFieldContext)_localctx).value = intervalValue();
			setState(1956);
			((IntervalFieldContext)_localctx).unit = identifier();
			setState(1959);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,266,_ctx) ) {
			case 1:
				{
				setState(1957);
				match(TO);
				setState(1958);
				((IntervalFieldContext)_localctx).to = identifier();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IntervalValueContext extends ParserRuleContext {
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public TerminalNode DECIMAL_VALUE() { return getToken(SqlBaseParser.DECIMAL_VALUE, 0); }
		public TerminalNode PLUS() { return getToken(SqlBaseParser.PLUS, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public IntervalValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_intervalValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIntervalValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIntervalValue(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIntervalValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IntervalValueContext intervalValue() throws RecognitionException {
		IntervalValueContext _localctx = new IntervalValueContext(_ctx, getState());
		enterRule(_localctx, 144, RULE_intervalValue);
		int _la;
		try {
			setState(1966);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case PLUS:
			case MINUS:
			case INTEGER_VALUE:
			case DECIMAL_VALUE:
				enterOuterAlt(_localctx, 1);
				{
				setState(1962);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PLUS || _la==MINUS) {
					{
					setState(1961);
					_la = _input.LA(1);
					if ( !(_la==PLUS || _la==MINUS) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

				setState(1964);
				_la = _input.LA(1);
				if ( !(_la==INTEGER_VALUE || _la==DECIMAL_VALUE) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			case STRING:
				enterOuterAlt(_localctx, 2);
				{
				setState(1965);
				match(STRING);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DataTypeContext extends ParserRuleContext {
		public DataTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dataType; }
	 
		public DataTypeContext() { }
		public void copyFrom(DataTypeContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class ComplexDataTypeContext extends DataTypeContext {
		public Token complex;
		public List<DataTypeContext> dataType() {
			return getRuleContexts(DataTypeContext.class);
		}
		public DataTypeContext dataType(int i) {
			return getRuleContext(DataTypeContext.class,i);
		}
		public TerminalNode ARRAY() { return getToken(SqlBaseParser.ARRAY, 0); }
		public TerminalNode MAP() { return getToken(SqlBaseParser.MAP, 0); }
		public TerminalNode STRUCT() { return getToken(SqlBaseParser.STRUCT, 0); }
		public TerminalNode NEQ() { return getToken(SqlBaseParser.NEQ, 0); }
		public ColTypeListContext colTypeList() {
			return getRuleContext(ColTypeListContext.class,0);
		}
		public ComplexDataTypeContext(DataTypeContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterComplexDataType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitComplexDataType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitComplexDataType(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class PrimitiveDataTypeContext extends DataTypeContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public List<TerminalNode> INTEGER_VALUE() { return getTokens(SqlBaseParser.INTEGER_VALUE); }
		public TerminalNode INTEGER_VALUE(int i) {
			return getToken(SqlBaseParser.INTEGER_VALUE, i);
		}
		public PrimitiveDataTypeContext(DataTypeContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPrimitiveDataType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPrimitiveDataType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPrimitiveDataType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DataTypeContext dataType() throws RecognitionException {
		DataTypeContext _localctx = new DataTypeContext(_ctx, getState());
		enterRule(_localctx, 146, RULE_dataType);
		int _la;
		try {
			setState(2002);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,273,_ctx) ) {
			case 1:
				_localctx = new ComplexDataTypeContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1968);
				((ComplexDataTypeContext)_localctx).complex = match(ARRAY);
				setState(1969);
				match(LT);
				setState(1970);
				dataType();
				setState(1971);
				match(GT);
				}
				break;
			case 2:
				_localctx = new ComplexDataTypeContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1973);
				((ComplexDataTypeContext)_localctx).complex = match(MAP);
				setState(1974);
				match(LT);
				setState(1975);
				dataType();
				setState(1976);
				match(T__2);
				setState(1977);
				dataType();
				setState(1978);
				match(GT);
				}
				break;
			case 3:
				_localctx = new ComplexDataTypeContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1980);
				((ComplexDataTypeContext)_localctx).complex = match(STRUCT);
				setState(1987);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case LT:
					{
					setState(1981);
					match(LT);
					setState(1983);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << SELECT) | (1L << FROM) | (1L << ADD) | (1L << AS) | (1L << ALL) | (1L << DISTINCT) | (1L << WHERE) | (1L << GROUP) | (1L << BY) | (1L << GROUPING) | (1L << SETS) | (1L << CUBE) | (1L << ROLLUP) | (1L << ORDER) | (1L << HAVING) | (1L << LIMIT) | (1L << AT) | (1L << OR) | (1L << AND) | (1L << IN) | (1L << NOT) | (1L << NO) | (1L << EXISTS) | (1L << BETWEEN) | (1L << LIKE) | (1L << RLIKE) | (1L << IS) | (1L << NULL) | (1L << TRUE) | (1L << FALSE) | (1L << NULLS) | (1L << ASC) | (1L << DESC) | (1L << FOR) | (1L << INTERVAL) | (1L << CASE) | (1L << WHEN) | (1L << THEN) | (1L << ELSE) | (1L << END) | (1L << JOIN) | (1L << CROSS) | (1L << OUTER) | (1L << INNER) | (1L << LEFT) | (1L << SEMI) | (1L << RIGHT) | (1L << FULL) | (1L << NATURAL) | (1L << ON) | (1L << LATERAL) | (1L << WINDOW) | (1L << OVER) | (1L << PARTITION) | (1L << RANGE) | (1L << ROWS))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (UNBOUNDED - 64)) | (1L << (PRECEDING - 64)) | (1L << (FOLLOWING - 64)) | (1L << (CURRENT - 64)) | (1L << (ROW - 64)) | (1L << (WITH - 64)) | (1L << (VALUES - 64)) | (1L << (CREATE - 64)) | (1L << (TABLE - 64)) | (1L << (VIEW - 64)) | (1L << (REPLACE - 64)) | (1L << (INSERT - 64)) | (1L << (DELETE - 64)) | (1L << (INTO - 64)) | (1L << (DESCRIBE - 64)) | (1L << (EXPLAIN - 64)) | (1L << (FORMAT - 64)) | (1L << (LOGICAL - 64)) | (1L << (CODEGEN - 64)) | (1L << (CAST - 64)) | (1L << (SHOW - 64)) | (1L << (TABLES - 64)) | (1L << (COLUMNS - 64)) | (1L << (COLUMN - 64)) | (1L << (USE - 64)) | (1L << (PARTITIONS - 64)) | (1L << (FUNCTIONS - 64)) | (1L << (DROP - 64)) | (1L << (UNION - 64)) | (1L << (EXCEPT - 64)) | (1L << (INTERSECT - 64)) | (1L << (TO - 64)) | (1L << (TABLESAMPLE - 64)) | (1L << (STRATIFY - 64)) | (1L << (ALTER - 64)) | (1L << (RENAME - 64)) | (1L << (ARRAY - 64)) | (1L << (MAP - 64)) | (1L << (STRUCT - 64)) | (1L << (COMMENT - 64)) | (1L << (SET - 64)) | (1L << (RESET - 64)) | (1L << (DATA - 64)) | (1L << (START - 64)) | (1L << (TRANSACTION - 64)) | (1L << (COMMIT - 64)) | (1L << (ROLLBACK - 64)) | (1L << (MACRO - 64)) | (1L << (IF - 64)) | (1L << (DIV - 64)))) != 0) || ((((_la - 131)) & ~0x3f) == 0 && ((1L << (_la - 131)) & ((1L << (PERCENTLIT - 131)) | (1L << (BUCKET - 131)) | (1L << (OUT - 131)) | (1L << (OF - 131)) | (1L << (SORT - 131)) | (1L << (CLUSTER - 131)) | (1L << (DISTRIBUTE - 131)) | (1L << (OVERWRITE - 131)) | (1L << (TRANSFORM - 131)) | (1L << (REDUCE - 131)) | (1L << (USING - 131)) | (1L << (SERDE - 131)) | (1L << (SERDEPROPERTIES - 131)) | (1L << (RECORDREADER - 131)) | (1L << (RECORDWRITER - 131)) | (1L << (DELIMITED - 131)) | (1L << (FIELDS - 131)) | (1L << (TERMINATED - 131)) | (1L << (COLLECTION - 131)) | (1L << (ITEMS - 131)) | (1L << (KEYS - 131)) | (1L << (ESCAPED - 131)) | (1L << (LINES - 131)) | (1L << (SEPARATED - 131)) | (1L << (FUNCTION - 131)) | (1L << (EXTENDED - 131)) | (1L << (REFRESH - 131)) | (1L << (CLEAR - 131)) | (1L << (CACHE - 131)) | (1L << (UNCACHE - 131)) | (1L << (LAZY - 131)) | (1L << (FORMATTED - 131)) | (1L << (TEMPORARY - 131)) | (1L << (OPTIONS - 131)) | (1L << (UNSET - 131)) | (1L << (TBLPROPERTIES - 131)) | (1L << (DBPROPERTIES - 131)) | (1L << (BUCKETS - 131)) | (1L << (SKEWED - 131)) | (1L << (STORED - 131)) | (1L << (DIRECTORIES - 131)) | (1L << (LOCATION - 131)) | (1L << (EXCHANGE - 131)) | (1L << (ARCHIVE - 131)) | (1L << (UNARCHIVE - 131)) | (1L << (FILEFORMAT - 131)) | (1L << (TOUCH - 131)) | (1L << (COMPACT - 131)) | (1L << (CONCATENATE - 131)) | (1L << (CHANGE - 131)) | (1L << (CASCADE - 131)) | (1L << (RESTRICT - 131)) | (1L << (CLUSTERED - 131)) | (1L << (SORTED - 131)) | (1L << (PURGE - 131)) | (1L << (INPUTFORMAT - 131)) | (1L << (OUTPUTFORMAT - 131)) | (1L << (DATABASE - 131)) | (1L << (DATABASES - 131)) | (1L << (DFS - 131)) | (1L << (TRUNCATE - 131)) | (1L << (ANALYZE - 131)) | (1L << (COMPUTE - 131)) | (1L << (LIST - 131)))) != 0) || ((((_la - 195)) & ~0x3f) == 0 && ((1L << (_la - 195)) & ((1L << (STATISTICS - 195)) | (1L << (PARTITIONED - 195)) | (1L << (EXTERNAL - 195)) | (1L << (DEFINED - 195)) | (1L << (REVOKE - 195)) | (1L << (GRANT - 195)) | (1L << (LOCK - 195)) | (1L << (UNLOCK - 195)) | (1L << (MSCK - 195)) | (1L << (REPAIR - 195)) | (1L << (EXPORT - 195)) | (1L << (IMPORT - 195)) | (1L << (LOAD - 195)) | (1L << (ROLE - 195)) | (1L << (ROLES - 195)) | (1L << (COMPACTIONS - 195)) | (1L << (PRINCIPALS - 195)) | (1L << (TRANSACTIONS - 195)) | (1L << (INDEX - 195)) | (1L << (INDEXES - 195)) | (1L << (LOCKS - 195)) | (1L << (OPTION - 195)) | (1L << (ANTI - 195)) | (1L << (LOCAL - 195)) | (1L << (INPATH - 195)) | (1L << (IDENTIFIER - 195)) | (1L << (BACKQUOTED_IDENTIFIER - 195)))) != 0)) {
						{
						setState(1982);
						colTypeList();
						}
					}

					setState(1985);
					match(GT);
					}
					break;
				case NEQ:
					{
					setState(1986);
					match(NEQ);
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				break;
			case 4:
				_localctx = new PrimitiveDataTypeContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(1989);
				identifier();
				setState(2000);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,272,_ctx) ) {
				case 1:
					{
					setState(1990);
					match(T__0);
					setState(1991);
					match(INTEGER_VALUE);
					setState(1996);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__2) {
						{
						{
						setState(1992);
						match(T__2);
						setState(1993);
						match(INTEGER_VALUE);
						}
						}
						setState(1998);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(1999);
					match(T__1);
					}
					break;
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ColTypeListContext extends ParserRuleContext {
		public List<ColTypeContext> colType() {
			return getRuleContexts(ColTypeContext.class);
		}
		public ColTypeContext colType(int i) {
			return getRuleContext(ColTypeContext.class,i);
		}
		public ColTypeListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_colTypeList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterColTypeList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitColTypeList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitColTypeList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColTypeListContext colTypeList() throws RecognitionException {
		ColTypeListContext _localctx = new ColTypeListContext(_ctx, getState());
		enterRule(_localctx, 148, RULE_colTypeList);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2004);
			colType();
			setState(2009);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,274,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(2005);
					match(T__2);
					setState(2006);
					colType();
					}
					} 
				}
				setState(2011);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,274,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ColTypeContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DataTypeContext dataType() {
			return getRuleContext(DataTypeContext.class,0);
		}
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public ColTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_colType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterColType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitColType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitColType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColTypeContext colType() throws RecognitionException {
		ColTypeContext _localctx = new ColTypeContext(_ctx, getState());
		enterRule(_localctx, 150, RULE_colType);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2012);
			identifier();
			setState(2014);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__6) {
				{
				setState(2013);
				match(T__6);
				}
			}

			setState(2016);
			dataType();
			setState(2019);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,276,_ctx) ) {
			case 1:
				{
				setState(2017);
				match(COMMENT);
				setState(2018);
				match(STRING);
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class WhenClauseContext extends ParserRuleContext {
		public ExpressionContext condition;
		public ExpressionContext result;
		public TerminalNode WHEN() { return getToken(SqlBaseParser.WHEN, 0); }
		public TerminalNode THEN() { return getToken(SqlBaseParser.THEN, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public WhenClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_whenClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterWhenClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitWhenClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitWhenClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WhenClauseContext whenClause() throws RecognitionException {
		WhenClauseContext _localctx = new WhenClauseContext(_ctx, getState());
		enterRule(_localctx, 152, RULE_whenClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2021);
			match(WHEN);
			setState(2022);
			((WhenClauseContext)_localctx).condition = expression();
			setState(2023);
			match(THEN);
			setState(2024);
			((WhenClauseContext)_localctx).result = expression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class WindowsContext extends ParserRuleContext {
		public TerminalNode WINDOW() { return getToken(SqlBaseParser.WINDOW, 0); }
		public List<NamedWindowContext> namedWindow() {
			return getRuleContexts(NamedWindowContext.class);
		}
		public NamedWindowContext namedWindow(int i) {
			return getRuleContext(NamedWindowContext.class,i);
		}
		public WindowsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_windows; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterWindows(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitWindows(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitWindows(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WindowsContext windows() throws RecognitionException {
		WindowsContext _localctx = new WindowsContext(_ctx, getState());
		enterRule(_localctx, 154, RULE_windows);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2026);
			match(WINDOW);
			setState(2027);
			namedWindow();
			setState(2032);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,277,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(2028);
					match(T__2);
					setState(2029);
					namedWindow();
					}
					} 
				}
				setState(2034);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,277,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NamedWindowContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public WindowSpecContext windowSpec() {
			return getRuleContext(WindowSpecContext.class,0);
		}
		public NamedWindowContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_namedWindow; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNamedWindow(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNamedWindow(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNamedWindow(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NamedWindowContext namedWindow() throws RecognitionException {
		NamedWindowContext _localctx = new NamedWindowContext(_ctx, getState());
		enterRule(_localctx, 156, RULE_namedWindow);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2035);
			identifier();
			setState(2036);
			match(AS);
			setState(2037);
			windowSpec();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class WindowSpecContext extends ParserRuleContext {
		public WindowSpecContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_windowSpec; }
	 
		public WindowSpecContext() { }
		public void copyFrom(WindowSpecContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class WindowRefContext extends WindowSpecContext {
		public IdentifierContext name;
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public WindowRefContext(WindowSpecContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterWindowRef(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitWindowRef(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitWindowRef(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class WindowDefContext extends WindowSpecContext {
		public ExpressionContext expression;
		public List<ExpressionContext> partition = new ArrayList<ExpressionContext>();
		public TerminalNode CLUSTER() { return getToken(SqlBaseParser.CLUSTER, 0); }
		public List<TerminalNode> BY() { return getTokens(SqlBaseParser.BY); }
		public TerminalNode BY(int i) {
			return getToken(SqlBaseParser.BY, i);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public WindowFrameContext windowFrame() {
			return getRuleContext(WindowFrameContext.class,0);
		}
		public List<SortItemContext> sortItem() {
			return getRuleContexts(SortItemContext.class);
		}
		public SortItemContext sortItem(int i) {
			return getRuleContext(SortItemContext.class,i);
		}
		public TerminalNode PARTITION() { return getToken(SqlBaseParser.PARTITION, 0); }
		public TerminalNode DISTRIBUTE() { return getToken(SqlBaseParser.DISTRIBUTE, 0); }
		public TerminalNode ORDER() { return getToken(SqlBaseParser.ORDER, 0); }
		public TerminalNode SORT() { return getToken(SqlBaseParser.SORT, 0); }
		public WindowDefContext(WindowSpecContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterWindowDef(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitWindowDef(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitWindowDef(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WindowSpecContext windowSpec() throws RecognitionException {
		WindowSpecContext _localctx = new WindowSpecContext(_ctx, getState());
		enterRule(_localctx, 158, RULE_windowSpec);
		int _la;
		try {
			setState(2081);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case SELECT:
			case FROM:
			case ADD:
			case AS:
			case ALL:
			case DISTINCT:
			case WHERE:
			case GROUP:
			case BY:
			case GROUPING:
			case SETS:
			case CUBE:
			case ROLLUP:
			case ORDER:
			case HAVING:
			case LIMIT:
			case AT:
			case OR:
			case AND:
			case IN:
			case NOT:
			case NO:
			case EXISTS:
			case BETWEEN:
			case LIKE:
			case RLIKE:
			case IS:
			case NULL:
			case TRUE:
			case FALSE:
			case NULLS:
			case ASC:
			case DESC:
			case FOR:
			case INTERVAL:
			case CASE:
			case WHEN:
			case THEN:
			case ELSE:
			case END:
			case JOIN:
			case CROSS:
			case OUTER:
			case INNER:
			case LEFT:
			case SEMI:
			case RIGHT:
			case FULL:
			case NATURAL:
			case ON:
			case LATERAL:
			case WINDOW:
			case OVER:
			case PARTITION:
			case RANGE:
			case ROWS:
			case UNBOUNDED:
			case PRECEDING:
			case FOLLOWING:
			case CURRENT:
			case ROW:
			case WITH:
			case VALUES:
			case CREATE:
			case TABLE:
			case VIEW:
			case REPLACE:
			case INSERT:
			case DELETE:
			case INTO:
			case DESCRIBE:
			case EXPLAIN:
			case FORMAT:
			case LOGICAL:
			case CODEGEN:
			case CAST:
			case SHOW:
			case TABLES:
			case COLUMNS:
			case COLUMN:
			case USE:
			case PARTITIONS:
			case FUNCTIONS:
			case DROP:
			case UNION:
			case EXCEPT:
			case INTERSECT:
			case TO:
			case TABLESAMPLE:
			case STRATIFY:
			case ALTER:
			case RENAME:
			case ARRAY:
			case MAP:
			case STRUCT:
			case COMMENT:
			case SET:
			case RESET:
			case DATA:
			case START:
			case TRANSACTION:
			case COMMIT:
			case ROLLBACK:
			case MACRO:
			case IF:
			case DIV:
			case PERCENTLIT:
			case BUCKET:
			case OUT:
			case OF:
			case SORT:
			case CLUSTER:
			case DISTRIBUTE:
			case OVERWRITE:
			case TRANSFORM:
			case REDUCE:
			case USING:
			case SERDE:
			case SERDEPROPERTIES:
			case RECORDREADER:
			case RECORDWRITER:
			case DELIMITED:
			case FIELDS:
			case TERMINATED:
			case COLLECTION:
			case ITEMS:
			case KEYS:
			case ESCAPED:
			case LINES:
			case SEPARATED:
			case FUNCTION:
			case EXTENDED:
			case REFRESH:
			case CLEAR:
			case CACHE:
			case UNCACHE:
			case LAZY:
			case FORMATTED:
			case TEMPORARY:
			case OPTIONS:
			case UNSET:
			case TBLPROPERTIES:
			case DBPROPERTIES:
			case BUCKETS:
			case SKEWED:
			case STORED:
			case DIRECTORIES:
			case LOCATION:
			case EXCHANGE:
			case ARCHIVE:
			case UNARCHIVE:
			case FILEFORMAT:
			case TOUCH:
			case COMPACT:
			case CONCATENATE:
			case CHANGE:
			case CASCADE:
			case RESTRICT:
			case CLUSTERED:
			case SORTED:
			case PURGE:
			case INPUTFORMAT:
			case OUTPUTFORMAT:
			case DATABASE:
			case DATABASES:
			case DFS:
			case TRUNCATE:
			case ANALYZE:
			case COMPUTE:
			case LIST:
			case STATISTICS:
			case PARTITIONED:
			case EXTERNAL:
			case DEFINED:
			case REVOKE:
			case GRANT:
			case LOCK:
			case UNLOCK:
			case MSCK:
			case REPAIR:
			case EXPORT:
			case IMPORT:
			case LOAD:
			case ROLE:
			case ROLES:
			case COMPACTIONS:
			case PRINCIPALS:
			case TRANSACTIONS:
			case INDEX:
			case INDEXES:
			case LOCKS:
			case OPTION:
			case ANTI:
			case LOCAL:
			case INPATH:
			case IDENTIFIER:
			case BACKQUOTED_IDENTIFIER:
				_localctx = new WindowRefContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2039);
				((WindowRefContext)_localctx).name = identifier();
				}
				break;
			case T__0:
				_localctx = new WindowDefContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2040);
				match(T__0);
				setState(2075);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case CLUSTER:
					{
					setState(2041);
					match(CLUSTER);
					setState(2042);
					match(BY);
					setState(2043);
					((WindowDefContext)_localctx).expression = expression();
					((WindowDefContext)_localctx).partition.add(((WindowDefContext)_localctx).expression);
					setState(2048);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__2) {
						{
						{
						setState(2044);
						match(T__2);
						setState(2045);
						((WindowDefContext)_localctx).expression = expression();
						((WindowDefContext)_localctx).partition.add(((WindowDefContext)_localctx).expression);
						}
						}
						setState(2050);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
					break;
				case T__1:
				case ORDER:
				case PARTITION:
				case RANGE:
				case ROWS:
				case SORT:
				case DISTRIBUTE:
					{
					setState(2061);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==PARTITION || _la==DISTRIBUTE) {
						{
						setState(2051);
						_la = _input.LA(1);
						if ( !(_la==PARTITION || _la==DISTRIBUTE) ) {
						_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(2052);
						match(BY);
						setState(2053);
						((WindowDefContext)_localctx).expression = expression();
						((WindowDefContext)_localctx).partition.add(((WindowDefContext)_localctx).expression);
						setState(2058);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la==T__2) {
							{
							{
							setState(2054);
							match(T__2);
							setState(2055);
							((WindowDefContext)_localctx).expression = expression();
							((WindowDefContext)_localctx).partition.add(((WindowDefContext)_localctx).expression);
							}
							}
							setState(2060);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
						}
					}

					setState(2073);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==ORDER || _la==SORT) {
						{
						setState(2063);
						_la = _input.LA(1);
						if ( !(_la==ORDER || _la==SORT) ) {
						_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(2064);
						match(BY);
						setState(2065);
						sortItem();
						setState(2070);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la==T__2) {
							{
							{
							setState(2066);
							match(T__2);
							setState(2067);
							sortItem();
							}
							}
							setState(2072);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
						}
					}

					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(2078);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==RANGE || _la==ROWS) {
					{
					setState(2077);
					windowFrame();
					}
				}

				setState(2080);
				match(T__1);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class WindowFrameContext extends ParserRuleContext {
		public Token frameType;
		public FrameBoundContext start;
		public FrameBoundContext end;
		public TerminalNode RANGE() { return getToken(SqlBaseParser.RANGE, 0); }
		public List<FrameBoundContext> frameBound() {
			return getRuleContexts(FrameBoundContext.class);
		}
		public FrameBoundContext frameBound(int i) {
			return getRuleContext(FrameBoundContext.class,i);
		}
		public TerminalNode ROWS() { return getToken(SqlBaseParser.ROWS, 0); }
		public TerminalNode BETWEEN() { return getToken(SqlBaseParser.BETWEEN, 0); }
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public WindowFrameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_windowFrame; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterWindowFrame(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitWindowFrame(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitWindowFrame(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WindowFrameContext windowFrame() throws RecognitionException {
		WindowFrameContext _localctx = new WindowFrameContext(_ctx, getState());
		enterRule(_localctx, 160, RULE_windowFrame);
		try {
			setState(2099);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,286,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2083);
				((WindowFrameContext)_localctx).frameType = match(RANGE);
				setState(2084);
				((WindowFrameContext)_localctx).start = frameBound();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2085);
				((WindowFrameContext)_localctx).frameType = match(ROWS);
				setState(2086);
				((WindowFrameContext)_localctx).start = frameBound();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(2087);
				((WindowFrameContext)_localctx).frameType = match(RANGE);
				setState(2088);
				match(BETWEEN);
				setState(2089);
				((WindowFrameContext)_localctx).start = frameBound();
				setState(2090);
				match(AND);
				setState(2091);
				((WindowFrameContext)_localctx).end = frameBound();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(2093);
				((WindowFrameContext)_localctx).frameType = match(ROWS);
				setState(2094);
				match(BETWEEN);
				setState(2095);
				((WindowFrameContext)_localctx).start = frameBound();
				setState(2096);
				match(AND);
				setState(2097);
				((WindowFrameContext)_localctx).end = frameBound();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FrameBoundContext extends ParserRuleContext {
		public Token boundType;
		public TerminalNode UNBOUNDED() { return getToken(SqlBaseParser.UNBOUNDED, 0); }
		public TerminalNode PRECEDING() { return getToken(SqlBaseParser.PRECEDING, 0); }
		public TerminalNode FOLLOWING() { return getToken(SqlBaseParser.FOLLOWING, 0); }
		public TerminalNode ROW() { return getToken(SqlBaseParser.ROW, 0); }
		public TerminalNode CURRENT() { return getToken(SqlBaseParser.CURRENT, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public FrameBoundContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_frameBound; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterFrameBound(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitFrameBound(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitFrameBound(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FrameBoundContext frameBound() throws RecognitionException {
		FrameBoundContext _localctx = new FrameBoundContext(_ctx, getState());
		enterRule(_localctx, 162, RULE_frameBound);
		int _la;
		try {
			setState(2108);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,287,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2101);
				match(UNBOUNDED);
				setState(2102);
				((FrameBoundContext)_localctx).boundType = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==PRECEDING || _la==FOLLOWING) ) {
					((FrameBoundContext)_localctx).boundType = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2103);
				((FrameBoundContext)_localctx).boundType = match(CURRENT);
				setState(2104);
				match(ROW);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(2105);
				expression();
				setState(2106);
				((FrameBoundContext)_localctx).boundType = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==PRECEDING || _la==FOLLOWING) ) {
					((FrameBoundContext)_localctx).boundType = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QualifiedNameContext extends ParserRuleContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public QualifiedNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_qualifiedName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQualifiedName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQualifiedName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQualifiedName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QualifiedNameContext qualifiedName() throws RecognitionException {
		QualifiedNameContext _localctx = new QualifiedNameContext(_ctx, getState());
		enterRule(_localctx, 164, RULE_qualifiedName);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2110);
			identifier();
			setState(2115);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,288,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(2111);
					match(T__3);
					setState(2112);
					identifier();
					}
					} 
				}
				setState(2117);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,288,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IdentifierContext extends ParserRuleContext {
		public StrictIdentifierContext strictIdentifier() {
			return getRuleContext(StrictIdentifierContext.class,0);
		}
		public TerminalNode ANTI() { return getToken(SqlBaseParser.ANTI, 0); }
		public TerminalNode FULL() { return getToken(SqlBaseParser.FULL, 0); }
		public TerminalNode INNER() { return getToken(SqlBaseParser.INNER, 0); }
		public TerminalNode LEFT() { return getToken(SqlBaseParser.LEFT, 0); }
		public TerminalNode SEMI() { return getToken(SqlBaseParser.SEMI, 0); }
		public TerminalNode RIGHT() { return getToken(SqlBaseParser.RIGHT, 0); }
		public TerminalNode NATURAL() { return getToken(SqlBaseParser.NATURAL, 0); }
		public TerminalNode JOIN() { return getToken(SqlBaseParser.JOIN, 0); }
		public TerminalNode CROSS() { return getToken(SqlBaseParser.CROSS, 0); }
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public TerminalNode UNION() { return getToken(SqlBaseParser.UNION, 0); }
		public TerminalNode INTERSECT() { return getToken(SqlBaseParser.INTERSECT, 0); }
		public TerminalNode EXCEPT() { return getToken(SqlBaseParser.EXCEPT, 0); }
		public IdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IdentifierContext identifier() throws RecognitionException {
		IdentifierContext _localctx = new IdentifierContext(_ctx, getState());
		enterRule(_localctx, 166, RULE_identifier);
		try {
			setState(2132);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case SELECT:
			case FROM:
			case ADD:
			case AS:
			case ALL:
			case DISTINCT:
			case WHERE:
			case GROUP:
			case BY:
			case GROUPING:
			case SETS:
			case CUBE:
			case ROLLUP:
			case ORDER:
			case HAVING:
			case LIMIT:
			case AT:
			case OR:
			case AND:
			case IN:
			case NOT:
			case NO:
			case EXISTS:
			case BETWEEN:
			case LIKE:
			case RLIKE:
			case IS:
			case NULL:
			case TRUE:
			case FALSE:
			case NULLS:
			case ASC:
			case DESC:
			case FOR:
			case INTERVAL:
			case CASE:
			case WHEN:
			case THEN:
			case ELSE:
			case END:
			case OUTER:
			case LATERAL:
			case WINDOW:
			case OVER:
			case PARTITION:
			case RANGE:
			case ROWS:
			case UNBOUNDED:
			case PRECEDING:
			case FOLLOWING:
			case CURRENT:
			case ROW:
			case WITH:
			case VALUES:
			case CREATE:
			case TABLE:
			case VIEW:
			case REPLACE:
			case INSERT:
			case DELETE:
			case INTO:
			case DESCRIBE:
			case EXPLAIN:
			case FORMAT:
			case LOGICAL:
			case CODEGEN:
			case CAST:
			case SHOW:
			case TABLES:
			case COLUMNS:
			case COLUMN:
			case USE:
			case PARTITIONS:
			case FUNCTIONS:
			case DROP:
			case TO:
			case TABLESAMPLE:
			case STRATIFY:
			case ALTER:
			case RENAME:
			case ARRAY:
			case MAP:
			case STRUCT:
			case COMMENT:
			case SET:
			case RESET:
			case DATA:
			case START:
			case TRANSACTION:
			case COMMIT:
			case ROLLBACK:
			case MACRO:
			case IF:
			case DIV:
			case PERCENTLIT:
			case BUCKET:
			case OUT:
			case OF:
			case SORT:
			case CLUSTER:
			case DISTRIBUTE:
			case OVERWRITE:
			case TRANSFORM:
			case REDUCE:
			case USING:
			case SERDE:
			case SERDEPROPERTIES:
			case RECORDREADER:
			case RECORDWRITER:
			case DELIMITED:
			case FIELDS:
			case TERMINATED:
			case COLLECTION:
			case ITEMS:
			case KEYS:
			case ESCAPED:
			case LINES:
			case SEPARATED:
			case FUNCTION:
			case EXTENDED:
			case REFRESH:
			case CLEAR:
			case CACHE:
			case UNCACHE:
			case LAZY:
			case FORMATTED:
			case TEMPORARY:
			case OPTIONS:
			case UNSET:
			case TBLPROPERTIES:
			case DBPROPERTIES:
			case BUCKETS:
			case SKEWED:
			case STORED:
			case DIRECTORIES:
			case LOCATION:
			case EXCHANGE:
			case ARCHIVE:
			case UNARCHIVE:
			case FILEFORMAT:
			case TOUCH:
			case COMPACT:
			case CONCATENATE:
			case CHANGE:
			case CASCADE:
			case RESTRICT:
			case CLUSTERED:
			case SORTED:
			case PURGE:
			case INPUTFORMAT:
			case OUTPUTFORMAT:
			case DATABASE:
			case DATABASES:
			case DFS:
			case TRUNCATE:
			case ANALYZE:
			case COMPUTE:
			case LIST:
			case STATISTICS:
			case PARTITIONED:
			case EXTERNAL:
			case DEFINED:
			case REVOKE:
			case GRANT:
			case LOCK:
			case UNLOCK:
			case MSCK:
			case REPAIR:
			case EXPORT:
			case IMPORT:
			case LOAD:
			case ROLE:
			case ROLES:
			case COMPACTIONS:
			case PRINCIPALS:
			case TRANSACTIONS:
			case INDEX:
			case INDEXES:
			case LOCKS:
			case OPTION:
			case LOCAL:
			case INPATH:
			case IDENTIFIER:
			case BACKQUOTED_IDENTIFIER:
				enterOuterAlt(_localctx, 1);
				{
				setState(2118);
				strictIdentifier();
				}
				break;
			case ANTI:
				enterOuterAlt(_localctx, 2);
				{
				setState(2119);
				match(ANTI);
				}
				break;
			case FULL:
				enterOuterAlt(_localctx, 3);
				{
				setState(2120);
				match(FULL);
				}
				break;
			case INNER:
				enterOuterAlt(_localctx, 4);
				{
				setState(2121);
				match(INNER);
				}
				break;
			case LEFT:
				enterOuterAlt(_localctx, 5);
				{
				setState(2122);
				match(LEFT);
				}
				break;
			case SEMI:
				enterOuterAlt(_localctx, 6);
				{
				setState(2123);
				match(SEMI);
				}
				break;
			case RIGHT:
				enterOuterAlt(_localctx, 7);
				{
				setState(2124);
				match(RIGHT);
				}
				break;
			case NATURAL:
				enterOuterAlt(_localctx, 8);
				{
				setState(2125);
				match(NATURAL);
				}
				break;
			case JOIN:
				enterOuterAlt(_localctx, 9);
				{
				setState(2126);
				match(JOIN);
				}
				break;
			case CROSS:
				enterOuterAlt(_localctx, 10);
				{
				setState(2127);
				match(CROSS);
				}
				break;
			case ON:
				enterOuterAlt(_localctx, 11);
				{
				setState(2128);
				match(ON);
				}
				break;
			case UNION:
				enterOuterAlt(_localctx, 12);
				{
				setState(2129);
				match(UNION);
				}
				break;
			case INTERSECT:
				enterOuterAlt(_localctx, 13);
				{
				setState(2130);
				match(INTERSECT);
				}
				break;
			case EXCEPT:
				enterOuterAlt(_localctx, 14);
				{
				setState(2131);
				match(EXCEPT);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class StrictIdentifierContext extends ParserRuleContext {
		public StrictIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_strictIdentifier; }
	 
		public StrictIdentifierContext() { }
		public void copyFrom(StrictIdentifierContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class QuotedIdentifierAlternativeContext extends StrictIdentifierContext {
		public QuotedIdentifierContext quotedIdentifier() {
			return getRuleContext(QuotedIdentifierContext.class,0);
		}
		public QuotedIdentifierAlternativeContext(StrictIdentifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQuotedIdentifierAlternative(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQuotedIdentifierAlternative(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQuotedIdentifierAlternative(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class UnquotedIdentifierContext extends StrictIdentifierContext {
		public TerminalNode IDENTIFIER() { return getToken(SqlBaseParser.IDENTIFIER, 0); }
		public NonReservedContext nonReserved() {
			return getRuleContext(NonReservedContext.class,0);
		}
		public UnquotedIdentifierContext(StrictIdentifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUnquotedIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUnquotedIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUnquotedIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StrictIdentifierContext strictIdentifier() throws RecognitionException {
		StrictIdentifierContext _localctx = new StrictIdentifierContext(_ctx, getState());
		enterRule(_localctx, 168, RULE_strictIdentifier);
		try {
			setState(2137);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case IDENTIFIER:
				_localctx = new UnquotedIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2134);
				match(IDENTIFIER);
				}
				break;
			case BACKQUOTED_IDENTIFIER:
				_localctx = new QuotedIdentifierAlternativeContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2135);
				quotedIdentifier();
				}
				break;
			case SELECT:
			case FROM:
			case ADD:
			case AS:
			case ALL:
			case DISTINCT:
			case WHERE:
			case GROUP:
			case BY:
			case GROUPING:
			case SETS:
			case CUBE:
			case ROLLUP:
			case ORDER:
			case HAVING:
			case LIMIT:
			case AT:
			case OR:
			case AND:
			case IN:
			case NOT:
			case NO:
			case EXISTS:
			case BETWEEN:
			case LIKE:
			case RLIKE:
			case IS:
			case NULL:
			case TRUE:
			case FALSE:
			case NULLS:
			case ASC:
			case DESC:
			case FOR:
			case INTERVAL:
			case CASE:
			case WHEN:
			case THEN:
			case ELSE:
			case END:
			case OUTER:
			case LATERAL:
			case WINDOW:
			case OVER:
			case PARTITION:
			case RANGE:
			case ROWS:
			case UNBOUNDED:
			case PRECEDING:
			case FOLLOWING:
			case CURRENT:
			case ROW:
			case WITH:
			case VALUES:
			case CREATE:
			case TABLE:
			case VIEW:
			case REPLACE:
			case INSERT:
			case DELETE:
			case INTO:
			case DESCRIBE:
			case EXPLAIN:
			case FORMAT:
			case LOGICAL:
			case CODEGEN:
			case CAST:
			case SHOW:
			case TABLES:
			case COLUMNS:
			case COLUMN:
			case USE:
			case PARTITIONS:
			case FUNCTIONS:
			case DROP:
			case TO:
			case TABLESAMPLE:
			case STRATIFY:
			case ALTER:
			case RENAME:
			case ARRAY:
			case MAP:
			case STRUCT:
			case COMMENT:
			case SET:
			case RESET:
			case DATA:
			case START:
			case TRANSACTION:
			case COMMIT:
			case ROLLBACK:
			case MACRO:
			case IF:
			case DIV:
			case PERCENTLIT:
			case BUCKET:
			case OUT:
			case OF:
			case SORT:
			case CLUSTER:
			case DISTRIBUTE:
			case OVERWRITE:
			case TRANSFORM:
			case REDUCE:
			case USING:
			case SERDE:
			case SERDEPROPERTIES:
			case RECORDREADER:
			case RECORDWRITER:
			case DELIMITED:
			case FIELDS:
			case TERMINATED:
			case COLLECTION:
			case ITEMS:
			case KEYS:
			case ESCAPED:
			case LINES:
			case SEPARATED:
			case FUNCTION:
			case EXTENDED:
			case REFRESH:
			case CLEAR:
			case CACHE:
			case UNCACHE:
			case LAZY:
			case FORMATTED:
			case TEMPORARY:
			case OPTIONS:
			case UNSET:
			case TBLPROPERTIES:
			case DBPROPERTIES:
			case BUCKETS:
			case SKEWED:
			case STORED:
			case DIRECTORIES:
			case LOCATION:
			case EXCHANGE:
			case ARCHIVE:
			case UNARCHIVE:
			case FILEFORMAT:
			case TOUCH:
			case COMPACT:
			case CONCATENATE:
			case CHANGE:
			case CASCADE:
			case RESTRICT:
			case CLUSTERED:
			case SORTED:
			case PURGE:
			case INPUTFORMAT:
			case OUTPUTFORMAT:
			case DATABASE:
			case DATABASES:
			case DFS:
			case TRUNCATE:
			case ANALYZE:
			case COMPUTE:
			case LIST:
			case STATISTICS:
			case PARTITIONED:
			case EXTERNAL:
			case DEFINED:
			case REVOKE:
			case GRANT:
			case LOCK:
			case UNLOCK:
			case MSCK:
			case REPAIR:
			case EXPORT:
			case IMPORT:
			case LOAD:
			case ROLE:
			case ROLES:
			case COMPACTIONS:
			case PRINCIPALS:
			case TRANSACTIONS:
			case INDEX:
			case INDEXES:
			case LOCKS:
			case OPTION:
			case LOCAL:
			case INPATH:
				_localctx = new UnquotedIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(2136);
				nonReserved();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QuotedIdentifierContext extends ParserRuleContext {
		public TerminalNode BACKQUOTED_IDENTIFIER() { return getToken(SqlBaseParser.BACKQUOTED_IDENTIFIER, 0); }
		public QuotedIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_quotedIdentifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQuotedIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQuotedIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQuotedIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QuotedIdentifierContext quotedIdentifier() throws RecognitionException {
		QuotedIdentifierContext _localctx = new QuotedIdentifierContext(_ctx, getState());
		enterRule(_localctx, 170, RULE_quotedIdentifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2139);
			match(BACKQUOTED_IDENTIFIER);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NumberContext extends ParserRuleContext {
		public NumberContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_number; }
	 
		public NumberContext() { }
		public void copyFrom(NumberContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class DecimalLiteralContext extends NumberContext {
		public TerminalNode DECIMAL_VALUE() { return getToken(SqlBaseParser.DECIMAL_VALUE, 0); }
		public DecimalLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDecimalLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDecimalLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDecimalLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class BigIntLiteralContext extends NumberContext {
		public TerminalNode BIGINT_LITERAL() { return getToken(SqlBaseParser.BIGINT_LITERAL, 0); }
		public BigIntLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBigIntLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBigIntLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBigIntLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class TinyIntLiteralContext extends NumberContext {
		public TerminalNode TINYINT_LITERAL() { return getToken(SqlBaseParser.TINYINT_LITERAL, 0); }
		public TinyIntLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTinyIntLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTinyIntLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTinyIntLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DoubleLiteralContext extends NumberContext {
		public TerminalNode DOUBLE_LITERAL() { return getToken(SqlBaseParser.DOUBLE_LITERAL, 0); }
		public DoubleLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDoubleLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDoubleLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDoubleLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class IntegerLiteralContext extends NumberContext {
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public IntegerLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIntegerLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIntegerLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIntegerLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SmallIntLiteralContext extends NumberContext {
		public TerminalNode SMALLINT_LITERAL() { return getToken(SqlBaseParser.SMALLINT_LITERAL, 0); }
		public SmallIntLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSmallIntLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSmallIntLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSmallIntLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ScientificDecimalLiteralContext extends NumberContext {
		public TerminalNode SCIENTIFIC_DECIMAL_VALUE() { return getToken(SqlBaseParser.SCIENTIFIC_DECIMAL_VALUE, 0); }
		public ScientificDecimalLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterScientificDecimalLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitScientificDecimalLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitScientificDecimalLiteral(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NumberContext number() throws RecognitionException {
		NumberContext _localctx = new NumberContext(_ctx, getState());
		enterRule(_localctx, 172, RULE_number);
		try {
			setState(2148);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case DECIMAL_VALUE:
				_localctx = new DecimalLiteralContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2141);
				match(DECIMAL_VALUE);
				}
				break;
			case SCIENTIFIC_DECIMAL_VALUE:
				_localctx = new ScientificDecimalLiteralContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2142);
				match(SCIENTIFIC_DECIMAL_VALUE);
				}
				break;
			case INTEGER_VALUE:
				_localctx = new IntegerLiteralContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(2143);
				match(INTEGER_VALUE);
				}
				break;
			case BIGINT_LITERAL:
				_localctx = new BigIntLiteralContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(2144);
				match(BIGINT_LITERAL);
				}
				break;
			case SMALLINT_LITERAL:
				_localctx = new SmallIntLiteralContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(2145);
				match(SMALLINT_LITERAL);
				}
				break;
			case TINYINT_LITERAL:
				_localctx = new TinyIntLiteralContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(2146);
				match(TINYINT_LITERAL);
				}
				break;
			case DOUBLE_LITERAL:
				_localctx = new DoubleLiteralContext(_localctx);
				enterOuterAlt(_localctx, 7);
				{
				setState(2147);
				match(DOUBLE_LITERAL);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NonReservedContext extends ParserRuleContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode TABLES() { return getToken(SqlBaseParser.TABLES, 0); }
		public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
		public TerminalNode COLUMN() { return getToken(SqlBaseParser.COLUMN, 0); }
		public TerminalNode PARTITIONS() { return getToken(SqlBaseParser.PARTITIONS, 0); }
		public TerminalNode FUNCTIONS() { return getToken(SqlBaseParser.FUNCTIONS, 0); }
		public TerminalNode DATABASES() { return getToken(SqlBaseParser.DATABASES, 0); }
		public TerminalNode ADD() { return getToken(SqlBaseParser.ADD, 0); }
		public TerminalNode OVER() { return getToken(SqlBaseParser.OVER, 0); }
		public TerminalNode PARTITION() { return getToken(SqlBaseParser.PARTITION, 0); }
		public TerminalNode RANGE() { return getToken(SqlBaseParser.RANGE, 0); }
		public TerminalNode ROWS() { return getToken(SqlBaseParser.ROWS, 0); }
		public TerminalNode PRECEDING() { return getToken(SqlBaseParser.PRECEDING, 0); }
		public TerminalNode FOLLOWING() { return getToken(SqlBaseParser.FOLLOWING, 0); }
		public TerminalNode CURRENT() { return getToken(SqlBaseParser.CURRENT, 0); }
		public TerminalNode ROW() { return getToken(SqlBaseParser.ROW, 0); }
		public TerminalNode MAP() { return getToken(SqlBaseParser.MAP, 0); }
		public TerminalNode ARRAY() { return getToken(SqlBaseParser.ARRAY, 0); }
		public TerminalNode STRUCT() { return getToken(SqlBaseParser.STRUCT, 0); }
		public TerminalNode LATERAL() { return getToken(SqlBaseParser.LATERAL, 0); }
		public TerminalNode WINDOW() { return getToken(SqlBaseParser.WINDOW, 0); }
		public TerminalNode REDUCE() { return getToken(SqlBaseParser.REDUCE, 0); }
		public TerminalNode TRANSFORM() { return getToken(SqlBaseParser.TRANSFORM, 0); }
		public TerminalNode USING() { return getToken(SqlBaseParser.USING, 0); }
		public TerminalNode SERDE() { return getToken(SqlBaseParser.SERDE, 0); }
		public TerminalNode SERDEPROPERTIES() { return getToken(SqlBaseParser.SERDEPROPERTIES, 0); }
		public TerminalNode RECORDREADER() { return getToken(SqlBaseParser.RECORDREADER, 0); }
		public TerminalNode DELIMITED() { return getToken(SqlBaseParser.DELIMITED, 0); }
		public TerminalNode FIELDS() { return getToken(SqlBaseParser.FIELDS, 0); }
		public TerminalNode TERMINATED() { return getToken(SqlBaseParser.TERMINATED, 0); }
		public TerminalNode COLLECTION() { return getToken(SqlBaseParser.COLLECTION, 0); }
		public TerminalNode ITEMS() { return getToken(SqlBaseParser.ITEMS, 0); }
		public TerminalNode KEYS() { return getToken(SqlBaseParser.KEYS, 0); }
		public TerminalNode ESCAPED() { return getToken(SqlBaseParser.ESCAPED, 0); }
		public TerminalNode LINES() { return getToken(SqlBaseParser.LINES, 0); }
		public TerminalNode SEPARATED() { return getToken(SqlBaseParser.SEPARATED, 0); }
		public TerminalNode EXTENDED() { return getToken(SqlBaseParser.EXTENDED, 0); }
		public TerminalNode REFRESH() { return getToken(SqlBaseParser.REFRESH, 0); }
		public TerminalNode CLEAR() { return getToken(SqlBaseParser.CLEAR, 0); }
		public TerminalNode CACHE() { return getToken(SqlBaseParser.CACHE, 0); }
		public TerminalNode UNCACHE() { return getToken(SqlBaseParser.UNCACHE, 0); }
		public TerminalNode LAZY() { return getToken(SqlBaseParser.LAZY, 0); }
		public TerminalNode TEMPORARY() { return getToken(SqlBaseParser.TEMPORARY, 0); }
		public TerminalNode OPTIONS() { return getToken(SqlBaseParser.OPTIONS, 0); }
		public TerminalNode GROUPING() { return getToken(SqlBaseParser.GROUPING, 0); }
		public TerminalNode CUBE() { return getToken(SqlBaseParser.CUBE, 0); }
		public TerminalNode ROLLUP() { return getToken(SqlBaseParser.ROLLUP, 0); }
		public TerminalNode EXPLAIN() { return getToken(SqlBaseParser.EXPLAIN, 0); }
		public TerminalNode FORMAT() { return getToken(SqlBaseParser.FORMAT, 0); }
		public TerminalNode LOGICAL() { return getToken(SqlBaseParser.LOGICAL, 0); }
		public TerminalNode FORMATTED() { return getToken(SqlBaseParser.FORMATTED, 0); }
		public TerminalNode CODEGEN() { return getToken(SqlBaseParser.CODEGEN, 0); }
		public TerminalNode TABLESAMPLE() { return getToken(SqlBaseParser.TABLESAMPLE, 0); }
		public TerminalNode USE() { return getToken(SqlBaseParser.USE, 0); }
		public List<TerminalNode> TO() { return getTokens(SqlBaseParser.TO); }
		public TerminalNode TO(int i) {
			return getToken(SqlBaseParser.TO, i);
		}
		public TerminalNode BUCKET() { return getToken(SqlBaseParser.BUCKET, 0); }
		public TerminalNode PERCENTLIT() { return getToken(SqlBaseParser.PERCENTLIT, 0); }
		public TerminalNode OUT() { return getToken(SqlBaseParser.OUT, 0); }
		public TerminalNode OF() { return getToken(SqlBaseParser.OF, 0); }
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode RESET() { return getToken(SqlBaseParser.RESET, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public TerminalNode REPLACE() { return getToken(SqlBaseParser.REPLACE, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NO() { return getToken(SqlBaseParser.NO, 0); }
		public TerminalNode DATA() { return getToken(SqlBaseParser.DATA, 0); }
		public TerminalNode START() { return getToken(SqlBaseParser.START, 0); }
		public TerminalNode TRANSACTION() { return getToken(SqlBaseParser.TRANSACTION, 0); }
		public TerminalNode COMMIT() { return getToken(SqlBaseParser.COMMIT, 0); }
		public TerminalNode ROLLBACK() { return getToken(SqlBaseParser.ROLLBACK, 0); }
		public TerminalNode SORT() { return getToken(SqlBaseParser.SORT, 0); }
		public TerminalNode CLUSTER() { return getToken(SqlBaseParser.CLUSTER, 0); }
		public TerminalNode DISTRIBUTE() { return getToken(SqlBaseParser.DISTRIBUTE, 0); }
		public TerminalNode UNSET() { return getToken(SqlBaseParser.UNSET, 0); }
		public TerminalNode TBLPROPERTIES() { return getToken(SqlBaseParser.TBLPROPERTIES, 0); }
		public TerminalNode SKEWED() { return getToken(SqlBaseParser.SKEWED, 0); }
		public TerminalNode STORED() { return getToken(SqlBaseParser.STORED, 0); }
		public TerminalNode DIRECTORIES() { return getToken(SqlBaseParser.DIRECTORIES, 0); }
		public TerminalNode LOCATION() { return getToken(SqlBaseParser.LOCATION, 0); }
		public TerminalNode EXCHANGE() { return getToken(SqlBaseParser.EXCHANGE, 0); }
		public TerminalNode ARCHIVE() { return getToken(SqlBaseParser.ARCHIVE, 0); }
		public TerminalNode UNARCHIVE() { return getToken(SqlBaseParser.UNARCHIVE, 0); }
		public TerminalNode FILEFORMAT() { return getToken(SqlBaseParser.FILEFORMAT, 0); }
		public TerminalNode TOUCH() { return getToken(SqlBaseParser.TOUCH, 0); }
		public TerminalNode COMPACT() { return getToken(SqlBaseParser.COMPACT, 0); }
		public TerminalNode CONCATENATE() { return getToken(SqlBaseParser.CONCATENATE, 0); }
		public TerminalNode CHANGE() { return getToken(SqlBaseParser.CHANGE, 0); }
		public TerminalNode CASCADE() { return getToken(SqlBaseParser.CASCADE, 0); }
		public TerminalNode RESTRICT() { return getToken(SqlBaseParser.RESTRICT, 0); }
		public TerminalNode BUCKETS() { return getToken(SqlBaseParser.BUCKETS, 0); }
		public TerminalNode CLUSTERED() { return getToken(SqlBaseParser.CLUSTERED, 0); }
		public TerminalNode SORTED() { return getToken(SqlBaseParser.SORTED, 0); }
		public TerminalNode PURGE() { return getToken(SqlBaseParser.PURGE, 0); }
		public TerminalNode INPUTFORMAT() { return getToken(SqlBaseParser.INPUTFORMAT, 0); }
		public TerminalNode OUTPUTFORMAT() { return getToken(SqlBaseParser.OUTPUTFORMAT, 0); }
		public TerminalNode DBPROPERTIES() { return getToken(SqlBaseParser.DBPROPERTIES, 0); }
		public TerminalNode DFS() { return getToken(SqlBaseParser.DFS, 0); }
		public TerminalNode TRUNCATE() { return getToken(SqlBaseParser.TRUNCATE, 0); }
		public TerminalNode COMPUTE() { return getToken(SqlBaseParser.COMPUTE, 0); }
		public TerminalNode LIST() { return getToken(SqlBaseParser.LIST, 0); }
		public TerminalNode STATISTICS() { return getToken(SqlBaseParser.STATISTICS, 0); }
		public TerminalNode ANALYZE() { return getToken(SqlBaseParser.ANALYZE, 0); }
		public TerminalNode PARTITIONED() { return getToken(SqlBaseParser.PARTITIONED, 0); }
		public TerminalNode EXTERNAL() { return getToken(SqlBaseParser.EXTERNAL, 0); }
		public TerminalNode DEFINED() { return getToken(SqlBaseParser.DEFINED, 0); }
		public TerminalNode RECORDWRITER() { return getToken(SqlBaseParser.RECORDWRITER, 0); }
		public TerminalNode REVOKE() { return getToken(SqlBaseParser.REVOKE, 0); }
		public TerminalNode GRANT() { return getToken(SqlBaseParser.GRANT, 0); }
		public TerminalNode LOCK() { return getToken(SqlBaseParser.LOCK, 0); }
		public TerminalNode UNLOCK() { return getToken(SqlBaseParser.UNLOCK, 0); }
		public TerminalNode MSCK() { return getToken(SqlBaseParser.MSCK, 0); }
		public TerminalNode REPAIR() { return getToken(SqlBaseParser.REPAIR, 0); }
		public TerminalNode EXPORT() { return getToken(SqlBaseParser.EXPORT, 0); }
		public TerminalNode IMPORT() { return getToken(SqlBaseParser.IMPORT, 0); }
		public TerminalNode LOAD() { return getToken(SqlBaseParser.LOAD, 0); }
		public TerminalNode VALUES() { return getToken(SqlBaseParser.VALUES, 0); }
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public TerminalNode ROLE() { return getToken(SqlBaseParser.ROLE, 0); }
		public TerminalNode ROLES() { return getToken(SqlBaseParser.ROLES, 0); }
		public TerminalNode COMPACTIONS() { return getToken(SqlBaseParser.COMPACTIONS, 0); }
		public TerminalNode PRINCIPALS() { return getToken(SqlBaseParser.PRINCIPALS, 0); }
		public TerminalNode TRANSACTIONS() { return getToken(SqlBaseParser.TRANSACTIONS, 0); }
		public TerminalNode INDEX() { return getToken(SqlBaseParser.INDEX, 0); }
		public TerminalNode INDEXES() { return getToken(SqlBaseParser.INDEXES, 0); }
		public TerminalNode LOCKS() { return getToken(SqlBaseParser.LOCKS, 0); }
		public TerminalNode OPTION() { return getToken(SqlBaseParser.OPTION, 0); }
		public TerminalNode LOCAL() { return getToken(SqlBaseParser.LOCAL, 0); }
		public TerminalNode INPATH() { return getToken(SqlBaseParser.INPATH, 0); }
		public TerminalNode ASC() { return getToken(SqlBaseParser.ASC, 0); }
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public TerminalNode LIMIT() { return getToken(SqlBaseParser.LIMIT, 0); }
		public TerminalNode RENAME() { return getToken(SqlBaseParser.RENAME, 0); }
		public TerminalNode SETS() { return getToken(SqlBaseParser.SETS, 0); }
		public TerminalNode AT() { return getToken(SqlBaseParser.AT, 0); }
		public TerminalNode NULLS() { return getToken(SqlBaseParser.NULLS, 0); }
		public TerminalNode OVERWRITE() { return getToken(SqlBaseParser.OVERWRITE, 0); }
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public TerminalNode BETWEEN() { return getToken(SqlBaseParser.BETWEEN, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode DELETE() { return getToken(SqlBaseParser.DELETE, 0); }
		public TerminalNode DESCRIBE() { return getToken(SqlBaseParser.DESCRIBE, 0); }
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode FALSE() { return getToken(SqlBaseParser.FALSE, 0); }
		public TerminalNode FOR() { return getToken(SqlBaseParser.FOR, 0); }
		public TerminalNode GROUP() { return getToken(SqlBaseParser.GROUP, 0); }
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public TerminalNode INSERT() { return getToken(SqlBaseParser.INSERT, 0); }
		public TerminalNode INTO() { return getToken(SqlBaseParser.INTO, 0); }
		public TerminalNode IS() { return getToken(SqlBaseParser.IS, 0); }
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public TerminalNode ORDER() { return getToken(SqlBaseParser.ORDER, 0); }
		public TerminalNode OUTER() { return getToken(SqlBaseParser.OUTER, 0); }
		public List<TerminalNode> TABLE() { return getTokens(SqlBaseParser.TABLE); }
		public TerminalNode TABLE(int i) {
			return getToken(SqlBaseParser.TABLE, i);
		}
		public TerminalNode TRUE() { return getToken(SqlBaseParser.TRUE, 0); }
		public List<TerminalNode> WITH() { return getTokens(SqlBaseParser.WITH); }
		public TerminalNode WITH(int i) {
			return getToken(SqlBaseParser.WITH, i);
		}
		public TerminalNode RLIKE() { return getToken(SqlBaseParser.RLIKE, 0); }
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public TerminalNode CASE() { return getToken(SqlBaseParser.CASE, 0); }
		public TerminalNode CAST() { return getToken(SqlBaseParser.CAST, 0); }
		public TerminalNode DISTINCT() { return getToken(SqlBaseParser.DISTINCT, 0); }
		public TerminalNode DIV() { return getToken(SqlBaseParser.DIV, 0); }
		public TerminalNode ELSE() { return getToken(SqlBaseParser.ELSE, 0); }
		public TerminalNode END() { return getToken(SqlBaseParser.END, 0); }
		public TerminalNode FUNCTION() { return getToken(SqlBaseParser.FUNCTION, 0); }
		public TerminalNode INTERVAL() { return getToken(SqlBaseParser.INTERVAL, 0); }
		public TerminalNode MACRO() { return getToken(SqlBaseParser.MACRO, 0); }
		public TerminalNode OR() { return getToken(SqlBaseParser.OR, 0); }
		public TerminalNode STRATIFY() { return getToken(SqlBaseParser.STRATIFY, 0); }
		public TerminalNode THEN() { return getToken(SqlBaseParser.THEN, 0); }
		public TerminalNode UNBOUNDED() { return getToken(SqlBaseParser.UNBOUNDED, 0); }
		public TerminalNode WHEN() { return getToken(SqlBaseParser.WHEN, 0); }
		public TerminalNode DATABASE() { return getToken(SqlBaseParser.DATABASE, 0); }
		public TerminalNode SELECT() { return getToken(SqlBaseParser.SELECT, 0); }
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode WHERE() { return getToken(SqlBaseParser.WHERE, 0); }
		public TerminalNode HAVING() { return getToken(SqlBaseParser.HAVING, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public NonReservedContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nonReserved; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNonReserved(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNonReserved(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNonReserved(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NonReservedContext nonReserved() throws RecognitionException {
		NonReservedContext _localctx = new NonReservedContext(_ctx, getState());
		enterRule(_localctx, 174, RULE_nonReserved);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2150);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << SELECT) | (1L << FROM) | (1L << ADD) | (1L << AS) | (1L << ALL) | (1L << DISTINCT) | (1L << WHERE) | (1L << GROUP) | (1L << BY) | (1L << GROUPING) | (1L << SETS) | (1L << CUBE) | (1L << ROLLUP) | (1L << ORDER) | (1L << HAVING) | (1L << LIMIT) | (1L << AT) | (1L << OR) | (1L << AND) | (1L << IN) | (1L << NOT) | (1L << NO) | (1L << EXISTS) | (1L << BETWEEN) | (1L << LIKE) | (1L << RLIKE) | (1L << IS) | (1L << NULL) | (1L << TRUE) | (1L << FALSE) | (1L << NULLS) | (1L << ASC) | (1L << DESC) | (1L << FOR) | (1L << INTERVAL) | (1L << CASE) | (1L << WHEN) | (1L << THEN) | (1L << ELSE) | (1L << END) | (1L << OUTER) | (1L << LATERAL) | (1L << WINDOW) | (1L << OVER) | (1L << PARTITION) | (1L << RANGE) | (1L << ROWS))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (UNBOUNDED - 64)) | (1L << (PRECEDING - 64)) | (1L << (FOLLOWING - 64)) | (1L << (CURRENT - 64)) | (1L << (ROW - 64)) | (1L << (WITH - 64)) | (1L << (VALUES - 64)) | (1L << (CREATE - 64)) | (1L << (TABLE - 64)) | (1L << (VIEW - 64)) | (1L << (REPLACE - 64)) | (1L << (INSERT - 64)) | (1L << (DELETE - 64)) | (1L << (INTO - 64)) | (1L << (DESCRIBE - 64)) | (1L << (EXPLAIN - 64)) | (1L << (FORMAT - 64)) | (1L << (LOGICAL - 64)) | (1L << (CODEGEN - 64)) | (1L << (CAST - 64)) | (1L << (SHOW - 64)) | (1L << (TABLES - 64)) | (1L << (COLUMNS - 64)) | (1L << (COLUMN - 64)) | (1L << (USE - 64)) | (1L << (PARTITIONS - 64)) | (1L << (FUNCTIONS - 64)) | (1L << (DROP - 64)) | (1L << (TO - 64)) | (1L << (TABLESAMPLE - 64)) | (1L << (STRATIFY - 64)) | (1L << (ALTER - 64)) | (1L << (RENAME - 64)) | (1L << (ARRAY - 64)) | (1L << (MAP - 64)) | (1L << (STRUCT - 64)) | (1L << (COMMENT - 64)) | (1L << (SET - 64)) | (1L << (RESET - 64)) | (1L << (DATA - 64)) | (1L << (START - 64)) | (1L << (TRANSACTION - 64)) | (1L << (COMMIT - 64)) | (1L << (ROLLBACK - 64)) | (1L << (MACRO - 64)) | (1L << (IF - 64)) | (1L << (DIV - 64)))) != 0) || ((((_la - 131)) & ~0x3f) == 0 && ((1L << (_la - 131)) & ((1L << (PERCENTLIT - 131)) | (1L << (BUCKET - 131)) | (1L << (OUT - 131)) | (1L << (OF - 131)) | (1L << (SORT - 131)) | (1L << (CLUSTER - 131)) | (1L << (DISTRIBUTE - 131)) | (1L << (OVERWRITE - 131)) | (1L << (TRANSFORM - 131)) | (1L << (REDUCE - 131)) | (1L << (USING - 131)) | (1L << (SERDE - 131)) | (1L << (SERDEPROPERTIES - 131)) | (1L << (RECORDREADER - 131)) | (1L << (RECORDWRITER - 131)) | (1L << (DELIMITED - 131)) | (1L << (FIELDS - 131)) | (1L << (TERMINATED - 131)) | (1L << (COLLECTION - 131)) | (1L << (ITEMS - 131)) | (1L << (KEYS - 131)) | (1L << (ESCAPED - 131)) | (1L << (LINES - 131)) | (1L << (SEPARATED - 131)) | (1L << (FUNCTION - 131)) | (1L << (EXTENDED - 131)) | (1L << (REFRESH - 131)) | (1L << (CLEAR - 131)) | (1L << (CACHE - 131)) | (1L << (UNCACHE - 131)) | (1L << (LAZY - 131)) | (1L << (FORMATTED - 131)) | (1L << (TEMPORARY - 131)) | (1L << (OPTIONS - 131)) | (1L << (UNSET - 131)) | (1L << (TBLPROPERTIES - 131)) | (1L << (DBPROPERTIES - 131)) | (1L << (BUCKETS - 131)) | (1L << (SKEWED - 131)) | (1L << (STORED - 131)) | (1L << (DIRECTORIES - 131)) | (1L << (LOCATION - 131)) | (1L << (EXCHANGE - 131)) | (1L << (ARCHIVE - 131)) | (1L << (UNARCHIVE - 131)) | (1L << (FILEFORMAT - 131)) | (1L << (TOUCH - 131)) | (1L << (COMPACT - 131)) | (1L << (CONCATENATE - 131)) | (1L << (CHANGE - 131)) | (1L << (CASCADE - 131)) | (1L << (RESTRICT - 131)) | (1L << (CLUSTERED - 131)) | (1L << (SORTED - 131)) | (1L << (PURGE - 131)) | (1L << (INPUTFORMAT - 131)) | (1L << (OUTPUTFORMAT - 131)) | (1L << (DATABASE - 131)) | (1L << (DATABASES - 131)) | (1L << (DFS - 131)) | (1L << (TRUNCATE - 131)) | (1L << (ANALYZE - 131)) | (1L << (COMPUTE - 131)) | (1L << (LIST - 131)))) != 0) || ((((_la - 195)) & ~0x3f) == 0 && ((1L << (_la - 195)) & ((1L << (STATISTICS - 195)) | (1L << (PARTITIONED - 195)) | (1L << (EXTERNAL - 195)) | (1L << (DEFINED - 195)) | (1L << (REVOKE - 195)) | (1L << (GRANT - 195)) | (1L << (LOCK - 195)) | (1L << (UNLOCK - 195)) | (1L << (MSCK - 195)) | (1L << (REPAIR - 195)) | (1L << (EXPORT - 195)) | (1L << (IMPORT - 195)) | (1L << (LOAD - 195)) | (1L << (ROLE - 195)) | (1L << (ROLES - 195)) | (1L << (COMPACTIONS - 195)) | (1L << (PRINCIPALS - 195)) | (1L << (TRANSACTIONS - 195)) | (1L << (INDEX - 195)) | (1L << (INDEXES - 195)) | (1L << (LOCKS - 195)) | (1L << (OPTION - 195)) | (1L << (LOCAL - 195)) | (1L << (INPATH - 195)))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
		switch (ruleIndex) {
		case 34:
			return queryTerm_sempred((QueryTermContext)_localctx, predIndex);
		case 43:
			return relation_sempred((RelationContext)_localctx, predIndex);
		case 60:
			return booleanExpression_sempred((BooleanExpressionContext)_localctx, predIndex);
		case 63:
			return valueExpression_sempred((ValueExpressionContext)_localctx, predIndex);
		case 64:
			return primaryExpression_sempred((PrimaryExpressionContext)_localctx, predIndex);
		}
		return true;
	}
	private boolean queryTerm_sempred(QueryTermContext _localctx, int predIndex) {
		switch (predIndex) {
		case 0:
			return precpred(_ctx, 1);
		}
		return true;
	}
	private boolean relation_sempred(RelationContext _localctx, int predIndex) {
		switch (predIndex) {
		case 1:
			return precpred(_ctx, 2);
		}
		return true;
	}
	private boolean booleanExpression_sempred(BooleanExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 2:
			return precpred(_ctx, 3);
		case 3:
			return precpred(_ctx, 2);
		}
		return true;
	}
	private boolean valueExpression_sempred(ValueExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 4:
			return precpred(_ctx, 6);
		case 5:
			return precpred(_ctx, 5);
		case 6:
			return precpred(_ctx, 4);
		case 7:
			return precpred(_ctx, 3);
		case 8:
			return precpred(_ctx, 2);
		case 9:
			return precpred(_ctx, 1);
		}
		return true;
	}
	private boolean primaryExpression_sempred(PrimaryExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 10:
			return precpred(_ctx, 4);
		case 11:
			return precpred(_ctx, 2);
		}
		return true;
	}

	public static final String _serializedATN =
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3\u00ee\u086b\4\2\t"+
		"\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
		"\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
		"\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
		"\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t+\4"+
		",\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63\4\64\t"+
		"\64\4\65\t\65\4\66\t\66\4\67\t\67\48\t8\49\t9\4:\t:\4;\t;\4<\t<\4=\t="+
		"\4>\t>\4?\t?\4@\t@\4A\tA\4B\tB\4C\tC\4D\tD\4E\tE\4F\tF\4G\tG\4H\tH\4I"+
		"\tI\4J\tJ\4K\tK\4L\tL\4M\tM\4N\tN\4O\tO\4P\tP\4Q\tQ\4R\tR\4S\tS\4T\tT"+
		"\4U\tU\4V\tV\4W\tW\4X\tX\4Y\tY\3\2\3\2\3\2\3\3\3\3\3\3\3\4\3\4\3\4\3\5"+
		"\3\5\3\5\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\5\6\u00c7\n\6\3\6\3\6\3\6\5\6"+
		"\u00cc\n\6\3\6\5\6\u00cf\n\6\3\6\3\6\3\6\5\6\u00d4\n\6\3\6\3\6\3\6\3\6"+
		"\3\6\3\6\3\6\3\6\3\6\3\6\3\6\5\6\u00e1\n\6\3\6\3\6\5\6\u00e5\n\6\3\6\3"+
		"\6\3\6\3\6\3\6\5\6\u00ec\n\6\3\6\3\6\3\6\5\6\u00f1\n\6\3\6\3\6\3\6\5\6"+
		"\u00f6\n\6\3\6\5\6\u00f9\n\6\3\6\3\6\3\6\3\6\5\6\u00ff\n\6\3\6\3\6\3\6"+
		"\5\6\u0104\n\6\3\6\5\6\u0107\n\6\3\6\5\6\u010a\n\6\3\6\3\6\3\6\3\6\3\6"+
		"\3\6\3\6\5\6\u0113\n\6\3\6\3\6\5\6\u0117\n\6\3\6\3\6\3\6\3\6\3\6\3\6\5"+
		"\6\u011f\n\6\3\6\5\6\u0122\n\6\3\6\5\6\u0125\n\6\3\6\5\6\u0128\n\6\3\6"+
		"\5\6\u012b\n\6\3\6\5\6\u012e\n\6\3\6\3\6\5\6\u0132\n\6\3\6\5\6\u0135\n"+
		"\6\3\6\5\6\u0138\n\6\3\6\3\6\3\6\3\6\3\6\5\6\u013f\n\6\3\6\3\6\3\6\3\6"+
		"\3\6\3\6\3\6\3\6\5\6\u0149\n\6\3\6\3\6\3\6\3\6\3\6\3\6\5\6\u0151\n\6\5"+
		"\6\u0153\n\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3"+
		"\6\3\6\3\6\3\6\3\6\3\6\3\6\5\6\u016a\n\6\3\6\3\6\3\6\3\6\3\6\3\6\5\6\u0172"+
		"\n\6\3\6\3\6\3\6\3\6\3\6\3\6\5\6\u017a\n\6\3\6\3\6\3\6\3\6\5\6\u0180\n"+
		"\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\5\6\u018d\n\6\3\6\6\6\u0190"+
		"\n\6\r\6\16\6\u0191\3\6\3\6\3\6\3\6\3\6\3\6\3\6\5\6\u019b\n\6\3\6\6\6"+
		"\u019e\n\6\r\6\16\6\u019f\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6"+
		"\3\6\3\6\3\6\5\6\u01b0\n\6\3\6\3\6\3\6\7\6\u01b5\n\6\f\6\16\6\u01b8\13"+
		"\6\3\6\5\6\u01bb\n\6\3\6\3\6\3\6\3\6\3\6\3\6\5\6\u01c3\n\6\3\6\3\6\3\6"+
		"\7\6\u01c8\n\6\f\6\16\6\u01cb\13\6\3\6\3\6\3\6\3\6\5\6\u01d1\n\6\3\6\3"+
		"\6\3\6\3\6\3\6\3\6\3\6\5\6\u01da\n\6\3\6\3\6\5\6\u01de\n\6\3\6\3\6\3\6"+
		"\3\6\5\6\u01e4\n\6\3\6\3\6\3\6\3\6\5\6\u01ea\n\6\3\6\5\6\u01ed\n\6\3\6"+
		"\3\6\3\6\3\6\5\6\u01f3\n\6\3\6\3\6\5\6\u01f7\n\6\3\6\3\6\5\6\u01fb\n\6"+
		"\3\6\3\6\3\6\5\6\u0200\n\6\3\6\3\6\5\6\u0204\n\6\3\6\3\6\3\6\3\6\3\6\3"+
		"\6\5\6\u020c\n\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\5\6\u0215\n\6\3\6\3\6\3\6"+
		"\5\6\u021a\n\6\3\6\3\6\3\6\3\6\5\6\u0220\n\6\3\6\3\6\3\6\3\6\5\6\u0226"+
		"\n\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\7\6\u0230\n\6\f\6\16\6\u0233\13\6"+
		"\5\6\u0235\n\6\3\6\3\6\5\6\u0239\n\6\3\6\3\6\3\6\5\6\u023e\n\6\3\6\3\6"+
		"\3\6\5\6\u0243\n\6\3\6\3\6\3\6\3\6\3\6\5\6\u024a\n\6\3\6\5\6\u024d\n\6"+
		"\3\6\5\6\u0250\n\6\3\6\3\6\3\6\3\6\5\6\u0256\n\6\3\6\3\6\3\6\3\6\3\6\3"+
		"\6\3\6\5\6\u025f\n\6\3\6\3\6\3\6\3\6\3\6\3\6\5\6\u0267\n\6\3\6\3\6\3\6"+
		"\3\6\5\6\u026d\n\6\3\6\3\6\5\6\u0271\n\6\3\6\3\6\5\6\u0275\n\6\3\6\3\6"+
		"\5\6\u0279\n\6\5\6\u027b\n\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\5\6\u0284\n\6"+
		"\3\6\3\6\3\6\3\6\5\6\u028a\n\6\3\6\3\6\3\6\5\6\u028f\n\6\3\6\3\6\5\6\u0293"+
		"\n\6\3\6\5\6\u0296\n\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\7\6\u02a1\n"+
		"\6\f\6\16\6\u02a4\13\6\3\6\3\6\5\6\u02a8\n\6\3\6\3\6\3\6\5\6\u02ad\n\6"+
		"\3\6\5\6\u02b0\n\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\5\6\u02ba\n\6\3\6\3"+
		"\6\3\6\5\6\u02bf\n\6\3\6\3\6\3\6\3\6\5\6\u02c5\n\6\3\6\3\6\3\6\3\6\5\6"+
		"\u02cb\n\6\3\6\3\6\3\6\7\6\u02d0\n\6\f\6\16\6\u02d3\13\6\3\6\3\6\3\6\7"+
		"\6\u02d8\n\6\f\6\16\6\u02db\13\6\3\6\3\6\7\6\u02df\n\6\f\6\16\6\u02e2"+
		"\13\6\3\6\3\6\3\6\3\6\3\6\3\6\5\6\u02ea\n\6\3\6\3\6\3\6\3\6\3\6\3\6\3"+
		"\6\3\6\3\6\5\6\u02f5\n\6\3\6\3\6\3\6\3\6\3\6\7\6\u02fc\n\6\f\6\16\6\u02ff"+
		"\13\6\5\6\u0301\n\6\3\7\3\7\3\7\3\7\7\7\u0307\n\7\f\7\16\7\u030a\13\7"+
		"\3\7\3\7\3\b\3\b\5\b\u0310\n\b\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u0318\n\t\3"+
		"\t\3\t\5\t\u031c\n\t\3\t\3\t\3\t\3\t\3\t\5\t\u0323\n\t\3\t\3\t\3\t\3\t"+
		"\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3"+
		"\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t"+
		"\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3"+
		"\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t"+
		"\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3"+
		"\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t"+
		"\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u039a\n\t\3\t\3\t\3\t\3\t\3\t\3\t"+
		"\5\t\u03a2\n\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u03aa\n\t\3\t\3\t\3\t\3\t\3"+
		"\t\3\t\3\t\5\t\u03b3\n\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u03bc\n\t\3\t"+
		"\3\t\5\t\u03c0\n\t\3\t\3\t\3\t\3\t\5\t\u03c6\n\t\3\t\3\t\3\t\3\t\3\t\3"+
		"\t\3\t\3\t\3\t\3\t\5\t\u03d2\n\t\3\n\3\n\5\n\u03d6\n\n\3\n\5\n\u03d9\n"+
		"\n\3\n\3\n\3\n\3\n\5\n\u03df\n\n\3\n\3\n\3\13\3\13\3\13\3\13\3\13\3\13"+
		"\5\13\u03e9\n\13\3\13\3\13\3\13\3\13\3\f\3\f\3\f\3\f\3\f\3\f\5\f\u03f5"+
		"\n\f\3\f\3\f\3\f\5\f\u03fa\n\f\3\r\3\r\3\r\3\16\5\16\u0400\n\16\3\16\3"+
		"\16\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\5\17\u040c\n\17\5\17\u040e"+
		"\n\17\3\17\3\17\3\17\5\17\u0413\n\17\3\17\3\17\5\17\u0417\n\17\5\17\u0419"+
		"\n\17\3\20\3\20\5\20\u041d\n\20\3\21\3\21\3\21\3\21\3\21\7\21\u0424\n"+
		"\21\f\21\16\21\u0427\13\21\3\21\3\21\3\22\3\22\3\22\5\22\u042e\n\22\3"+
		"\23\3\23\3\23\3\23\3\23\5\23\u0435\n\23\3\24\3\24\3\24\3\24\5\24\u043b"+
		"\n\24\7\24\u043d\n\24\f\24\16\24\u0440\13\24\3\25\3\25\3\25\3\25\7\25"+
		"\u0446\n\25\f\25\16\25\u0449\13\25\3\26\3\26\5\26\u044d\n\26\3\26\3\26"+
		"\3\26\3\26\3\27\3\27\3\27\3\30\3\30\3\30\3\30\7\30\u045a\n\30\f\30\16"+
		"\30\u045d\13\30\3\30\3\30\3\31\3\31\5\31\u0463\n\31\3\31\5\31\u0466\n"+
		"\31\3\32\3\32\3\32\7\32\u046b\n\32\f\32\16\32\u046e\13\32\3\32\5\32\u0471"+
		"\n\32\3\33\3\33\3\33\3\33\7\33\u0477\n\33\f\33\16\33\u047a\13\33\3\33"+
		"\3\33\3\34\3\34\3\34\3\34\7\34\u0482\n\34\f\34\16\34\u0485\13\34\3\34"+
		"\3\34\3\35\3\35\3\35\3\35\3\35\3\35\5\35\u048f\n\35\3\36\3\36\3\36\3\36"+
		"\3\36\5\36\u0496\n\36\3\37\3\37\3\37\3\37\5\37\u049c\n\37\3 \3 \3 \3!"+
		"\5!\u04a2\n!\3!\3!\3!\3!\3!\6!\u04a9\n!\r!\16!\u04aa\5!\u04ad\n!\3\"\3"+
		"\"\3\"\3\"\3\"\7\"\u04b4\n\"\f\"\16\"\u04b7\13\"\5\"\u04b9\n\"\3\"\3\""+
		"\3\"\3\"\3\"\7\"\u04c0\n\"\f\"\16\"\u04c3\13\"\5\"\u04c5\n\"\3\"\3\"\3"+
		"\"\3\"\3\"\7\"\u04cc\n\"\f\"\16\"\u04cf\13\"\5\"\u04d1\n\"\3\"\3\"\3\""+
		"\3\"\3\"\7\"\u04d8\n\"\f\"\16\"\u04db\13\"\5\"\u04dd\n\"\3\"\5\"\u04e0"+
		"\n\"\3\"\3\"\5\"\u04e4\n\"\3#\5#\u04e7\n#\3#\3#\3#\3$\3$\3$\3$\3$\3$\5"+
		"$\u04f2\n$\3$\7$\u04f5\n$\f$\16$\u04f8\13$\3%\3%\3%\3%\3%\3%\3%\3%\5%"+
		"\u0502\n%\3&\3&\5&\u0506\n&\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\5"+
		"\'\u0512\n\'\3\'\5\'\u0515\n\'\3\'\3\'\5\'\u0519\n\'\3\'\3\'\3\'\3\'\3"+
		"\'\3\'\3\'\3\'\5\'\u0523\n\'\3\'\3\'\5\'\u0527\n\'\5\'\u0529\n\'\3\'\5"+
		"\'\u052c\n\'\3\'\3\'\5\'\u0530\n\'\3\'\5\'\u0533\n\'\3\'\3\'\5\'\u0537"+
		"\n\'\3\'\3\'\5\'\u053b\n\'\3\'\3\'\5\'\u053f\n\'\3\'\3\'\3\'\5\'\u0544"+
		"\n\'\3\'\5\'\u0547\n\'\5\'\u0549\n\'\3\'\7\'\u054c\n\'\f\'\16\'\u054f"+
		"\13\'\3\'\3\'\5\'\u0553\n\'\3\'\5\'\u0556\n\'\3\'\3\'\5\'\u055a\n\'\3"+
		"\'\5\'\u055d\n\'\5\'\u055f\n\'\3(\3(\3(\3(\7(\u0565\n(\f(\16(\u0568\13"+
		"(\3(\7(\u056b\n(\f(\16(\u056e\13(\3)\3)\3)\3)\3)\7)\u0575\n)\f)\16)\u0578"+
		"\13)\3)\3)\3)\3)\3)\3)\3)\3)\3)\3)\7)\u0584\n)\f)\16)\u0587\13)\3)\3)"+
		"\5)\u058b\n)\3*\3*\3*\3*\7*\u0591\n*\f*\16*\u0594\13*\5*\u0596\n*\3*\3"+
		"*\5*\u059a\n*\3+\3+\3+\5+\u059f\n+\3+\3+\3+\3+\3+\7+\u05a6\n+\f+\16+\u05a9"+
		"\13+\5+\u05ab\n+\3+\3+\3+\5+\u05b0\n+\3+\3+\3+\7+\u05b5\n+\f+\16+\u05b8"+
		"\13+\5+\u05ba\n+\3,\3,\3-\3-\3-\3-\3-\3-\5-\u05c4\n-\3-\3-\3-\5-\u05c9"+
		"\n-\3-\3-\3-\3-\3-\5-\u05d0\n-\7-\u05d2\n-\f-\16-\u05d5\13-\3.\5.\u05d8"+
		"\n.\3.\3.\5.\u05dc\n.\3.\3.\3.\3.\5.\u05e2\n.\3.\3.\5.\u05e6\n.\3.\5."+
		"\u05e9\n.\3.\5.\u05ec\n.\3/\3/\3/\3/\3/\3/\3/\7/\u05f5\n/\f/\16/\u05f8"+
		"\13/\3/\3/\5/\u05fc\n/\3\60\3\60\3\60\3\60\3\60\3\60\3\60\3\60\3\60\3"+
		"\60\3\60\3\60\3\60\3\60\3\60\3\60\3\60\3\60\3\60\5\60\u0611\n\60\5\60"+
		"\u0613\n\60\5\60\u0615\n\60\3\60\3\60\3\61\3\61\3\61\3\61\3\62\3\62\3"+
		"\62\7\62\u0620\n\62\f\62\16\62\u0623\13\62\3\63\3\63\3\63\3\63\7\63\u0629"+
		"\n\63\f\63\16\63\u062c\13\63\3\63\3\63\3\64\3\64\5\64\u0632\n\64\3\65"+
		"\3\65\3\65\3\65\7\65\u0638\n\65\f\65\16\65\u063b\13\65\3\65\3\65\3\66"+
		"\3\66\3\66\5\66\u0642\n\66\3\67\3\67\5\67\u0646\n\67\3\67\5\67\u0649\n"+
		"\67\3\67\5\67\u064c\n\67\3\67\3\67\3\67\3\67\5\67\u0652\n\67\3\67\5\67"+
		"\u0655\n\67\3\67\5\67\u0658\n\67\3\67\3\67\3\67\3\67\5\67\u065e\n\67\3"+
		"\67\5\67\u0661\n\67\3\67\5\67\u0664\n\67\3\67\5\67\u0667\n\67\38\38\3"+
		"8\38\78\u066d\n8\f8\168\u0670\138\38\58\u0673\n8\38\38\58\u0677\n8\58"+
		"\u0679\n8\39\39\39\39\39\39\39\59\u0682\n9\39\39\39\39\39\39\39\39\39"+
		"\39\59\u068e\n9\59\u0690\n9\39\39\39\39\39\59\u0697\n9\39\39\39\39\39"+
		"\59\u069e\n9\39\39\39\39\59\u06a4\n9\39\39\39\39\59\u06aa\n9\59\u06ac"+
		"\n9\3:\3:\3:\5:\u06b1\n:\3:\3:\3;\3;\5;\u06b7\n;\3;\3;\5;\u06bb\n;\5;"+
		"\u06bd\n;\3<\3<\3<\7<\u06c2\n<\f<\16<\u06c5\13<\3=\3=\3>\3>\3>\3>\3>\3"+
		">\3>\3>\3>\5>\u06d2\n>\3>\3>\3>\3>\3>\3>\7>\u06da\n>\f>\16>\u06dd\13>"+
		"\3?\3?\5?\u06e1\n?\3@\5@\u06e4\n@\3@\3@\3@\3@\3@\3@\5@\u06ec\n@\3@\3@"+
		"\3@\3@\3@\7@\u06f3\n@\f@\16@\u06f6\13@\3@\3@\3@\5@\u06fb\n@\3@\3@\3@\3"+
		"@\3@\3@\5@\u0703\n@\3@\3@\3@\3@\5@\u0709\n@\3@\5@\u070c\n@\3A\3A\3A\3"+
		"A\5A\u0712\nA\3A\3A\3A\3A\3A\3A\3A\3A\3A\3A\3A\3A\3A\3A\3A\3A\3A\3A\3"+
		"A\7A\u0727\nA\fA\16A\u072a\13A\3B\3B\3B\3B\3B\3B\3B\3B\3B\3B\3B\6B\u0737"+
		"\nB\rB\16B\u0738\3B\3B\3B\3B\3B\5B\u0740\nB\3B\3B\3B\7B\u0745\nB\fB\16"+
		"B\u0748\13B\5B\u074a\nB\3B\3B\3B\5B\u074f\nB\3B\3B\3B\3B\3B\3B\3B\6B\u0758"+
		"\nB\rB\16B\u0759\3B\3B\5B\u075e\nB\3B\3B\3B\3B\6B\u0764\nB\rB\16B\u0765"+
		"\3B\3B\5B\u076a\nB\3B\3B\3B\3B\3B\3B\3B\3B\3B\3B\3B\3B\3B\3B\5B\u077a"+
		"\nB\3B\3B\3B\3B\3B\3B\3B\3B\7B\u0784\nB\fB\16B\u0787\13B\3C\3C\3C\3C\3"+
		"C\3C\3C\3C\6C\u0791\nC\rC\16C\u0792\5C\u0795\nC\3D\3D\3E\3E\3F\3F\3G\3"+
		"G\3H\3H\7H\u07a1\nH\fH\16H\u07a4\13H\3I\3I\3I\3I\5I\u07aa\nI\3J\5J\u07ad"+
		"\nJ\3J\3J\5J\u07b1\nJ\3K\3K\3K\3K\3K\3K\3K\3K\3K\3K\3K\3K\3K\3K\3K\5K"+
		"\u07c2\nK\3K\3K\5K\u07c6\nK\3K\3K\3K\3K\3K\7K\u07cd\nK\fK\16K\u07d0\13"+
		"K\3K\5K\u07d3\nK\5K\u07d5\nK\3L\3L\3L\7L\u07da\nL\fL\16L\u07dd\13L\3M"+
		"\3M\5M\u07e1\nM\3M\3M\3M\5M\u07e6\nM\3N\3N\3N\3N\3N\3O\3O\3O\3O\7O\u07f1"+
		"\nO\fO\16O\u07f4\13O\3P\3P\3P\3P\3Q\3Q\3Q\3Q\3Q\3Q\3Q\7Q\u0801\nQ\fQ\16"+
		"Q\u0804\13Q\3Q\3Q\3Q\3Q\3Q\7Q\u080b\nQ\fQ\16Q\u080e\13Q\5Q\u0810\nQ\3"+
		"Q\3Q\3Q\3Q\3Q\7Q\u0817\nQ\fQ\16Q\u081a\13Q\5Q\u081c\nQ\5Q\u081e\nQ\3Q"+
		"\5Q\u0821\nQ\3Q\5Q\u0824\nQ\3R\3R\3R\3R\3R\3R\3R\3R\3R\3R\3R\3R\3R\3R"+
		"\3R\3R\5R\u0836\nR\3S\3S\3S\3S\3S\3S\3S\5S\u083f\nS\3T\3T\3T\7T\u0844"+
		"\nT\fT\16T\u0847\13T\3U\3U\3U\3U\3U\3U\3U\3U\3U\3U\3U\3U\3U\3U\5U\u0857"+
		"\nU\3V\3V\3V\5V\u085c\nV\3W\3W\3X\3X\3X\3X\3X\3X\3X\5X\u0867\nX\3Y\3Y"+
		"\3Y\7\u02a2\u02d1\u02d9\u02e0\u02fd\7FXz\u0080\u0082Z\2\4\6\b\n\f\16\20"+
		"\22\24\26\30\32\34\36 \"$&(*,.\60\62\64\668:<>@BDFHJLNPRTVXZ\\^`bdfhj"+
		"lnprtvxz|~\u0080\u0082\u0084\u0086\u0088\u008a\u008c\u008e\u0090\u0092"+
		"\u0094\u0096\u0098\u009a\u009c\u009e\u00a0\u00a2\u00a4\u00a6\u00a8\u00aa"+
		"\u00ac\u00ae\u00b0\2\31\3\2\u00b7\u00b8\3\2JK\5\2ST\u009e\u009e\u00a4"+
		"\u00a4\4\2\13\13\35\35\4\2**PP\4\2\u009e\u009e\u00a4\u00a4\4\2\f\f\u00c4"+
		"\u00c4\3\2)*\3\2^`\3\2\16\17\3\2\u00e4\u00e5\3\2\"#\4\2{|\u0081\u0081"+
		"\3\2}\u0080\3\2{|\3\2sz\3\2{\u0084\3\2\33\36\3\2&\'\4\2??\u008b\u008b"+
		"\4\2\27\27\u0089\u0089\3\2CD\t\2\n\61\64\64<]ar\u0080\u0080\u0085\u00da"+
		"\u00dc\u00dd\u09d6\2\u00b2\3\2\2\2\4\u00b5\3\2\2\2\6\u00b8\3\2\2\2\b\u00bb"+
		"\3\2\2\2\n\u0300\3\2\2\2\f\u0302\3\2\2\2\16\u030d\3\2\2\2\20\u03d1\3\2"+
		"\2\2\22\u03d3\3\2\2\2\24\u03e2\3\2\2\2\26\u03ee\3\2\2\2\30\u03fb\3\2\2"+
		"\2\32\u03ff\3\2\2\2\34\u0418\3\2\2\2\36\u041a\3\2\2\2 \u041e\3\2\2\2\""+
		"\u042a\3\2\2\2$\u0434\3\2\2\2&\u0436\3\2\2\2(\u0441\3\2\2\2*\u044a\3\2"+
		"\2\2,\u0452\3\2\2\2.\u0455\3\2\2\2\60\u0460\3\2\2\2\62\u0470\3\2\2\2\64"+
		"\u0472\3\2\2\2\66\u047d\3\2\2\28\u048e\3\2\2\2:\u0495\3\2\2\2<\u0497\3"+
		"\2\2\2>\u049d\3\2\2\2@\u04ac\3\2\2\2B\u04b8\3\2\2\2D\u04e6\3\2\2\2F\u04eb"+
		"\3\2\2\2H\u0501\3\2\2\2J\u0503\3\2\2\2L\u055e\3\2\2\2N\u0560\3\2\2\2P"+
		"\u056f\3\2\2\2R\u0599\3\2\2\2T\u059b\3\2\2\2V\u05bb\3\2\2\2X\u05bd\3\2"+
		"\2\2Z\u05eb\3\2\2\2\\\u05fb\3\2\2\2^\u05fd\3\2\2\2`\u0618\3\2\2\2b\u061c"+
		"\3\2\2\2d\u0624\3\2\2\2f\u062f\3\2\2\2h\u0633\3\2\2\2j\u063e\3\2\2\2l"+
		"\u0666\3\2\2\2n\u0668\3\2\2\2p\u06ab\3\2\2\2r\u06b0\3\2\2\2t\u06b4\3\2"+
		"\2\2v\u06be\3\2\2\2x\u06c6\3\2\2\2z\u06d1\3\2\2\2|\u06de\3\2\2\2~\u070b"+
		"\3\2\2\2\u0080\u0711\3\2\2\2\u0082\u0779\3\2\2\2\u0084\u0794\3\2\2\2\u0086"+
		"\u0796\3\2\2\2\u0088\u0798\3\2\2\2\u008a\u079a\3\2\2\2\u008c\u079c\3\2"+
		"\2\2\u008e\u079e\3\2\2\2\u0090\u07a5\3\2\2\2\u0092\u07b0\3\2\2\2\u0094"+
		"\u07d4\3\2\2\2\u0096\u07d6\3\2\2\2\u0098\u07de\3\2\2\2\u009a\u07e7\3\2"+
		"\2\2\u009c\u07ec\3\2\2\2\u009e\u07f5\3\2\2\2\u00a0\u0823\3\2\2\2\u00a2"+
		"\u0835\3\2\2\2\u00a4\u083e\3\2\2\2\u00a6\u0840\3\2\2\2\u00a8\u0856\3\2"+
		"\2\2\u00aa\u085b\3\2\2\2\u00ac\u085d\3\2\2\2\u00ae\u0866\3\2\2\2\u00b0"+
		"\u0868\3\2\2\2\u00b2\u00b3\5\n\6\2\u00b3\u00b4\7\2\2\3\u00b4\3\3\2\2\2"+
		"\u00b5\u00b6\5t;\2\u00b6\u00b7\7\2\2\3\u00b7\5\3\2\2\2\u00b8\u00b9\5r"+
		":\2\u00b9\u00ba\7\2\2\3\u00ba\7\3\2\2\2\u00bb\u00bc\5\u0094K\2\u00bc\u00bd"+
		"\7\2\2\3\u00bd\t\3\2\2\2\u00be\u0301\5\32\16\2\u00bf\u00c0\7Z\2\2\u00c0"+
		"\u0301\5\u00a8U\2\u00c1\u00c2\7I\2\2\u00c2\u00c6\7\u00be\2\2\u00c3\u00c4"+
		"\7r\2\2\u00c4\u00c5\7\36\2\2\u00c5\u00c7\7 \2\2\u00c6\u00c3\3\2\2\2\u00c6"+
		"\u00c7\3\2\2\2\u00c7\u00c8\3\2\2\2\u00c8\u00cb\5\u00a8U\2\u00c9\u00ca"+
		"\7i\2\2\u00ca\u00cc\7\u00df\2\2\u00cb\u00c9\3\2\2\2\u00cb\u00cc\3\2\2"+
		"\2\u00cc\u00ce\3\2\2\2\u00cd\u00cf\5\30\r\2\u00ce\u00cd\3\2\2\2\u00ce"+
		"\u00cf\3\2\2\2\u00cf\u00d3\3\2\2\2\u00d0\u00d1\7G\2\2\u00d1\u00d2\7\u00a9"+
		"\2\2\u00d2\u00d4\5.\30\2\u00d3\u00d0\3\2\2\2\u00d3\u00d4\3\2\2\2\u00d4"+
		"\u0301\3\2\2\2\u00d5\u00d6\7d\2\2\u00d6\u00d7\7\u00be\2\2\u00d7\u00d8"+
		"\5\u00a8U\2\u00d8\u00d9\7j\2\2\u00d9\u00da\7\u00a9\2\2\u00da\u00db\5."+
		"\30\2\u00db\u0301\3\2\2\2\u00dc\u00dd\7]\2\2\u00dd\u00e0\7\u00be\2\2\u00de"+
		"\u00df\7r\2\2\u00df\u00e1\7 \2\2\u00e0\u00de\3\2\2\2\u00e0\u00e1\3\2\2"+
		"\2\u00e1\u00e2\3\2\2\2\u00e2\u00e4\5\u00a8U\2\u00e3\u00e5\t\2\2\2\u00e4"+
		"\u00e3\3\2\2\2\u00e4\u00e5\3\2\2\2\u00e5\u0301\3\2\2\2\u00e6\u00eb\5\22"+
		"\n\2\u00e7\u00e8\7\3\2\2\u00e8\u00e9\5\u0096L\2\u00e9\u00ea\7\4\2\2\u00ea"+
		"\u00ec\3\2\2\2\u00eb\u00e7\3\2\2\2\u00eb\u00ec\3\2\2\2\u00ec\u00ed\3\2"+
		"\2\2\u00ed\u00f0\5,\27\2\u00ee\u00ef\7\u00a6\2\2\u00ef\u00f1\5.\30\2\u00f0"+
		"\u00ee\3\2\2\2\u00f0\u00f1\3\2\2\2\u00f1\u00f5\3\2\2\2\u00f2\u00f3\7\u00c6"+
		"\2\2\u00f3\u00f4\7\22\2\2\u00f4\u00f6\5`\61\2\u00f5\u00f2\3\2\2\2\u00f5"+
		"\u00f6\3\2\2\2\u00f6\u00f8\3\2\2\2\u00f7\u00f9\5\24\13\2\u00f8\u00f7\3"+
		"\2\2\2\u00f8\u00f9\3\2\2\2\u00f9\u0301\3\2\2\2\u00fa\u00fb\5\22\n\2\u00fb"+
		"\u00fe\5,\27\2\u00fc\u00fd\7\u00a6\2\2\u00fd\u00ff\5.\30\2\u00fe\u00fc"+
		"\3\2\2\2\u00fe\u00ff\3\2\2\2\u00ff\u0103\3\2\2\2\u0100\u0101\7\u00c6\2"+
		"\2\u0101\u0102\7\22\2\2\u0102\u0104\5`\61\2\u0103\u0100\3\2\2\2\u0103"+
		"\u0104\3\2\2\2\u0104\u0106\3\2\2\2\u0105\u0107\5\24\13\2\u0106\u0105\3"+
		"\2\2\2\u0106\u0107\3\2\2\2\u0107\u0109\3\2\2\2\u0108\u010a\7\r\2\2\u0109"+
		"\u0108\3\2\2\2\u0109\u010a\3\2\2\2\u010a\u010b\3\2\2\2\u010b\u010c\5\32"+
		"\16\2\u010c\u0301\3\2\2\2\u010d\u0112\5\22\n\2\u010e\u010f\7\3\2\2\u010f"+
		"\u0110\5\u0096L\2\u0110\u0111\7\4\2\2\u0111\u0113\3\2\2\2\u0112\u010e"+
		"\3\2\2\2\u0112\u0113\3\2\2\2\u0113\u0116\3\2\2\2\u0114\u0115\7i\2\2\u0115"+
		"\u0117\7\u00df\2\2\u0116\u0114\3\2\2\2\u0116\u0117\3\2\2\2\u0117\u011e"+
		"\3\2\2\2\u0118\u0119\7\u00c6\2\2\u0119\u011a\7\22\2\2\u011a\u011b\7\3"+
		"\2\2\u011b\u011c\5\u0096L\2\u011c\u011d\7\4\2\2\u011d\u011f\3\2\2\2\u011e"+
		"\u0118\3\2\2\2\u011e\u011f\3\2\2\2\u011f\u0121\3\2\2\2\u0120\u0122\5\24"+
		"\13\2\u0121\u0120\3\2\2\2\u0121\u0122\3\2\2\2\u0122\u0124\3\2\2\2\u0123"+
		"\u0125\5\26\f\2\u0124\u0123\3\2\2\2\u0124\u0125\3\2\2\2\u0125\u0127\3"+
		"\2\2\2\u0126\u0128\5p9\2\u0127\u0126\3\2\2\2\u0127\u0128\3\2\2\2\u0128"+
		"\u012a\3\2\2\2\u0129\u012b\58\35\2\u012a\u0129\3\2\2\2\u012a\u012b\3\2"+
		"\2\2\u012b\u012d\3\2\2\2\u012c\u012e\5\30\r\2\u012d\u012c\3\2\2\2\u012d"+
		"\u012e\3\2\2\2\u012e\u0131\3\2\2\2\u012f\u0130\7\u00a8\2\2\u0130\u0132"+
		"\5.\30\2\u0131\u012f\3\2\2\2\u0131\u0132\3\2\2\2\u0132\u0137\3\2\2\2\u0133"+
		"\u0135\7\r\2\2\u0134\u0133\3\2\2\2\u0134\u0135\3\2\2\2\u0135\u0136\3\2"+
		"\2\2\u0136\u0138\5\32\16\2\u0137\u0134\3\2\2\2\u0137\u0138\3\2\2\2\u0138"+
		"\u0301\3\2\2\2\u0139\u013a\7I\2\2\u013a\u013e\7J\2\2\u013b\u013c\7r\2"+
		"\2\u013c\u013d\7\36\2\2\u013d\u013f\7 \2\2\u013e\u013b\3\2\2\2\u013e\u013f"+
		"\3\2\2\2\u013f\u0140\3\2\2\2\u0140\u0141\5r:\2\u0141\u0142\7\"\2\2\u0142"+
		"\u0143\5r:\2\u0143\u0301\3\2\2\2\u0144\u0145\7\u00c2\2\2\u0145\u0146\7"+
		"J\2\2\u0146\u0148\5r:\2\u0147\u0149\5 \21\2\u0148\u0147\3\2\2\2\u0148"+
		"\u0149\3\2\2\2\u0149\u014a\3\2\2\2\u014a\u014b\7\u00c3\2\2\u014b\u0152"+
		"\7\u00c5\2\2\u014c\u0153\5\u00a8U\2\u014d\u014e\7+\2\2\u014e\u0150\7X"+
		"\2\2\u014f\u0151\5b\62\2\u0150\u014f\3\2\2\2\u0150\u0151\3\2\2\2\u0151"+
		"\u0153\3\2\2\2\u0152\u014c\3\2\2\2\u0152\u014d\3\2\2\2\u0152\u0153\3\2"+
		"\2\2\u0153\u0301\3\2\2\2\u0154\u0155\7d\2\2\u0155\u0156\t\3\2\2\u0156"+
		"\u0157\5r:\2\u0157\u0158\7e\2\2\u0158\u0159\7a\2\2\u0159\u015a\5r:\2\u015a"+
		"\u0301\3\2\2\2\u015b\u015c\7d\2\2\u015c\u015d\t\3\2\2\u015d\u015e\5r:"+
		"\2\u015e\u015f\7j\2\2\u015f\u0160\7\u00a8\2\2\u0160\u0161\5.\30\2\u0161"+
		"\u0301\3\2\2\2\u0162\u0163\7d\2\2\u0163\u0164\t\3\2\2\u0164\u0165\5r:"+
		"\2\u0165\u0166\7\u00a7\2\2\u0166\u0169\7\u00a8\2\2\u0167\u0168\7r\2\2"+
		"\u0168\u016a\7 \2\2\u0169\u0167\3\2\2\2\u0169\u016a\3\2\2\2\u016a\u016b"+
		"\3\2\2\2\u016b\u016c\5.\30\2\u016c\u0301\3\2\2\2\u016d\u016e\7d\2\2\u016e"+
		"\u016f\7J\2\2\u016f\u0171\5r:\2\u0170\u0172\5 \21\2\u0171\u0170\3\2\2"+
		"\2\u0171\u0172\3\2\2\2\u0172\u0173\3\2\2\2\u0173\u0174\7j\2\2\u0174\u0175"+
		"\7\u0090\2\2\u0175\u0179\7\u00df\2\2\u0176\u0177\7G\2\2\u0177\u0178\7"+
		"\u0091\2\2\u0178\u017a\5.\30\2\u0179\u0176\3\2\2\2\u0179\u017a\3\2\2\2"+
		"\u017a\u0301\3\2\2\2\u017b\u017c\7d\2\2\u017c\u017d\7J\2\2\u017d\u017f"+
		"\5r:\2\u017e\u0180\5 \21\2\u017f\u017e\3\2\2\2\u017f\u0180\3\2\2\2\u0180"+
		"\u0181\3\2\2\2\u0181\u0182\7j\2\2\u0182\u0183\7\u0091\2\2\u0183\u0184"+
		"\5.\30\2\u0184\u0301\3\2\2\2\u0185\u0186\7d\2\2\u0186\u0187\7J\2\2\u0187"+
		"\u0188\5r:\2\u0188\u018c\7\f\2\2\u0189\u018a\7r\2\2\u018a\u018b\7\36\2"+
		"\2\u018b\u018d\7 \2\2\u018c\u0189\3\2\2\2\u018c\u018d\3\2\2\2\u018d\u018f"+
		"\3\2\2\2\u018e\u0190\5\36\20\2\u018f\u018e\3\2\2\2\u0190\u0191\3\2\2\2"+
		"\u0191\u018f\3\2\2\2\u0191\u0192\3\2\2\2\u0192\u0301\3\2\2\2\u0193\u0194"+
		"\7d\2\2\u0194\u0195\7K\2\2\u0195\u0196\5r:\2\u0196\u019a\7\f\2\2\u0197"+
		"\u0198\7r\2\2\u0198\u0199\7\36\2\2\u0199\u019b\7 \2\2\u019a\u0197\3\2"+
		"\2\2\u019a\u019b\3\2\2\2\u019b\u019d\3\2\2\2\u019c\u019e\5 \21\2\u019d"+
		"\u019c\3\2\2\2\u019e\u019f\3\2\2\2\u019f\u019d\3\2\2\2\u019f\u01a0\3\2"+
		"\2\2\u01a0\u0301\3\2\2\2\u01a1\u01a2\7d\2\2\u01a2\u01a3\7J\2\2\u01a3\u01a4"+
		"\5r:\2\u01a4\u01a5\5 \21\2\u01a5\u01a6\7e\2\2\u01a6\u01a7\7a\2\2\u01a7"+
		"\u01a8\5 \21\2\u01a8\u0301\3\2\2\2\u01a9\u01aa\7d\2\2\u01aa\u01ab\7J\2"+
		"\2\u01ab\u01ac\5r:\2\u01ac\u01af\7]\2\2\u01ad\u01ae\7r\2\2\u01ae\u01b0"+
		"\7 \2\2\u01af\u01ad\3\2\2\2\u01af\u01b0\3\2\2\2\u01b0\u01b1\3\2\2\2\u01b1"+
		"\u01b6\5 \21\2\u01b2\u01b3\7\5\2\2\u01b3\u01b5\5 \21\2\u01b4\u01b2\3\2"+
		"\2\2\u01b5\u01b8\3\2\2\2\u01b6\u01b4\3\2\2\2\u01b6\u01b7\3\2\2\2\u01b7"+
		"\u01ba\3\2\2\2\u01b8\u01b6\3\2\2\2\u01b9\u01bb\7\u00bb\2\2\u01ba\u01b9"+
		"\3\2\2\2\u01ba\u01bb\3\2\2\2\u01bb\u0301\3\2\2\2\u01bc\u01bd\7d\2\2\u01bd"+
		"\u01be\7K\2\2\u01be\u01bf\5r:\2\u01bf\u01c2\7]\2\2\u01c0\u01c1\7r\2\2"+
		"\u01c1\u01c3\7 \2\2\u01c2\u01c0\3\2\2\2\u01c2\u01c3\3\2\2\2\u01c3\u01c4"+
		"\3\2\2\2\u01c4\u01c9\5 \21\2\u01c5\u01c6\7\5\2\2\u01c6\u01c8\5 \21\2\u01c7"+
		"\u01c5\3\2\2\2\u01c8\u01cb\3\2\2\2\u01c9\u01c7\3\2\2\2\u01c9\u01ca\3\2"+
		"\2\2\u01ca\u0301\3\2\2\2\u01cb\u01c9\3\2\2\2\u01cc\u01cd\7d\2\2\u01cd"+
		"\u01ce\7J\2\2\u01ce\u01d0\5r:\2\u01cf\u01d1\5 \21\2\u01d0\u01cf\3\2\2"+
		"\2\u01d0\u01d1\3\2\2\2\u01d1\u01d2\3\2\2\2\u01d2\u01d3\7j\2\2\u01d3\u01d4"+
		"\5\30\r\2\u01d4\u0301\3\2\2\2\u01d5\u01d6\7]\2\2\u01d6\u01d9\7J\2\2\u01d7"+
		"\u01d8\7r\2\2\u01d8\u01da\7 \2\2\u01d9\u01d7\3\2\2\2\u01d9\u01da\3\2\2"+
		"\2\u01da\u01db\3\2\2\2\u01db\u01dd\5r:\2\u01dc\u01de\7\u00bb\2\2\u01dd"+
		"\u01dc\3\2\2\2\u01dd\u01de\3\2\2\2\u01de\u0301\3\2\2\2\u01df\u01e0\7]"+
		"\2\2\u01e0\u01e3\7K\2\2\u01e1\u01e2\7r\2\2\u01e2\u01e4\7 \2\2\u01e3\u01e1"+
		"\3\2\2\2\u01e3\u01e4\3\2\2\2\u01e4\u01e5\3\2\2\2\u01e5\u0301\5r:\2\u01e6"+
		"\u01e9\7I\2\2\u01e7\u01e8\7\33\2\2\u01e8\u01ea\7L\2\2\u01e9\u01e7\3\2"+
		"\2\2\u01e9\u01ea\3\2\2\2\u01ea\u01ec\3\2\2\2\u01eb\u01ed\7\u00a5\2\2\u01ec"+
		"\u01eb\3\2\2\2\u01ec\u01ed\3\2\2\2\u01ed\u01ee\3\2\2\2\u01ee\u01f2\7K"+
		"\2\2\u01ef\u01f0\7r\2\2\u01f0\u01f1\7\36\2\2\u01f1\u01f3\7 \2\2\u01f2"+
		"\u01ef\3\2\2\2\u01f2\u01f3\3\2\2\2\u01f3\u01f4\3\2\2\2\u01f4\u01f6\5r"+
		":\2\u01f5\u01f7\5h\65\2\u01f6\u01f5\3\2\2\2\u01f6\u01f7\3\2\2\2\u01f7"+
		"\u01fa\3\2\2\2\u01f8\u01f9\7i\2\2\u01f9\u01fb\7\u00df\2\2\u01fa\u01f8"+
		"\3\2\2\2\u01fa\u01fb\3\2\2\2\u01fb\u01ff\3\2\2\2\u01fc\u01fd\7\u00c6\2"+
		"\2\u01fd\u01fe\7;\2\2\u01fe\u0200\5`\61\2\u01ff\u01fc\3\2\2\2\u01ff\u0200"+
		"\3\2\2\2\u0200\u0203\3\2\2\2\u0201\u0202\7\u00a8\2\2\u0202\u0204\5.\30"+
		"\2\u0203\u0201\3\2\2\2\u0203\u0204\3\2\2\2\u0204\u0205\3\2\2\2\u0205\u0206"+
		"\7\r\2\2\u0206\u0207\5\32\16\2\u0207\u0301\3\2\2\2\u0208\u020b\7I\2\2"+
		"\u0209\u020a\7\33\2\2\u020a\u020c\7L\2\2\u020b\u0209\3\2\2\2\u020b\u020c"+
		"\3\2\2\2\u020c\u020d\3\2\2\2\u020d\u020e\7\u00a5\2\2\u020e\u020f\7K\2"+
		"\2\u020f\u0214\5r:\2\u0210\u0211\7\3\2\2\u0211\u0212\5\u0096L\2\u0212"+
		"\u0213\7\4\2\2\u0213\u0215\3\2\2\2\u0214\u0210\3\2\2\2\u0214\u0215\3\2"+
		"\2\2\u0215\u0216\3\2\2\2\u0216\u0219\5,\27\2\u0217\u0218\7\u00a6\2\2\u0218"+
		"\u021a\5.\30\2\u0219\u0217\3\2\2\2\u0219\u021a\3\2\2\2\u021a\u0301\3\2"+
		"\2\2\u021b\u021c\7d\2\2\u021c\u021d\7K\2\2\u021d\u021f\5r:\2\u021e\u0220"+
		"\7\r\2\2\u021f\u021e\3\2\2\2\u021f\u0220\3\2\2\2\u0220\u0221\3\2\2\2\u0221"+
		"\u0222\5\32\16\2\u0222\u0301\3\2\2\2\u0223\u0225\7I\2\2\u0224\u0226\7"+
		"\u00a5\2\2\u0225\u0224\3\2\2\2\u0225\u0226\3\2\2\2\u0226\u0227\3\2\2\2"+
		"\u0227\u0228\7\u009d\2\2\u0228\u0229\5\u00a6T\2\u0229\u022a\7\r\2\2\u022a"+
		"\u0234\7\u00df\2\2\u022b\u022c\7\u008f\2\2\u022c\u0231\5> \2\u022d\u022e"+
		"\7\5\2\2\u022e\u0230\5> \2\u022f\u022d\3\2\2\2\u0230\u0233\3\2\2\2\u0231"+
		"\u022f\3\2\2\2\u0231\u0232\3\2\2\2\u0232\u0235\3\2\2\2\u0233\u0231\3\2"+
		"\2\2\u0234\u022b\3\2\2\2\u0234\u0235\3\2\2\2\u0235\u0301\3\2\2\2\u0236"+
		"\u0238\7]\2\2\u0237\u0239\7\u00a5\2\2\u0238\u0237\3\2\2\2\u0238\u0239"+
		"\3\2\2\2\u0239\u023a\3\2\2\2\u023a\u023d\7\u009d\2\2\u023b\u023c\7r\2"+
		"\2\u023c\u023e\7 \2\2\u023d\u023b\3\2\2\2\u023d\u023e\3\2\2\2\u023e\u023f"+
		"\3\2\2\2\u023f\u0301\5\u00a6T\2\u0240\u0242\7Q\2\2\u0241\u0243\t\4\2\2"+
		"\u0242\u0241\3\2\2\2\u0242\u0243\3\2\2\2\u0243\u0244\3\2\2\2\u0244\u0301"+
		"\5\n\6\2\u0245\u0246\7V\2\2\u0246\u0249\7W\2\2\u0247\u0248\t\5\2\2\u0248"+
		"\u024a\5\u00a8U\2\u0249\u0247\3\2\2\2\u0249\u024a\3\2\2\2\u024a\u024f"+
		"\3\2\2\2\u024b\u024d\7\"\2\2\u024c\u024b\3\2\2\2\u024c\u024d\3\2\2\2\u024d"+
		"\u024e\3\2\2\2\u024e\u0250\7\u00df\2\2\u024f\u024c\3\2\2\2\u024f\u0250"+
		"\3\2\2\2\u0250\u0301\3\2\2\2\u0251\u0252\7V\2\2\u0252\u0255\7\u00bf\2"+
		"\2\u0253\u0254\7\"\2\2\u0254\u0256\7\u00df\2\2\u0255\u0253\3\2\2\2\u0255"+
		"\u0256\3\2\2\2\u0256\u0301\3\2\2\2\u0257\u0258\7V\2\2\u0258\u0259\7\u00a8"+
		"\2\2\u0259\u025e\5r:\2\u025a\u025b\7\3\2\2\u025b\u025c\5\62\32\2\u025c"+
		"\u025d\7\4\2\2\u025d\u025f\3\2\2\2\u025e\u025a\3\2\2\2\u025e\u025f\3\2"+
		"\2\2\u025f\u0301\3\2\2\2\u0260\u0261\7V\2\2\u0261\u0262\7X\2\2\u0262\u0263"+
		"\t\5\2\2\u0263\u0266\5r:\2\u0264\u0265\t\5\2\2\u0265\u0267\5\u00a8U\2"+
		"\u0266\u0264\3\2\2\2\u0266\u0267\3\2\2\2\u0267\u0301\3\2\2\2\u0268\u0269"+
		"\7V\2\2\u0269\u026a\7[\2\2\u026a\u026c\5r:\2\u026b\u026d\5 \21\2\u026c"+
		"\u026b\3\2\2\2\u026c\u026d\3\2\2\2\u026d\u0301\3\2\2\2\u026e\u0270\7V"+
		"\2\2\u026f\u0271\5\u00a8U\2\u0270\u026f\3\2\2\2\u0270\u0271\3\2\2\2\u0271"+
		"\u0272\3\2\2\2\u0272\u027a\7\\\2\2\u0273\u0275\7\"\2\2\u0274\u0273\3\2"+
		"\2\2\u0274\u0275\3\2\2\2\u0275\u0278\3\2\2\2\u0276\u0279\5\u00a6T\2\u0277"+
		"\u0279\7\u00df\2\2\u0278\u0276\3\2\2\2\u0278\u0277\3\2\2\2\u0279\u027b"+
		"\3\2\2\2\u027a\u0274\3\2\2\2\u027a\u027b\3\2\2\2\u027b\u0301\3\2\2\2\u027c"+
		"\u027d\7V\2\2\u027d\u027e\7I\2\2\u027e\u027f\7J\2\2\u027f\u0301\5r:\2"+
		"\u0280\u0281\t\6\2\2\u0281\u0283\7\u009d\2\2\u0282\u0284\7\u009e\2\2\u0283"+
		"\u0282\3\2\2\2\u0283\u0284\3\2\2\2\u0284\u0285\3\2\2\2\u0285\u0301\5$"+
		"\23\2\u0286\u0287\t\6\2\2\u0287\u0289\7\u00be\2\2\u0288\u028a\7\u009e"+
		"\2\2\u0289\u0288\3\2\2\2\u0289\u028a\3\2\2\2\u028a\u028b\3\2\2\2\u028b"+
		"\u0301\5\u00a8U\2\u028c\u028e\t\6\2\2\u028d\u028f\t\7\2\2\u028e\u028d"+
		"\3\2\2\2\u028e\u028f\3\2\2\2\u028f\u0290\3\2\2\2\u0290\u0292\5r:\2\u0291"+
		"\u0293\5 \21\2\u0292\u0291\3\2\2\2\u0292\u0293\3\2\2\2\u0293\u0295\3\2"+
		"\2\2\u0294\u0296\5&\24\2\u0295\u0294\3\2\2\2\u0295\u0296\3\2\2\2\u0296"+
		"\u0301\3\2\2\2\u0297\u0298\7\u009f\2\2\u0298\u0299\7J\2\2\u0299\u0301"+
		"\5r:\2\u029a\u029b\7\u009f\2\2\u029b\u029c\7\u00de\2\2\u029c\u029d\7;"+
		"\2\2\u029d\u0301\5r:\2\u029e\u02a2\7\u009f\2\2\u029f\u02a1\13\2\2\2\u02a0"+
		"\u029f\3\2\2\2\u02a1\u02a4\3\2\2\2\u02a2\u02a3\3\2\2\2\u02a2\u02a0\3\2"+
		"\2\2\u02a3\u0301\3\2\2\2\u02a4\u02a2\3\2\2\2\u02a5\u02a7\7\u00a1\2\2\u02a6"+
		"\u02a8\7\u00a3\2\2\u02a7\u02a6\3\2\2\2\u02a7\u02a8\3\2\2\2\u02a8\u02a9"+
		"\3\2\2\2\u02a9\u02aa\7J\2\2\u02aa\u02af\5r:\2\u02ab\u02ad\7\r\2\2\u02ac"+
		"\u02ab\3\2\2\2\u02ac\u02ad\3\2\2\2\u02ad\u02ae\3\2\2\2\u02ae\u02b0\5\32"+
		"\16\2\u02af\u02ac\3\2\2\2\u02af\u02b0\3\2\2\2\u02b0\u0301\3\2\2\2\u02b1"+
		"\u02b2\7\u00a2\2\2\u02b2\u02b3\7J\2\2\u02b3\u0301\5r:\2\u02b4\u02b5\7"+
		"\u00a0\2\2\u02b5\u0301\7\u00a1\2\2\u02b6\u02b7\7\u00d1\2\2\u02b7\u02b9"+
		"\7l\2\2\u02b8\u02ba\7\u00dc\2\2\u02b9\u02b8\3\2\2\2\u02b9\u02ba\3\2\2"+
		"\2\u02ba\u02bb\3\2\2\2\u02bb\u02bc\7\u00dd\2\2\u02bc\u02be\7\u00df\2\2"+
		"\u02bd\u02bf\7\u008c\2\2\u02be\u02bd\3\2\2\2\u02be\u02bf\3\2\2\2\u02bf"+
		"\u02c0\3\2\2\2\u02c0\u02c1\7O\2\2\u02c1\u02c2\7J\2\2\u02c2\u02c4\5r:\2"+
		"\u02c3\u02c5\5 \21\2\u02c4\u02c3\3\2\2\2\u02c4\u02c5\3\2\2\2\u02c5\u0301"+
		"\3\2\2\2\u02c6\u02c7\7\u00c1\2\2\u02c7\u02c8\7J\2\2\u02c8\u02ca\5r:\2"+
		"\u02c9\u02cb\5 \21\2\u02ca\u02c9\3\2\2\2\u02ca\u02cb\3\2\2\2\u02cb\u0301"+
		"\3\2\2\2\u02cc\u02cd\t\b\2\2\u02cd\u02d1\5\u00a8U\2\u02ce\u02d0\13\2\2"+
		"\2\u02cf\u02ce\3\2\2\2\u02d0\u02d3\3\2\2\2\u02d1\u02d2\3\2\2\2\u02d1\u02cf"+
		"\3\2\2\2\u02d2\u0301\3\2\2\2\u02d3\u02d1\3\2\2\2\u02d4\u02d5\7j\2\2\u02d5"+
		"\u02d9\7\u00d2\2\2\u02d6\u02d8\13\2\2\2\u02d7\u02d6\3\2\2\2\u02d8\u02db"+
		"\3\2\2\2\u02d9\u02da\3\2\2\2\u02d9\u02d7\3\2\2\2\u02da\u0301\3\2\2\2\u02db"+
		"\u02d9\3\2\2\2\u02dc\u02e0\7j\2\2\u02dd\u02df\13\2\2\2\u02de\u02dd\3\2"+
		"\2\2\u02df\u02e2\3\2\2\2\u02e0\u02e1\3\2\2\2\u02e0\u02de\3\2\2\2\u02e1"+
		"\u0301\3\2\2\2\u02e2\u02e0\3\2\2\2\u02e3\u0301\7k\2\2\u02e4\u02e5\7I\2"+
		"\2\u02e5\u02e9\7\u00de\2\2\u02e6\u02e7\7r\2\2\u02e7\u02e8\7\36\2\2\u02e8"+
		"\u02ea\7 \2\2\u02e9\u02e6\3\2\2\2\u02e9\u02ea\3\2\2\2\u02ea\u02eb\3\2"+
		"\2\2\u02eb\u02ec\7\u00e8\2\2\u02ec\u02ed\7;\2\2\u02ed\u02ee\5r:\2\u02ee"+
		"\u02ef\5\f\7\2\u02ef\u0301\3\2\2\2\u02f0\u02f1\7]\2\2\u02f1\u02f4\7\u00de"+
		"\2\2\u02f2\u02f3\7r\2\2\u02f3\u02f5\7 \2\2\u02f4\u02f2\3\2\2\2\u02f4\u02f5"+
		"\3\2\2\2\u02f5\u02f6\3\2\2\2\u02f6\u02f7\7\u00e8\2\2\u02f7\u02f8\7;\2"+
		"\2\u02f8\u0301\5r:\2\u02f9\u02fd\5\20\t\2\u02fa\u02fc\13\2\2\2\u02fb\u02fa"+
		"\3\2\2\2\u02fc\u02ff\3\2\2\2\u02fd\u02fe\3\2\2\2\u02fd\u02fb\3\2\2\2\u02fe"+
		"\u0301\3\2\2\2\u02ff\u02fd\3\2\2\2\u0300\u00be\3\2\2\2\u0300\u00bf\3\2"+
		"\2\2\u0300\u00c1\3\2\2\2\u0300\u00d5\3\2\2\2\u0300\u00dc\3\2\2\2\u0300"+
		"\u00e6\3\2\2\2\u0300\u00fa\3\2\2\2\u0300\u010d\3\2\2\2\u0300\u0139\3\2"+
		"\2\2\u0300\u0144\3\2\2\2\u0300\u0154\3\2\2\2\u0300\u015b\3\2\2\2\u0300"+
		"\u0162\3\2\2\2\u0300\u016d\3\2\2\2\u0300\u017b\3\2\2\2\u0300\u0185\3\2"+
		"\2\2\u0300\u0193\3\2\2\2\u0300\u01a1\3\2\2\2\u0300\u01a9\3\2\2\2\u0300"+
		"\u01bc\3\2\2\2\u0300\u01cc\3\2\2\2\u0300\u01d5\3\2\2\2\u0300\u01df\3\2"+
		"\2\2\u0300\u01e6\3\2\2\2\u0300\u0208\3\2\2\2\u0300\u021b\3\2\2\2\u0300"+
		"\u0223\3\2\2\2\u0300\u0236\3\2\2\2\u0300\u0240\3\2\2\2\u0300\u0245\3\2"+
		"\2\2\u0300\u0251\3\2\2\2\u0300\u0257\3\2\2\2\u0300\u0260\3\2\2\2\u0300"+
		"\u0268\3\2\2\2\u0300\u026e\3\2\2\2\u0300\u027c\3\2\2\2\u0300\u0280\3\2"+
		"\2\2\u0300\u0286\3\2\2\2\u0300\u028c\3\2\2\2\u0300\u0297\3\2\2\2\u0300"+
		"\u029a\3\2\2\2\u0300\u029e\3\2\2\2\u0300\u02a5\3\2\2\2\u0300\u02b1\3\2"+
		"\2\2\u0300\u02b4\3\2\2\2\u0300\u02b6\3\2\2\2\u0300\u02c6\3\2\2\2\u0300"+
		"\u02cc\3\2\2\2\u0300\u02d4\3\2\2\2\u0300\u02dc\3\2\2\2\u0300\u02e3\3\2"+
		"\2\2\u0300\u02e4\3\2\2\2\u0300\u02f0\3\2\2\2\u0300\u02f9\3\2\2\2\u0301"+
		"\13\3\2\2\2\u0302\u0303\7\3\2\2\u0303\u0308\5\16\b\2\u0304\u0305\7\5\2"+
		"\2\u0305\u0307\5\16\b\2\u0306\u0304\3\2\2\2\u0307\u030a\3\2\2\2\u0308"+
		"\u0306\3\2\2\2\u0308\u0309\3\2\2\2\u0309\u030b\3\2\2\2\u030a\u0308\3\2"+
		"\2\2\u030b\u030c\7\4\2\2\u030c\r\3\2\2\2\u030d\u030f\5\u00a8U\2\u030e"+
		"\u0310\t\t\2\2\u030f\u030e\3\2\2\2\u030f\u0310\3\2\2\2\u0310\17\3\2\2"+
		"\2\u0311\u0312\7I\2\2\u0312\u03d2\7\u00d2\2\2\u0313\u0314\7]\2\2\u0314"+
		"\u03d2\7\u00d2\2\2\u0315\u0317\7\u00ca\2\2\u0316\u0318\7\u00d2\2\2\u0317"+
		"\u0316\3\2\2\2\u0317\u0318\3\2\2\2\u0318\u03d2\3\2\2\2\u0319\u031b\7\u00c9"+
		"\2\2\u031a\u031c\7\u00d2\2\2\u031b\u031a\3\2\2\2\u031b\u031c\3\2\2\2\u031c"+
		"\u03d2\3\2\2\2\u031d\u031e\7V\2\2\u031e\u03d2\7\u00ca\2\2\u031f\u0320"+
		"\7V\2\2\u0320\u0322\7\u00d2\2\2\u0321\u0323\7\u00ca\2\2\u0322\u0321\3"+
		"\2\2\2\u0322\u0323\3\2\2\2\u0323\u03d2\3\2\2\2\u0324\u0325\7V\2\2\u0325"+
		"\u03d2\7\u00d5\2\2\u0326\u0327\7V\2\2\u0327\u03d2\7\u00d3\2\2\u0328\u0329"+
		"\7V\2\2\u0329\u032a\7E\2\2\u032a\u03d2\7\u00d3\2\2\u032b\u032c\7\u00cf"+
		"\2\2\u032c\u03d2\7J\2\2\u032d\u032e\7\u00d0\2\2\u032e\u03d2\7J\2\2\u032f"+
		"\u0330\7V\2\2\u0330\u03d2\7\u00d4\2\2\u0331\u0332\7V\2\2\u0332\u0333\7"+
		"I\2\2\u0333\u03d2\7J\2\2\u0334\u0335\7V\2\2\u0335\u03d2\7\u00d6\2\2\u0336"+
		"\u0337\7V\2\2\u0337\u03d2\7\u00d8\2\2\u0338\u0339\7V\2\2\u0339\u03d2\7"+
		"\u00d9\2\2\u033a\u033b\7I\2\2\u033b\u03d2\7\u00d7\2\2\u033c\u033d\7]\2"+
		"\2\u033d\u03d2\7\u00d7\2\2\u033e\u033f\7d\2\2\u033f\u03d2\7\u00d7\2\2"+
		"\u0340\u0341\7\u00cb\2\2\u0341\u03d2\7J\2\2\u0342\u0343\7\u00cb\2\2\u0343"+
		"\u03d2\7\u00be\2\2\u0344\u0345\7\u00cc\2\2\u0345\u03d2\7J\2\2\u0346\u0347"+
		"\7\u00cc\2\2\u0347\u03d2\7\u00be\2\2\u0348\u0349\7I\2\2\u0349\u034a\7"+
		"\u00a5\2\2\u034a\u03d2\7q\2\2\u034b\u034c\7]\2\2\u034c\u034d\7\u00a5\2"+
		"\2\u034d\u03d2\7q\2\2\u034e\u034f\7\u00cd\2\2\u034f\u0350\7\u00ce\2\2"+
		"\u0350\u03d2\7J\2\2\u0351\u0352\7d\2\2\u0352\u0353\7J\2\2\u0353\u0354"+
		"\5r:\2\u0354\u0355\7\36\2\2\u0355\u0356\7\u00b9\2\2\u0356\u03d2\3\2\2"+
		"\2\u0357\u0358\7d\2\2\u0358\u0359\7J\2\2\u0359\u035a\5r:\2\u035a\u035b"+
		"\7\u00b9\2\2\u035b\u035c\7\22\2\2\u035c\u03d2\3\2\2\2\u035d\u035e\7d\2"+
		"\2\u035e\u035f\7J\2\2\u035f\u0360\5r:\2\u0360\u0361\7\36\2\2\u0361\u0362"+
		"\7\u00ba\2\2\u0362\u03d2\3\2\2\2\u0363\u0364\7d\2\2\u0364\u0365\7J\2\2"+
		"\u0365\u0366\5r:\2\u0366\u0367\7\u00ab\2\2\u0367\u0368\7\22\2\2\u0368"+
		"\u03d2\3\2\2\2\u0369\u036a\7d\2\2\u036a\u036b\7J\2\2\u036b\u036c\5r:\2"+
		"\u036c\u036d\7\36\2\2\u036d\u036e\7\u00ab\2\2\u036e\u03d2\3\2\2\2\u036f"+
		"\u0370\7d\2\2\u0370\u0371\7J\2\2\u0371\u0372\5r:\2\u0372\u0373\7\36\2"+
		"\2\u0373\u0374\7\u00ac\2\2\u0374\u0375\7\r\2\2\u0375\u0376\7\u00ad\2\2"+
		"\u0376\u03d2\3\2\2\2\u0377\u0378\7d\2\2\u0378\u0379\7J\2\2\u0379\u037a"+
		"\5r:\2\u037a\u037b\7j\2\2\u037b\u037c\7\u00ab\2\2\u037c\u037d\7\u00ae"+
		"\2\2\u037d\u03d2\3\2\2\2\u037e\u037f\7d\2\2\u037f\u0380\7J\2\2\u0380\u0381"+
		"\5r:\2\u0381\u0382\7\u00af\2\2\u0382\u0383\7?\2\2\u0383\u03d2\3\2\2\2"+
		"\u0384\u0385\7d\2\2\u0385\u0386\7J\2\2\u0386\u0387\5r:\2\u0387\u0388\7"+
		"\u00b0\2\2\u0388\u0389\7?\2\2\u0389\u03d2\3\2\2\2\u038a\u038b\7d\2\2\u038b"+
		"\u038c\7J\2\2\u038c\u038d\5r:\2\u038d\u038e\7\u00b1\2\2\u038e\u038f\7"+
		"?\2\2\u038f\u03d2\3\2\2\2\u0390\u0391\7d\2\2\u0391\u0392\7J\2\2\u0392"+
		"\u0393\5r:\2\u0393\u0394\7\u00b3\2\2\u0394\u03d2\3\2\2\2\u0395\u0396\7"+
		"d\2\2\u0396\u0397\7J\2\2\u0397\u0399\5r:\2\u0398\u039a\5 \21\2\u0399\u0398"+
		"\3\2\2\2\u0399\u039a\3\2\2\2\u039a\u039b\3\2\2\2\u039b\u039c\7\u00b4\2"+
		"\2\u039c\u03d2\3\2\2\2\u039d\u039e\7d\2\2\u039e\u039f\7J\2\2\u039f\u03a1"+
		"\5r:\2\u03a0\u03a2\5 \21\2\u03a1\u03a0\3\2\2\2\u03a1\u03a2\3\2\2\2\u03a2"+
		"\u03a3\3\2\2\2\u03a3\u03a4\7\u00b5\2\2\u03a4\u03d2\3\2\2\2\u03a5\u03a6"+
		"\7d\2\2\u03a6\u03a7\7J\2\2\u03a7\u03a9\5r:\2\u03a8\u03aa\5 \21\2\u03a9"+
		"\u03a8\3\2\2\2\u03a9\u03aa\3\2\2\2\u03aa\u03ab\3\2\2\2\u03ab\u03ac\7j"+
		"\2\2\u03ac\u03ad\7\u00b2\2\2\u03ad\u03d2\3\2\2\2\u03ae\u03af\7d\2\2\u03af"+
		"\u03b0\7J\2\2\u03b0\u03b2\5r:\2\u03b1\u03b3\5 \21\2\u03b2\u03b1\3\2\2"+
		"\2\u03b2\u03b3\3\2\2\2\u03b3\u03b4\3\2\2\2\u03b4\u03b5\7\f\2\2\u03b5\u03b6"+
		"\7X\2\2\u03b6\u03d2\3\2\2\2\u03b7\u03b8\7d\2\2\u03b8\u03b9\7J\2\2\u03b9"+
		"\u03bb\5r:\2\u03ba\u03bc\5 \21\2\u03bb\u03ba\3\2\2\2\u03bb\u03bc\3\2\2"+
		"\2\u03bc\u03bd\3\2\2\2\u03bd\u03bf\7\u00b6\2\2\u03be\u03c0\7X\2\2\u03bf"+
		"\u03be\3\2\2\2\u03bf\u03c0\3\2\2\2\u03c0\u03d2\3\2\2\2\u03c1\u03c2\7d"+
		"\2\2\u03c2\u03c3\7J\2\2\u03c3\u03c5\5r:\2\u03c4\u03c6\5 \21\2\u03c5\u03c4"+
		"\3\2\2\2\u03c5\u03c6\3\2\2\2\u03c6\u03c7\3\2\2\2\u03c7\u03c8\7L\2\2\u03c8"+
		"\u03c9\7X\2\2\u03c9\u03d2\3\2\2\2\u03ca\u03cb\7m\2\2\u03cb\u03d2\7n\2"+
		"\2\u03cc\u03d2\7o\2\2\u03cd\u03d2\7p\2\2\u03ce\u03d2\7\u00c0\2\2\u03cf"+
		"\u03d0\7N\2\2\u03d0\u03d2\7\13\2\2\u03d1\u0311\3\2\2\2\u03d1\u0313\3\2"+
		"\2\2\u03d1\u0315\3\2\2\2\u03d1\u0319\3\2\2\2\u03d1\u031d\3\2\2\2\u03d1"+
		"\u031f\3\2\2\2\u03d1\u0324\3\2\2\2\u03d1\u0326\3\2\2\2\u03d1\u0328\3\2"+
		"\2\2\u03d1\u032b\3\2\2\2\u03d1\u032d\3\2\2\2\u03d1\u032f\3\2\2\2\u03d1"+
		"\u0331\3\2\2\2\u03d1\u0334\3\2\2\2\u03d1\u0336\3\2\2\2\u03d1\u0338\3\2"+
		"\2\2\u03d1\u033a\3\2\2\2\u03d1\u033c\3\2\2\2\u03d1\u033e\3\2\2\2\u03d1"+
		"\u0340\3\2\2\2\u03d1\u0342\3\2\2\2\u03d1\u0344\3\2\2\2\u03d1\u0346\3\2"+
		"\2\2\u03d1\u0348\3\2\2\2\u03d1\u034b\3\2\2\2\u03d1\u034e\3\2\2\2\u03d1"+
		"\u0351\3\2\2\2\u03d1\u0357\3\2\2\2\u03d1\u035d\3\2\2\2\u03d1\u0363\3\2"+
		"\2\2\u03d1\u0369\3\2\2\2\u03d1\u036f\3\2\2\2\u03d1\u0377\3\2\2\2\u03d1"+
		"\u037e\3\2\2\2\u03d1\u0384\3\2\2\2\u03d1\u038a\3\2\2\2\u03d1\u0390\3\2"+
		"\2\2\u03d1\u0395\3\2\2\2\u03d1\u039d\3\2\2\2\u03d1\u03a5\3\2\2\2\u03d1"+
		"\u03ae\3\2\2\2\u03d1\u03b7\3\2\2\2\u03d1\u03c1\3\2\2\2\u03d1\u03ca\3\2"+
		"\2\2\u03d1\u03cc\3\2\2\2\u03d1\u03cd\3\2\2\2\u03d1\u03ce\3\2\2\2\u03d1"+
		"\u03cf\3\2\2\2\u03d2\21\3\2\2\2\u03d3\u03d5\7I\2\2\u03d4\u03d6\7\u00a5"+
		"\2\2\u03d5\u03d4\3\2\2\2\u03d5\u03d6\3\2\2\2\u03d6\u03d8\3\2\2\2\u03d7"+
		"\u03d9\7\u00c7\2\2\u03d8\u03d7\3\2\2\2\u03d8\u03d9\3\2\2\2\u03d9\u03da"+
		"\3\2\2\2\u03da\u03de\7J\2\2\u03db\u03dc\7r\2\2\u03dc\u03dd\7\36\2\2\u03dd"+
		"\u03df\7 \2\2\u03de\u03db\3\2\2\2\u03de\u03df\3\2\2\2\u03df\u03e0\3\2"+
		"\2\2\u03e0\u03e1\5r:\2\u03e1\23\3\2\2\2\u03e2\u03e3\7\u00b9\2\2\u03e3"+
		"\u03e4\7\22\2\2\u03e4\u03e8\5`\61\2\u03e5\u03e6\7\u00ba\2\2\u03e6\u03e7"+
		"\7\22\2\2\u03e7\u03e9\5d\63\2\u03e8\u03e5\3\2\2\2\u03e8\u03e9\3\2\2\2"+
		"\u03e9\u03ea\3\2\2\2\u03ea\u03eb\7O\2\2\u03eb\u03ec\7\u00e4\2\2\u03ec"+
		"\u03ed\7\u00aa\2\2\u03ed\25\3\2\2\2\u03ee\u03ef\7\u00ab\2\2\u03ef\u03f0"+
		"\7\22\2\2\u03f0\u03f1\5`\61\2\u03f1\u03f4\7;\2\2\u03f2\u03f5\5\64\33\2"+
		"\u03f3\u03f5\5\66\34\2\u03f4\u03f2\3\2\2\2\u03f4\u03f3\3\2\2\2\u03f5\u03f9"+
		"\3\2\2\2\u03f6\u03f7\7\u00ac\2\2\u03f7\u03f8\7\r\2\2\u03f8\u03fa\7\u00ad"+
		"\2\2\u03f9\u03f6\3\2\2\2\u03f9\u03fa\3\2\2\2\u03fa\27\3\2\2\2\u03fb\u03fc"+
		"\7\u00ae\2\2\u03fc\u03fd\7\u00df\2\2\u03fd\31\3\2\2\2\u03fe\u0400\5(\25"+
		"\2\u03ff\u03fe\3\2\2\2\u03ff\u0400\3\2\2\2\u0400\u0401\3\2\2\2\u0401\u0402"+
		"\5@!\2\u0402\33\3\2\2\2\u0403\u0404\7M\2\2\u0404\u0405\7\u008c\2\2\u0405"+
		"\u0406\7J\2\2\u0406\u040d\5r:\2\u0407\u040b\5 \21\2\u0408\u0409\7r\2\2"+
		"\u0409\u040a\7\36\2\2\u040a\u040c\7 \2\2\u040b\u0408\3\2\2\2\u040b\u040c"+
		"\3\2\2\2\u040c\u040e\3\2\2\2\u040d\u0407\3\2\2\2\u040d\u040e\3\2\2\2\u040e"+
		"\u0419\3\2\2\2\u040f\u0410\7M\2\2\u0410\u0412\7O\2\2\u0411\u0413\7J\2"+
		"\2\u0412\u0411\3\2\2\2\u0412\u0413\3\2\2\2\u0413\u0414\3\2\2\2\u0414\u0416"+
		"\5r:\2\u0415\u0417\5 \21\2\u0416\u0415\3\2\2\2\u0416\u0417\3\2\2\2\u0417"+
		"\u0419\3\2\2\2\u0418\u0403\3\2\2\2\u0418\u040f\3\2\2\2\u0419\35\3\2\2"+
		"\2\u041a\u041c\5 \21\2\u041b\u041d\5\30\r\2\u041c\u041b\3\2\2\2\u041c"+
		"\u041d\3\2\2\2\u041d\37\3\2\2\2\u041e\u041f\7?\2\2\u041f\u0420\7\3\2\2"+
		"\u0420\u0425\5\"\22\2\u0421\u0422\7\5\2\2\u0422\u0424\5\"\22\2\u0423\u0421"+
		"\3\2\2\2\u0424\u0427\3\2\2\2\u0425\u0423\3\2\2\2\u0425\u0426\3\2\2\2\u0426"+
		"\u0428\3\2\2\2\u0427\u0425\3\2\2\2\u0428\u0429\7\4\2\2\u0429!\3\2\2\2"+
		"\u042a\u042d\5\u00a8U\2\u042b\u042c\7s\2\2\u042c\u042e\5\u0084C\2\u042d"+
		"\u042b\3\2\2\2\u042d\u042e\3\2\2\2\u042e#\3\2\2\2\u042f\u0435\5\u00a6"+
		"T\2\u0430\u0435\7\u00df\2\2\u0431\u0435\5\u0086D\2\u0432\u0435\5\u0088"+
		"E\2\u0433\u0435\5\u008aF\2\u0434\u042f\3\2\2\2\u0434\u0430\3\2\2\2\u0434"+
		"\u0431\3\2\2\2\u0434\u0432\3\2\2\2\u0434\u0433\3\2\2\2\u0435%\3\2\2\2"+
		"\u0436\u043e\5\u00a8U\2\u0437\u043a\7\6\2\2\u0438\u043b\5\u00a8U\2\u0439"+
		"\u043b\7\u00df\2\2\u043a\u0438\3\2\2\2\u043a\u0439\3\2\2\2\u043b\u043d"+
		"\3\2\2\2\u043c\u0437\3\2\2\2\u043d\u0440\3\2\2\2\u043e\u043c\3\2\2\2\u043e"+
		"\u043f\3\2\2\2\u043f\'\3\2\2\2\u0440\u043e\3\2\2\2\u0441\u0442\7G\2\2"+
		"\u0442\u0447\5*\26\2\u0443\u0444\7\5\2\2\u0444\u0446\5*\26\2\u0445\u0443"+
		"\3\2\2\2\u0446\u0449\3\2\2\2\u0447\u0445\3\2\2\2\u0447\u0448\3\2\2\2\u0448"+
		")\3\2\2\2\u0449\u0447\3\2\2\2\u044a\u044c\5\u00a8U\2\u044b\u044d\7\r\2"+
		"\2\u044c\u044b\3\2\2\2\u044c\u044d\3\2\2\2\u044d\u044e\3\2\2\2\u044e\u044f"+
		"\7\3\2\2\u044f\u0450\5@!\2\u0450\u0451\7\4\2\2\u0451+\3\2\2\2\u0452\u0453"+
		"\7\u008f\2\2\u0453\u0454\5\u00a6T\2\u0454-\3\2\2\2\u0455\u0456\7\3\2\2"+
		"\u0456\u045b\5\60\31\2\u0457\u0458\7\5\2\2\u0458\u045a\5\60\31\2\u0459"+
		"\u0457\3\2\2\2\u045a\u045d\3\2\2\2\u045b\u0459\3\2\2\2\u045b\u045c\3\2"+
		"\2\2\u045c\u045e\3\2\2\2\u045d\u045b\3\2\2\2\u045e\u045f\7\4\2\2\u045f"+
		"/\3\2\2\2\u0460\u0465\5\62\32\2\u0461\u0463\7s\2\2\u0462\u0461\3\2\2\2"+
		"\u0462\u0463\3\2\2\2\u0463\u0464\3\2\2\2\u0464\u0466\7\u00df\2\2\u0465"+
		"\u0462\3\2\2\2\u0465\u0466\3\2\2\2\u0466\61\3\2\2\2\u0467\u046c\5\u00a8"+
		"U\2\u0468\u0469\7\6\2\2\u0469\u046b\5\u00a8U\2\u046a\u0468\3\2\2\2\u046b"+
		"\u046e\3\2\2\2\u046c\u046a\3\2\2\2\u046c\u046d\3\2\2\2\u046d\u0471\3\2"+
		"\2\2\u046e\u046c\3\2\2\2\u046f\u0471\7\u00df\2\2\u0470\u0467\3\2\2\2\u0470"+
		"\u046f\3\2\2\2\u0471\63\3\2\2\2\u0472\u0473\7\3\2\2\u0473\u0478\5\u0084"+
		"C\2\u0474\u0475\7\5\2\2\u0475\u0477\5\u0084C\2\u0476\u0474\3\2\2\2\u0477"+
		"\u047a\3\2\2\2\u0478\u0476\3\2\2\2\u0478\u0479\3\2\2\2\u0479\u047b\3\2"+
		"\2\2\u047a\u0478\3\2\2\2\u047b\u047c\7\4\2\2\u047c\65\3\2\2\2\u047d\u047e"+
		"\7\3\2\2\u047e\u0483\5\64\33\2\u047f\u0480\7\5\2\2\u0480\u0482\5\64\33"+
		"\2\u0481\u047f\3\2\2\2\u0482\u0485\3\2\2\2\u0483\u0481\3\2\2\2\u0483\u0484"+
		"\3\2\2\2\u0484\u0486\3\2\2\2\u0485\u0483\3\2\2\2\u0486\u0487\7\4\2\2\u0487"+
		"\67\3\2\2\2\u0488\u0489\7\u00ac\2\2\u0489\u048a\7\r\2\2\u048a\u048f\5"+
		":\36\2\u048b\u048c\7\u00ac\2\2\u048c\u048d\7\22\2\2\u048d\u048f\5<\37"+
		"\2\u048e\u0488\3\2\2\2\u048e\u048b\3\2\2\2\u048f9\3\2\2\2\u0490\u0491"+
		"\7\u00bc\2\2\u0491\u0492\7\u00df\2\2\u0492\u0493\7\u00bd\2\2\u0493\u0496"+
		"\7\u00df\2\2\u0494\u0496\5\u00a8U\2\u0495\u0490\3\2\2\2\u0495\u0494\3"+
		"\2\2\2\u0496;\3\2\2\2\u0497\u049b\7\u00df\2\2\u0498\u0499\7G\2\2\u0499"+
		"\u049a\7\u0091\2\2\u049a\u049c\5.\30\2\u049b\u0498\3\2\2\2\u049b\u049c"+
		"\3\2\2\2\u049c=\3\2\2\2\u049d\u049e\5\u00a8U\2\u049e\u049f\7\u00df\2\2"+
		"\u049f?\3\2\2\2\u04a0\u04a2\5\34\17\2\u04a1\u04a0\3\2\2\2\u04a1\u04a2"+
		"\3\2\2\2\u04a2\u04a3\3\2\2\2\u04a3\u04a4\5F$\2\u04a4\u04a5\5B\"\2\u04a5"+
		"\u04ad\3\2\2\2\u04a6\u04a8\5N(\2\u04a7\u04a9\5D#\2\u04a8\u04a7\3\2\2\2"+
		"\u04a9\u04aa\3\2\2\2\u04aa\u04a8\3\2\2\2\u04aa\u04ab\3\2\2\2\u04ab\u04ad"+
		"\3\2\2\2\u04ac\u04a1\3\2\2\2\u04ac\u04a6\3\2\2\2\u04adA\3\2\2\2\u04ae"+
		"\u04af\7\27\2\2\u04af\u04b0\7\22\2\2\u04b0\u04b5\5J&\2\u04b1\u04b2\7\5"+
		"\2\2\u04b2\u04b4\5J&\2\u04b3\u04b1\3\2\2\2\u04b4\u04b7\3\2\2\2\u04b5\u04b3"+
		"\3\2\2\2\u04b5\u04b6\3\2\2\2\u04b6\u04b9\3\2\2\2\u04b7\u04b5\3\2\2\2\u04b8"+
		"\u04ae\3\2\2\2\u04b8\u04b9\3\2\2\2\u04b9\u04c4\3\2\2\2\u04ba\u04bb\7\u008a"+
		"\2\2\u04bb\u04bc\7\22\2\2\u04bc\u04c1\5x=\2\u04bd\u04be\7\5\2\2\u04be"+
		"\u04c0\5x=\2\u04bf\u04bd\3\2\2\2\u04c0\u04c3\3\2\2\2\u04c1\u04bf\3\2\2"+
		"\2\u04c1\u04c2\3\2\2\2\u04c2\u04c5\3\2\2\2\u04c3\u04c1\3\2\2\2\u04c4\u04ba"+
		"\3\2\2\2\u04c4\u04c5\3\2\2\2\u04c5\u04d0\3\2\2\2\u04c6\u04c7\7\u008b\2"+
		"\2\u04c7\u04c8\7\22\2\2\u04c8\u04cd\5x=\2\u04c9\u04ca\7\5\2\2\u04ca\u04cc"+
		"\5x=\2\u04cb\u04c9\3\2\2\2\u04cc\u04cf\3\2\2\2\u04cd\u04cb\3\2\2\2\u04cd"+
		"\u04ce\3\2\2\2\u04ce\u04d1\3\2\2\2\u04cf\u04cd\3\2\2\2\u04d0\u04c6\3\2"+
		"\2\2\u04d0\u04d1\3\2\2\2\u04d1\u04dc\3\2\2\2\u04d2\u04d3\7\u0089\2\2\u04d3"+
		"\u04d4\7\22\2\2\u04d4\u04d9\5J&\2\u04d5\u04d6\7\5\2\2\u04d6\u04d8\5J&"+
		"\2\u04d7\u04d5\3\2\2\2\u04d8\u04db\3\2\2\2\u04d9\u04d7\3\2\2\2\u04d9\u04da"+
		"\3\2\2\2\u04da\u04dd\3\2\2\2\u04db\u04d9\3\2\2\2\u04dc\u04d2\3\2\2\2\u04dc"+
		"\u04dd\3\2\2\2\u04dd\u04df\3\2\2\2\u04de\u04e0\5\u009cO\2\u04df\u04de"+
		"\3\2\2\2\u04df\u04e0\3\2\2\2\u04e0\u04e3\3\2\2\2\u04e1\u04e2\7\31\2\2"+
		"\u04e2\u04e4\5x=\2\u04e3\u04e1\3\2\2\2\u04e3\u04e4\3\2\2\2\u04e4C\3\2"+
		"\2\2\u04e5\u04e7\5\34\17\2\u04e6\u04e5\3\2\2\2\u04e6\u04e7\3\2\2\2\u04e7"+
		"\u04e8\3\2\2\2\u04e8\u04e9\5L\'\2\u04e9\u04ea\5B\"\2\u04eaE\3\2\2\2\u04eb"+
		"\u04ec\b$\1\2\u04ec\u04ed\5H%\2\u04ed\u04f6\3\2\2\2\u04ee\u04ef\f\3\2"+
		"\2\u04ef\u04f1\t\n\2\2\u04f0\u04f2\5V,\2\u04f1\u04f0\3\2\2\2\u04f1\u04f2"+
		"\3\2\2\2\u04f2\u04f3\3\2\2\2\u04f3\u04f5\5F$\4\u04f4\u04ee\3\2\2\2\u04f5"+
		"\u04f8\3\2\2\2\u04f6\u04f4\3\2\2\2\u04f6\u04f7\3\2\2\2\u04f7G\3\2\2\2"+
		"\u04f8\u04f6\3\2\2\2\u04f9\u0502\5L\'\2\u04fa\u04fb\7J\2\2\u04fb\u0502"+
		"\5r:\2\u04fc\u0502\5n8\2\u04fd\u04fe\7\3\2\2\u04fe\u04ff\5@!\2\u04ff\u0500"+
		"\7\4\2\2\u0500\u0502\3\2\2\2\u0501\u04f9\3\2\2\2\u0501\u04fa\3\2\2\2\u0501"+
		"\u04fc\3\2\2\2\u0501\u04fd\3\2\2\2\u0502I\3\2\2\2\u0503\u0505\5x=\2\u0504"+
		"\u0506\t\t\2\2\u0505\u0504\3\2\2\2\u0505\u0506\3\2\2\2\u0506K\3\2\2\2"+
		"\u0507\u0508\7\n\2\2\u0508\u0509\7\u008d\2\2\u0509\u050a\7\3\2\2\u050a"+
		"\u050b\5v<\2\u050b\u050c\7\4\2\2\u050c\u0512\3\2\2\2\u050d\u050e\7g\2"+
		"\2\u050e\u0512\5v<\2\u050f\u0510\7\u008e\2\2\u0510\u0512\5v<\2\u0511\u0507"+
		"\3\2\2\2\u0511\u050d\3\2\2\2\u0511\u050f\3\2\2\2\u0512\u0514\3\2\2\2\u0513"+
		"\u0515\5p9\2\u0514\u0513\3\2\2\2\u0514\u0515\3\2\2\2\u0515\u0518\3\2\2"+
		"\2\u0516\u0517\7\u0093\2\2\u0517\u0519\7\u00df\2\2\u0518\u0516\3\2\2\2"+
		"\u0518\u0519\3\2\2\2\u0519\u051a\3\2\2\2\u051a\u051b\7\u008f\2\2\u051b"+
		"\u0528\7\u00df\2\2\u051c\u0526\7\r\2\2\u051d\u0527\5b\62\2\u051e\u0527"+
		"\5\u0096L\2\u051f\u0522\7\3\2\2\u0520\u0523\5b\62\2\u0521\u0523\5\u0096"+
		"L\2\u0522\u0520\3\2\2\2\u0522\u0521\3\2\2\2\u0523\u0524\3\2\2\2\u0524"+
		"\u0525\7\4\2\2\u0525\u0527\3\2\2\2\u0526\u051d\3\2\2\2\u0526\u051e\3\2"+
		"\2\2\u0526\u051f\3\2\2\2\u0527\u0529\3\2\2\2\u0528\u051c\3\2\2\2\u0528"+
		"\u0529\3\2\2\2\u0529\u052b\3\2\2\2\u052a\u052c\5p9\2\u052b\u052a\3\2\2"+
		"\2\u052b\u052c\3\2\2\2\u052c\u052f\3\2\2\2\u052d\u052e\7\u0092\2\2\u052e"+
		"\u0530\7\u00df\2\2\u052f\u052d\3\2\2\2\u052f\u0530\3\2\2\2\u0530\u0532"+
		"\3\2\2\2\u0531\u0533\5N(\2\u0532\u0531\3\2\2\2\u0532\u0533\3\2\2\2\u0533"+
		"\u0536\3\2\2\2\u0534\u0535\7\20\2\2\u0535\u0537\5z>\2\u0536\u0534\3\2"+
		"\2\2\u0536\u0537\3\2\2\2\u0537\u055f\3\2\2\2\u0538\u053a\7\n\2\2\u0539"+
		"\u053b\5V,\2\u053a\u0539\3\2\2\2\u053a\u053b\3\2\2\2\u053b\u053c\3\2\2"+
		"\2\u053c\u053e\5v<\2\u053d\u053f\5N(\2\u053e\u053d\3\2\2\2\u053e\u053f"+
		"\3\2\2\2\u053f\u0549\3\2\2\2\u0540\u0546\5N(\2\u0541\u0543\7\n\2\2\u0542"+
		"\u0544\5V,\2\u0543\u0542\3\2\2\2\u0543\u0544\3\2\2\2\u0544\u0545\3\2\2"+
		"\2\u0545\u0547\5v<\2\u0546\u0541\3\2\2\2\u0546\u0547\3\2\2\2\u0547\u0549"+
		"\3\2\2\2\u0548\u0538\3\2\2\2\u0548\u0540\3\2\2\2\u0549\u054d\3\2\2\2\u054a"+
		"\u054c\5T+\2\u054b\u054a\3\2\2\2\u054c\u054f\3\2\2\2\u054d\u054b\3\2\2"+
		"\2\u054d\u054e\3\2\2\2\u054e\u0552\3\2\2\2\u054f\u054d\3\2\2\2\u0550\u0551"+
		"\7\20\2\2\u0551\u0553\5z>\2\u0552\u0550\3\2\2\2\u0552\u0553\3\2\2\2\u0553"+
		"\u0555\3\2\2\2\u0554\u0556\5P)\2\u0555\u0554\3\2\2\2\u0555\u0556\3\2\2"+
		"\2\u0556\u0559\3\2\2\2\u0557\u0558\7\30\2\2\u0558\u055a\5z>\2\u0559\u0557"+
		"\3\2\2\2\u0559\u055a\3\2\2\2\u055a\u055c\3\2\2\2\u055b\u055d\5\u009cO"+
		"\2\u055c\u055b\3\2\2\2\u055c\u055d\3\2\2\2\u055d\u055f\3\2\2\2\u055e\u0511"+
		"\3\2\2\2\u055e\u0548\3\2\2\2\u055fM\3\2\2\2\u0560\u0561\7\13\2\2\u0561"+
		"\u0566\5X-\2\u0562\u0563\7\5\2\2\u0563\u0565\5X-\2\u0564\u0562\3\2\2\2"+
		"\u0565\u0568\3\2\2\2\u0566\u0564\3\2\2\2\u0566\u0567\3\2\2\2\u0567\u056c"+
		"\3\2\2\2\u0568\u0566\3\2\2\2\u0569\u056b\5T+\2\u056a\u0569\3\2\2\2\u056b"+
		"\u056e\3\2\2\2\u056c\u056a\3\2\2\2\u056c\u056d\3\2\2\2\u056dO\3\2\2\2"+
		"\u056e\u056c\3\2\2\2\u056f\u0570\7\21\2\2\u0570\u0571\7\22\2\2\u0571\u0576"+
		"\5x=\2\u0572\u0573\7\5\2\2\u0573\u0575\5x=\2\u0574\u0572\3\2\2\2\u0575"+
		"\u0578\3\2\2\2\u0576\u0574\3\2\2\2\u0576\u0577\3\2\2\2\u0577\u058a\3\2"+
		"\2\2\u0578\u0576\3\2\2\2\u0579\u057a\7G\2\2\u057a\u058b\7\26\2\2\u057b"+
		"\u057c\7G\2\2\u057c\u058b\7\25\2\2\u057d\u057e\7\23\2\2\u057e\u057f\7"+
		"\24\2\2\u057f\u0580\7\3\2\2\u0580\u0585\5R*\2\u0581\u0582\7\5\2\2\u0582"+
		"\u0584\5R*\2\u0583\u0581\3\2\2\2\u0584\u0587\3\2\2\2\u0585\u0583\3\2\2"+
		"\2\u0585\u0586\3\2\2\2\u0586\u0588\3\2\2\2\u0587\u0585\3\2\2\2\u0588\u0589"+
		"\7\4\2\2\u0589\u058b\3\2\2\2\u058a\u0579\3\2\2\2\u058a\u057b\3\2\2\2\u058a"+
		"\u057d\3\2\2\2\u058a\u058b\3\2\2\2\u058bQ\3\2\2\2\u058c\u0595\7\3\2\2"+
		"\u058d\u0592\5x=\2\u058e\u058f\7\5\2\2\u058f\u0591\5x=\2\u0590\u058e\3"+
		"\2\2\2\u0591\u0594\3\2\2\2\u0592\u0590\3\2\2\2\u0592\u0593\3\2\2\2\u0593"+
		"\u0596\3\2\2\2\u0594\u0592\3\2\2\2\u0595\u058d\3\2\2\2\u0595\u0596\3\2"+
		"\2\2\u0596\u0597\3\2\2\2\u0597\u059a\7\4\2\2\u0598\u059a\5x=\2\u0599\u058c"+
		"\3\2\2\2\u0599\u0598\3\2\2\2\u059aS\3\2\2\2\u059b\u059c\7<\2\2\u059c\u059e"+
		"\7K\2\2\u059d\u059f\7\64\2\2\u059e\u059d\3\2\2\2\u059e\u059f\3\2\2\2\u059f"+
		"\u05a0\3\2\2\2\u05a0\u05a1\5\u00a6T\2\u05a1\u05aa\7\3\2\2\u05a2\u05a7"+
		"\5x=\2\u05a3\u05a4\7\5\2\2\u05a4\u05a6\5x=\2\u05a5\u05a3\3\2\2\2\u05a6"+
		"\u05a9\3\2\2\2\u05a7\u05a5\3\2\2\2\u05a7\u05a8\3\2\2\2\u05a8\u05ab\3\2"+
		"\2\2\u05a9\u05a7\3\2\2\2\u05aa\u05a2\3\2\2\2\u05aa\u05ab\3\2\2\2\u05ab"+
		"\u05ac\3\2\2\2\u05ac\u05ad\7\4\2\2\u05ad\u05b9\5\u00a8U\2\u05ae\u05b0"+
		"\7\r\2\2\u05af\u05ae\3\2\2\2\u05af\u05b0\3\2\2\2\u05b0\u05b1\3\2\2\2\u05b1"+
		"\u05b6\5\u00a8U\2\u05b2\u05b3\7\5\2\2\u05b3\u05b5\5\u00a8U\2\u05b4\u05b2"+
		"\3\2\2\2\u05b5\u05b8\3\2\2\2\u05b6\u05b4\3\2\2\2\u05b6\u05b7\3\2\2\2\u05b7"+
		"\u05ba\3\2\2\2\u05b8\u05b6\3\2\2\2\u05b9\u05af\3\2\2\2\u05b9\u05ba\3\2"+
		"\2\2\u05baU\3\2\2\2\u05bb\u05bc\t\13\2\2\u05bcW\3\2\2\2\u05bd\u05be\b"+
		"-\1\2\u05be\u05bf\5l\67\2\u05bf\u05d3\3\2\2\2\u05c0\u05cf\f\4\2\2\u05c1"+
		"\u05c4\7\63\2\2\u05c2\u05c4\5Z.\2\u05c3\u05c1\3\2\2\2\u05c3\u05c2\3\2"+
		"\2\2\u05c4\u05c5\3\2\2\2\u05c5\u05c6\7\62\2\2\u05c6\u05c8\5X-\2\u05c7"+
		"\u05c9\5\\/\2\u05c8\u05c7\3\2\2\2\u05c8\u05c9\3\2\2\2\u05c9\u05d0\3\2"+
		"\2\2\u05ca\u05cb\7:\2\2\u05cb\u05cc\5Z.\2\u05cc\u05cd\7\62\2\2\u05cd\u05ce"+
		"\5X-\2\u05ce\u05d0\3\2\2\2\u05cf\u05c3\3\2\2\2\u05cf\u05ca\3\2\2\2\u05d0"+
		"\u05d2\3\2\2\2\u05d1\u05c0\3\2\2\2\u05d2\u05d5\3\2\2\2\u05d3\u05d1\3\2"+
		"\2\2\u05d3\u05d4\3\2\2\2\u05d4Y\3\2\2\2\u05d5\u05d3\3\2\2\2\u05d6\u05d8"+
		"\7\65\2\2\u05d7\u05d6\3\2\2\2\u05d7\u05d8\3\2\2\2\u05d8\u05ec\3\2\2\2"+
		"\u05d9\u05db\7\66\2\2\u05da\u05dc\7\64\2\2\u05db\u05da\3\2\2\2\u05db\u05dc"+
		"\3\2\2\2\u05dc\u05ec\3\2\2\2\u05dd\u05de\7\66\2\2\u05de\u05ec\7\67\2\2"+
		"\u05df\u05e1\78\2\2\u05e0\u05e2\7\64\2\2\u05e1\u05e0\3\2\2\2\u05e1\u05e2"+
		"\3\2\2\2\u05e2\u05ec\3\2\2\2\u05e3\u05e5\79\2\2\u05e4\u05e6\7\64\2\2\u05e5"+
		"\u05e4\3\2\2\2\u05e5\u05e6\3\2\2\2\u05e6\u05ec\3\2\2\2\u05e7\u05e9\7\66"+
		"\2\2\u05e8\u05e7\3\2\2\2\u05e8\u05e9\3\2\2\2\u05e9\u05ea\3\2\2\2\u05ea"+
		"\u05ec\7\u00db\2\2\u05eb\u05d7\3\2\2\2\u05eb\u05d9\3\2\2\2\u05eb\u05dd"+
		"\3\2\2\2\u05eb\u05df\3\2\2\2\u05eb\u05e3\3\2\2\2\u05eb\u05e8\3\2\2\2\u05ec"+
		"[\3\2\2\2\u05ed\u05ee\7;\2\2\u05ee\u05fc\5z>\2\u05ef\u05f0\7\u008f\2\2"+
		"\u05f0\u05f1\7\3\2\2\u05f1\u05f6\5\u00a8U\2\u05f2\u05f3\7\5\2\2\u05f3"+
		"\u05f5\5\u00a8U\2\u05f4\u05f2\3\2\2\2\u05f5\u05f8\3\2\2\2\u05f6\u05f4"+
		"\3\2\2\2\u05f6\u05f7\3\2\2\2\u05f7\u05f9\3\2\2\2\u05f8\u05f6\3\2\2\2\u05f9"+
		"\u05fa\7\4\2\2\u05fa\u05fc\3\2\2\2\u05fb\u05ed\3\2\2\2\u05fb\u05ef\3\2"+
		"\2\2\u05fc]\3\2\2\2\u05fd\u05fe\7b\2\2\u05fe\u0614\7\3\2\2\u05ff\u0600"+
		"\t\f\2\2\u0600\u0615\7\u0085\2\2\u0601\u0602\5x=\2\u0602\u0603\7A\2\2"+
		"\u0603\u0615\3\2\2\2\u0604\u0615\7\u00e3\2\2\u0605\u0606\7\u0086\2\2\u0606"+
		"\u0607\7\u00e4\2\2\u0607\u0608\7\u0087\2\2\u0608\u0609\7\u0088\2\2\u0609"+
		"\u0612\7\u00e4\2\2\u060a\u0610\7;\2\2\u060b\u0611\5\u00a8U\2\u060c\u060d"+
		"\5\u00a6T\2\u060d\u060e\7\3\2\2\u060e\u060f\7\4\2\2\u060f\u0611\3\2\2"+
		"\2\u0610\u060b\3\2\2\2\u0610\u060c\3\2\2\2\u0611\u0613\3\2\2\2\u0612\u060a"+
		"\3\2\2\2\u0612\u0613\3\2\2\2\u0613\u0615\3\2\2\2\u0614\u05ff\3\2\2\2\u0614"+
		"\u0601\3\2\2\2\u0614\u0604\3\2\2\2\u0614\u0605\3\2\2\2\u0615\u0616\3\2"+
		"\2\2\u0616\u0617\7\4\2\2\u0617_\3\2\2\2\u0618\u0619\7\3\2\2\u0619\u061a"+
		"\5b\62\2\u061a\u061b\7\4\2\2\u061ba\3\2\2\2\u061c\u0621\5\u00a8U\2\u061d"+
		"\u061e\7\5\2\2\u061e\u0620\5\u00a8U\2\u061f\u061d\3\2\2\2\u0620\u0623"+
		"\3\2\2\2\u0621\u061f\3\2\2\2\u0621\u0622\3\2\2\2\u0622c\3\2\2\2\u0623"+
		"\u0621\3\2\2\2\u0624\u0625\7\3\2\2\u0625\u062a\5f\64\2\u0626\u0627\7\5"+
		"\2\2\u0627\u0629\5f\64\2\u0628\u0626\3\2\2\2\u0629\u062c\3\2\2\2\u062a"+
		"\u0628\3\2\2\2\u062a\u062b\3\2\2\2\u062b\u062d\3\2\2\2\u062c\u062a\3\2"+
		"\2\2\u062d\u062e\7\4\2\2\u062ee\3\2\2\2\u062f\u0631\5\u00a8U\2\u0630\u0632"+
		"\t\t\2\2\u0631\u0630\3\2\2\2\u0631\u0632\3\2\2\2\u0632g\3\2\2\2\u0633"+
		"\u0634\7\3\2\2\u0634\u0639\5j\66\2\u0635\u0636\7\5\2\2\u0636\u0638\5j"+
		"\66\2\u0637\u0635\3\2\2\2\u0638\u063b\3\2\2\2\u0639\u0637\3\2\2\2\u0639"+
		"\u063a\3\2\2\2\u063a\u063c\3\2\2\2\u063b\u0639\3\2\2\2\u063c\u063d\7\4"+
		"\2\2\u063di\3\2\2\2\u063e\u0641\5\u00a8U\2\u063f\u0640\7i\2\2\u0640\u0642"+
		"\7\u00df\2\2\u0641\u063f\3\2\2\2\u0641\u0642\3\2\2\2\u0642k\3\2\2\2\u0643"+
		"\u0645\5r:\2\u0644\u0646\5^\60\2\u0645\u0644\3\2\2\2\u0645\u0646\3\2\2"+
		"\2\u0646\u064b\3\2\2\2\u0647\u0649\7\r\2\2\u0648\u0647\3\2\2\2\u0648\u0649"+
		"\3\2\2\2\u0649\u064a\3\2\2\2\u064a\u064c\5\u00aaV\2\u064b\u0648\3\2\2"+
		"\2\u064b\u064c\3\2\2\2\u064c\u0667\3\2\2\2\u064d\u064e\7\3\2\2\u064e\u064f"+
		"\5@!\2\u064f\u0651\7\4\2\2\u0650\u0652\5^\60\2\u0651\u0650\3\2\2\2\u0651"+
		"\u0652\3\2\2\2\u0652\u0657\3\2\2\2\u0653\u0655\7\r\2\2\u0654\u0653\3\2"+
		"\2\2\u0654\u0655\3\2\2\2\u0655\u0656\3\2\2\2\u0656\u0658\5\u00aaV\2\u0657"+
		"\u0654\3\2\2\2\u0657\u0658\3\2\2\2\u0658\u0667\3\2\2\2\u0659\u065a\7\3"+
		"\2\2\u065a\u065b\5X-\2\u065b\u065d\7\4\2\2\u065c\u065e\5^\60\2\u065d\u065c"+
		"\3\2\2\2\u065d\u065e\3\2\2\2\u065e\u0663\3\2\2\2\u065f\u0661\7\r\2\2\u0660"+
		"\u065f\3\2\2\2\u0660\u0661\3\2\2\2\u0661\u0662\3\2\2\2\u0662\u0664\5\u00aa"+
		"V\2\u0663\u0660\3\2\2\2\u0663\u0664\3\2\2\2\u0664\u0667\3\2\2\2\u0665"+
		"\u0667\5n8\2\u0666\u0643\3\2\2\2\u0666\u064d\3\2\2\2\u0666\u0659\3\2\2"+
		"\2\u0666\u0665\3\2\2\2\u0667m\3\2\2\2\u0668\u0669\7H\2\2\u0669\u066e\5"+
		"x=\2\u066a\u066b\7\5\2\2\u066b\u066d\5x=\2\u066c\u066a\3\2\2\2\u066d\u0670"+
		"\3\2\2\2\u066e\u066c\3\2\2\2\u066e\u066f\3\2\2\2\u066f\u0678\3\2\2\2\u0670"+
		"\u066e\3\2\2\2\u0671\u0673\7\r\2\2\u0672\u0671\3\2\2\2\u0672\u0673\3\2"+
		"\2\2\u0673\u0674\3\2\2\2\u0674\u0676\5\u00a8U\2\u0675\u0677\5`\61\2\u0676"+
		"\u0675\3\2\2\2\u0676\u0677\3\2\2\2\u0677\u0679\3\2\2\2\u0678\u0672\3\2"+
		"\2\2\u0678\u0679\3\2\2\2\u0679o\3\2\2\2\u067a\u067b\7F\2\2\u067b\u067c"+
		"\7R\2\2\u067c\u067d\7\u0090\2\2\u067d\u0681\7\u00df\2\2\u067e\u067f\7"+
		"G\2\2\u067f\u0680\7\u0091\2\2\u0680\u0682\5.\30\2\u0681\u067e\3\2\2\2"+
		"\u0681\u0682\3\2\2\2\u0682\u06ac\3\2\2\2\u0683\u0684\7F\2\2\u0684\u0685"+
		"\7R\2\2\u0685\u068f\7\u0094\2\2\u0686\u0687\7\u0095\2\2\u0687\u0688\7"+
		"\u0096\2\2\u0688\u0689\7\22\2\2\u0689\u068d\7\u00df\2\2\u068a\u068b\7"+
		"\u009a\2\2\u068b\u068c\7\22\2\2\u068c\u068e\7\u00df\2\2\u068d\u068a\3"+
		"\2\2\2\u068d\u068e\3\2\2\2\u068e\u0690\3\2\2\2\u068f\u0686\3\2\2\2\u068f"+
		"\u0690\3\2\2\2\u0690\u0696\3\2\2\2\u0691\u0692\7\u0097\2\2\u0692\u0693"+
		"\7\u0098\2\2\u0693\u0694\7\u0096\2\2\u0694\u0695\7\22\2\2\u0695\u0697"+
		"\7\u00df\2\2\u0696\u0691\3\2\2\2\u0696\u0697\3\2\2\2\u0697\u069d\3\2\2"+
		"\2\u0698\u0699\7g\2\2\u0699\u069a\7\u0099\2\2\u069a\u069b\7\u0096\2\2"+
		"\u069b\u069c\7\22\2\2\u069c\u069e\7\u00df\2\2\u069d\u0698\3\2\2\2\u069d"+
		"\u069e\3\2\2\2\u069e\u06a3\3\2\2\2\u069f\u06a0\7\u009b\2\2\u06a0\u06a1"+
		"\7\u0096\2\2\u06a1\u06a2\7\22\2\2\u06a2\u06a4\7\u00df\2\2\u06a3\u069f"+
		"\3\2\2\2\u06a3\u06a4\3\2\2\2\u06a4\u06a9\3\2\2\2\u06a5\u06a6\7%\2\2\u06a6"+
		"\u06a7\7\u00c8\2\2\u06a7\u06a8\7\r\2\2\u06a8\u06aa\7\u00df\2\2\u06a9\u06a5"+
		"\3\2\2\2\u06a9\u06aa\3\2\2\2\u06aa\u06ac\3\2\2\2\u06ab\u067a\3\2\2\2\u06ab"+
		"\u0683\3\2\2\2\u06acq\3\2\2\2\u06ad\u06ae\5\u00a8U\2\u06ae\u06af\7\6\2"+
		"\2\u06af\u06b1\3\2\2\2\u06b0\u06ad\3\2\2\2\u06b0\u06b1\3\2\2\2\u06b1\u06b2"+
		"\3\2\2\2\u06b2\u06b3\5\u00a8U\2\u06b3s\3\2\2\2\u06b4\u06bc\5x=\2\u06b5"+
		"\u06b7\7\r\2\2\u06b6\u06b5\3\2\2\2\u06b6\u06b7\3\2\2\2\u06b7\u06ba\3\2"+
		"\2\2\u06b8\u06bb\5\u00a8U\2\u06b9\u06bb\5`\61\2\u06ba\u06b8\3\2\2\2\u06ba"+
		"\u06b9\3\2\2\2\u06bb\u06bd\3\2\2\2\u06bc\u06b6\3\2\2\2\u06bc\u06bd\3\2"+
		"\2\2\u06bdu\3\2\2\2\u06be\u06c3\5t;\2\u06bf\u06c0\7\5\2\2\u06c0\u06c2"+
		"\5t;\2\u06c1\u06bf\3\2\2\2\u06c2\u06c5\3\2\2\2\u06c3\u06c1\3\2\2\2\u06c3"+
		"\u06c4\3\2\2\2\u06c4w\3\2\2\2\u06c5\u06c3\3\2\2\2\u06c6\u06c7\5z>\2\u06c7"+
		"y\3\2\2\2\u06c8\u06c9\b>\1\2\u06c9\u06ca\7\36\2\2\u06ca\u06d2\5z>\7\u06cb"+
		"\u06d2\5|?\2\u06cc\u06cd\7 \2\2\u06cd\u06ce\7\3\2\2\u06ce\u06cf\5\32\16"+
		"\2\u06cf\u06d0\7\4\2\2\u06d0\u06d2\3\2\2\2\u06d1\u06c8\3\2\2\2\u06d1\u06cb"+
		"\3\2\2\2\u06d1\u06cc\3\2\2\2\u06d2\u06db\3\2\2\2\u06d3\u06d4\f\5\2\2\u06d4"+
		"\u06d5\7\34\2\2\u06d5\u06da\5z>\6\u06d6\u06d7\f\4\2\2\u06d7\u06d8\7\33"+
		"\2\2\u06d8\u06da\5z>\5\u06d9\u06d3\3\2\2\2\u06d9\u06d6\3\2\2\2\u06da\u06dd"+
		"\3\2\2\2\u06db\u06d9\3\2\2\2\u06db\u06dc\3\2\2\2\u06dc{\3\2\2\2\u06dd"+
		"\u06db\3\2\2\2\u06de\u06e0\5\u0080A\2\u06df\u06e1\5~@\2\u06e0\u06df\3"+
		"\2\2\2\u06e0\u06e1\3\2\2\2\u06e1}\3\2\2\2\u06e2\u06e4\7\36\2\2\u06e3\u06e2"+
		"\3\2\2\2\u06e3\u06e4\3\2\2\2\u06e4\u06e5\3\2\2\2\u06e5\u06e6\7!\2\2\u06e6"+
		"\u06e7\5\u0080A\2\u06e7\u06e8\7\34\2\2\u06e8\u06e9\5\u0080A\2\u06e9\u070c"+
		"\3\2\2\2\u06ea\u06ec\7\36\2\2\u06eb\u06ea\3\2\2\2\u06eb\u06ec\3\2\2\2"+
		"\u06ec\u06ed\3\2\2\2\u06ed\u06ee\7\35\2\2\u06ee\u06ef\7\3\2\2\u06ef\u06f4"+
		"\5x=\2\u06f0\u06f1\7\5\2\2\u06f1\u06f3\5x=\2\u06f2\u06f0\3\2\2\2\u06f3"+
		"\u06f6\3\2\2\2\u06f4\u06f2\3\2\2\2\u06f4\u06f5\3\2\2\2\u06f5\u06f7\3\2"+
		"\2\2\u06f6\u06f4\3\2\2\2\u06f7\u06f8\7\4\2\2\u06f8\u070c\3\2\2\2\u06f9"+
		"\u06fb\7\36\2\2\u06fa\u06f9\3\2\2\2\u06fa\u06fb\3\2\2\2\u06fb\u06fc\3"+
		"\2\2\2\u06fc\u06fd\7\35\2\2\u06fd\u06fe\7\3\2\2\u06fe\u06ff\5\32\16\2"+
		"\u06ff\u0700\7\4\2\2\u0700\u070c\3\2\2\2\u0701\u0703\7\36\2\2\u0702\u0701"+
		"\3\2\2\2\u0702\u0703\3\2\2\2\u0703\u0704\3\2\2\2\u0704\u0705\t\r\2\2\u0705"+
		"\u070c\5\u0080A\2\u0706\u0708\7$\2\2\u0707\u0709\7\36\2\2\u0708\u0707"+
		"\3\2\2\2\u0708\u0709\3\2\2\2\u0709\u070a\3\2\2\2\u070a\u070c\7%\2\2\u070b"+
		"\u06e3\3\2\2\2\u070b\u06eb\3\2\2\2\u070b\u06fa\3\2\2\2\u070b\u0702\3\2"+
		"\2\2\u070b\u0706\3\2\2\2\u070c\177\3\2\2\2\u070d\u070e\bA\1\2\u070e\u0712"+
		"\5\u0082B\2\u070f\u0710\t\16\2\2\u0710\u0712\5\u0080A\t\u0711\u070d\3"+
		"\2\2\2\u0711\u070f\3\2\2\2\u0712\u0728\3\2\2\2\u0713\u0714\f\b\2\2\u0714"+
		"\u0715\t\17\2\2\u0715\u0727\5\u0080A\t\u0716\u0717\f\7\2\2\u0717\u0718"+
		"\t\20\2\2\u0718\u0727\5\u0080A\b\u0719\u071a\f\6\2\2\u071a\u071b\7\u0082"+
		"\2\2\u071b\u0727\5\u0080A\7\u071c\u071d\f\5\2\2\u071d\u071e\7\u0084\2"+
		"\2\u071e\u0727\5\u0080A\6\u071f\u0720\f\4\2\2\u0720\u0721\7\u0083\2\2"+
		"\u0721\u0727\5\u0080A\5\u0722\u0723\f\3\2\2\u0723\u0724\5\u0086D\2\u0724"+
		"\u0725\5\u0080A\4\u0725\u0727\3\2\2\2\u0726\u0713\3\2\2\2\u0726\u0716"+
		"\3\2\2\2\u0726\u0719\3\2\2\2\u0726\u071c\3\2\2\2\u0726\u071f\3\2\2\2\u0726"+
		"\u0722\3\2\2\2\u0727\u072a\3\2\2\2\u0728\u0726\3\2\2\2\u0728\u0729\3\2"+
		"\2\2\u0729\u0081\3\2\2\2\u072a\u0728\3\2\2\2\u072b\u072c\bB\1\2\u072c"+
		"\u077a\5\u0084C\2\u072d\u077a\7}\2\2\u072e\u072f\5\u00a6T\2\u072f\u0730"+
		"\7\6\2\2\u0730\u0731\7}\2\2\u0731\u077a\3\2\2\2\u0732\u0733\7\3\2\2\u0733"+
		"\u0736\5x=\2\u0734\u0735\7\5\2\2\u0735\u0737\5x=\2\u0736\u0734\3\2\2\2"+
		"\u0737\u0738\3\2\2\2\u0738\u0736\3\2\2\2\u0738\u0739\3\2\2\2\u0739\u073a"+
		"\3\2\2\2\u073a\u073b\7\4\2\2\u073b\u077a\3\2\2\2\u073c\u073d\5\u00a6T"+
		"\2\u073d\u0749\7\3\2\2\u073e\u0740\5V,\2\u073f\u073e\3\2\2\2\u073f\u0740"+
		"\3\2\2\2\u0740\u0741\3\2\2\2\u0741\u0746\5x=\2\u0742\u0743\7\5\2\2\u0743"+
		"\u0745\5x=\2\u0744\u0742\3\2\2\2\u0745\u0748\3\2\2\2\u0746\u0744\3\2\2"+
		"\2\u0746\u0747\3\2\2\2\u0747\u074a\3\2\2\2\u0748\u0746\3\2\2\2\u0749\u073f"+
		"\3\2\2\2\u0749\u074a\3\2\2\2\u074a\u074b\3\2\2\2\u074b\u074e\7\4\2\2\u074c"+
		"\u074d\7>\2\2\u074d\u074f\5\u00a0Q\2\u074e\u074c\3\2\2\2\u074e\u074f\3"+
		"\2\2\2\u074f\u077a\3\2\2\2\u0750\u0751\7\3\2\2\u0751\u0752\5\32\16\2\u0752"+
		"\u0753\7\4\2\2\u0753\u077a\3\2\2\2\u0754\u0755\7-\2\2\u0755\u0757\5\u0080"+
		"A\2\u0756\u0758\5\u009aN\2\u0757\u0756\3\2\2\2\u0758\u0759\3\2\2\2\u0759"+
		"\u0757\3\2\2\2\u0759\u075a\3\2\2\2\u075a\u075d\3\2\2\2\u075b\u075c\7\60"+
		"\2\2\u075c\u075e\5x=\2\u075d\u075b\3\2\2\2\u075d\u075e\3\2\2\2\u075e\u075f"+
		"\3\2\2\2\u075f\u0760\7\61\2\2\u0760\u077a\3\2\2\2\u0761\u0763\7-\2\2\u0762"+
		"\u0764\5\u009aN\2\u0763\u0762\3\2\2\2\u0764\u0765\3\2\2\2\u0765\u0763"+
		"\3\2\2\2\u0765\u0766\3\2\2\2\u0766\u0769\3\2\2\2\u0767\u0768\7\60\2\2"+
		"\u0768\u076a\5x=\2\u0769\u0767\3\2\2\2\u0769\u076a\3\2\2\2\u076a\u076b"+
		"\3\2\2\2\u076b\u076c\7\61\2\2\u076c\u077a\3\2\2\2\u076d\u076e\7U\2\2\u076e"+
		"\u076f\7\3\2\2\u076f\u0770\5x=\2\u0770\u0771\7\r\2\2\u0771\u0772\5\u0094"+
		"K\2\u0772\u0773\7\4\2\2\u0773\u077a\3\2\2\2\u0774\u077a\5\u00a8U\2\u0775"+
		"\u0776\7\3\2\2\u0776\u0777\5x=\2\u0777\u0778\7\4\2\2\u0778\u077a\3\2\2"+
		"\2\u0779\u072b\3\2\2\2\u0779\u072d\3\2\2\2\u0779\u072e\3\2\2\2\u0779\u0732"+
		"\3\2\2\2\u0779\u073c\3\2\2\2\u0779\u0750\3\2\2\2\u0779\u0754\3\2\2\2\u0779"+
		"\u0761\3\2\2\2\u0779\u076d\3\2\2\2\u0779\u0774\3\2\2\2\u0779\u0775\3\2"+
		"\2\2\u077a\u0785\3\2\2\2\u077b\u077c\f\6\2\2\u077c\u077d\7\7\2\2\u077d"+
		"\u077e\5\u0080A\2\u077e\u077f\7\b\2\2\u077f\u0784\3\2\2\2\u0780\u0781"+
		"\f\4\2\2\u0781\u0782\7\6\2\2\u0782\u0784\5\u00a8U\2\u0783\u077b\3\2\2"+
		"\2\u0783\u0780\3\2\2\2\u0784\u0787\3\2\2\2\u0785\u0783\3\2\2\2\u0785\u0786"+
		"\3\2\2\2\u0786\u0083\3\2\2\2\u0787\u0785\3\2\2\2\u0788\u0795\7%\2\2\u0789"+
		"\u0795\5\u008eH\2\u078a\u078b\5\u00a8U\2\u078b\u078c\7\u00df\2\2\u078c"+
		"\u0795\3\2\2\2\u078d\u0795\5\u00aeX\2\u078e\u0795\5\u008cG\2\u078f\u0791"+
		"\7\u00df\2\2\u0790\u078f\3\2\2\2\u0791\u0792\3\2\2\2\u0792\u0790\3\2\2"+
		"\2\u0792\u0793\3\2\2\2\u0793\u0795\3\2\2\2\u0794\u0788\3\2\2\2\u0794\u0789"+
		"\3\2\2\2\u0794\u078a\3\2\2\2\u0794\u078d\3\2\2\2\u0794\u078e\3\2\2\2\u0794"+
		"\u0790\3\2\2\2\u0795\u0085\3\2\2\2\u0796\u0797\t\21\2\2\u0797\u0087\3"+
		"\2\2\2\u0798\u0799\t\22\2\2\u0799\u0089\3\2\2\2\u079a\u079b\t\23\2\2\u079b"+
		"\u008b\3\2\2\2\u079c\u079d\t\24\2\2\u079d\u008d\3\2\2\2\u079e\u07a2\7"+
		",\2\2\u079f\u07a1\5\u0090I\2\u07a0\u079f\3\2\2\2\u07a1\u07a4\3\2\2\2\u07a2"+
		"\u07a0\3\2\2\2\u07a2\u07a3\3\2\2\2\u07a3\u008f\3\2\2\2\u07a4\u07a2\3\2"+
		"\2\2\u07a5\u07a6\5\u0092J\2\u07a6\u07a9\5\u00a8U\2\u07a7\u07a8\7a\2\2"+
		"\u07a8\u07aa\5\u00a8U\2\u07a9\u07a7\3\2\2\2\u07a9\u07aa\3\2\2\2\u07aa"+
		"\u0091\3\2\2\2\u07ab\u07ad\t\20\2\2\u07ac\u07ab\3\2\2\2\u07ac\u07ad\3"+
		"\2\2\2\u07ad\u07ae\3\2\2\2\u07ae\u07b1\t\f\2\2\u07af\u07b1\7\u00df\2\2"+
		"\u07b0\u07ac\3\2\2\2\u07b0\u07af\3\2\2\2\u07b1\u0093\3\2\2\2\u07b2\u07b3"+
		"\7f\2\2\u07b3\u07b4\7w\2\2\u07b4\u07b5\5\u0094K\2\u07b5\u07b6\7y\2\2\u07b6"+
		"\u07d5\3\2\2\2\u07b7\u07b8\7g\2\2\u07b8\u07b9\7w\2\2\u07b9\u07ba\5\u0094"+
		"K\2\u07ba\u07bb\7\5\2\2\u07bb\u07bc\5\u0094K\2\u07bc\u07bd\7y\2\2\u07bd"+
		"\u07d5\3\2\2\2\u07be\u07c5\7h\2\2\u07bf\u07c1\7w\2\2\u07c0\u07c2\5\u0096"+
		"L\2\u07c1\u07c0\3\2\2\2\u07c1\u07c2\3\2\2\2\u07c2\u07c3\3\2\2\2\u07c3"+
		"\u07c6\7y\2\2\u07c4\u07c6\7u\2\2\u07c5\u07bf\3\2\2\2\u07c5\u07c4\3\2\2"+
		"\2\u07c6\u07d5\3\2\2\2\u07c7\u07d2\5\u00a8U\2\u07c8\u07c9\7\3\2\2\u07c9"+
		"\u07ce\7\u00e4\2\2\u07ca\u07cb\7\5\2\2\u07cb\u07cd\7\u00e4\2\2\u07cc\u07ca"+
		"\3\2\2\2\u07cd\u07d0\3\2\2\2\u07ce\u07cc\3\2\2\2\u07ce\u07cf\3\2\2\2\u07cf"+
		"\u07d1\3\2\2\2\u07d0\u07ce\3\2\2\2\u07d1\u07d3\7\4\2\2\u07d2\u07c8\3\2"+
		"\2\2\u07d2\u07d3\3\2\2\2\u07d3\u07d5\3\2\2\2\u07d4\u07b2\3\2\2\2\u07d4"+
		"\u07b7\3\2\2\2\u07d4\u07be\3\2\2\2\u07d4\u07c7\3\2\2\2\u07d5\u0095\3\2"+
		"\2\2\u07d6\u07db\5\u0098M\2\u07d7\u07d8\7\5\2\2\u07d8\u07da\5\u0098M\2"+
		"\u07d9\u07d7\3\2\2\2\u07da\u07dd\3\2\2\2\u07db\u07d9\3\2\2\2\u07db\u07dc"+
		"\3\2\2\2\u07dc\u0097\3\2\2\2\u07dd\u07db\3\2\2\2\u07de\u07e0\5\u00a8U"+
		"\2\u07df\u07e1\7\t\2\2\u07e0\u07df\3\2\2\2\u07e0\u07e1\3\2\2\2\u07e1\u07e2"+
		"\3\2\2\2\u07e2\u07e5\5\u0094K\2\u07e3\u07e4\7i\2\2\u07e4\u07e6\7\u00df"+
		"\2\2\u07e5\u07e3\3\2\2\2\u07e5\u07e6\3\2\2\2\u07e6\u0099\3\2\2\2\u07e7"+
		"\u07e8\7.\2\2\u07e8\u07e9\5x=\2\u07e9\u07ea\7/\2\2\u07ea\u07eb\5x=\2\u07eb"+
		"\u009b\3\2\2\2\u07ec\u07ed\7=\2\2\u07ed\u07f2\5\u009eP\2\u07ee\u07ef\7"+
		"\5\2\2\u07ef\u07f1\5\u009eP\2\u07f0\u07ee\3\2\2\2\u07f1\u07f4\3\2\2\2"+
		"\u07f2\u07f0\3\2\2\2\u07f2\u07f3\3\2\2\2\u07f3\u009d\3\2\2\2\u07f4\u07f2"+
		"\3\2\2\2\u07f5\u07f6\5\u00a8U\2\u07f6\u07f7\7\r\2\2\u07f7\u07f8\5\u00a0"+
		"Q\2\u07f8\u009f\3\2\2\2\u07f9\u0824\5\u00a8U\2\u07fa\u081d\7\3\2\2\u07fb"+
		"\u07fc\7\u008a\2\2\u07fc\u07fd\7\22\2\2\u07fd\u0802\5x=\2\u07fe\u07ff"+
		"\7\5\2\2\u07ff\u0801\5x=\2\u0800\u07fe\3\2\2\2\u0801\u0804\3\2\2\2\u0802"+
		"\u0800\3\2\2\2\u0802\u0803\3\2\2\2\u0803\u081e\3\2\2\2\u0804\u0802\3\2"+
		"\2\2\u0805\u0806\t\25\2\2\u0806\u0807\7\22\2\2\u0807\u080c\5x=\2\u0808"+
		"\u0809\7\5\2\2\u0809\u080b\5x=\2\u080a\u0808\3\2\2\2\u080b\u080e\3\2\2"+
		"\2\u080c\u080a\3\2\2\2\u080c\u080d\3\2\2\2\u080d\u0810\3\2\2\2\u080e\u080c"+
		"\3\2\2\2\u080f\u0805\3\2\2\2\u080f\u0810\3\2\2\2\u0810\u081b\3\2\2\2\u0811"+
		"\u0812\t\26\2\2\u0812\u0813\7\22\2\2\u0813\u0818\5J&\2\u0814\u0815\7\5"+
		"\2\2\u0815\u0817\5J&\2\u0816\u0814\3\2\2\2\u0817\u081a\3\2\2\2\u0818\u0816"+
		"\3\2\2\2\u0818\u0819\3\2\2\2\u0819\u081c\3\2\2\2\u081a\u0818\3\2\2\2\u081b"+
		"\u0811\3\2\2\2\u081b\u081c\3\2\2\2\u081c\u081e\3\2\2\2\u081d\u07fb\3\2"+
		"\2\2\u081d\u080f\3\2\2\2\u081e\u0820\3\2\2\2\u081f\u0821\5\u00a2R\2\u0820"+
		"\u081f\3\2\2\2\u0820\u0821\3\2\2\2\u0821\u0822\3\2\2\2\u0822\u0824\7\4"+
		"\2\2\u0823\u07f9\3\2\2\2\u0823\u07fa\3\2\2\2\u0824\u00a1\3\2\2\2\u0825"+
		"\u0826\7@\2\2\u0826\u0836\5\u00a4S\2\u0827\u0828\7A\2\2\u0828\u0836\5"+
		"\u00a4S\2\u0829\u082a\7@\2\2\u082a\u082b\7!\2\2\u082b\u082c\5\u00a4S\2"+
		"\u082c\u082d\7\34\2\2\u082d\u082e\5\u00a4S\2\u082e\u0836\3\2\2\2\u082f"+
		"\u0830\7A\2\2\u0830\u0831\7!\2\2\u0831\u0832\5\u00a4S\2\u0832\u0833\7"+
		"\34\2\2\u0833\u0834\5\u00a4S\2\u0834\u0836\3\2\2\2\u0835\u0825\3\2\2\2"+
		"\u0835\u0827\3\2\2\2\u0835\u0829\3\2\2\2\u0835\u082f\3\2\2\2\u0836\u00a3"+
		"\3\2\2\2\u0837\u0838\7B\2\2\u0838\u083f\t\27\2\2\u0839\u083a\7E\2\2\u083a"+
		"\u083f\7F\2\2\u083b\u083c\5x=\2\u083c\u083d\t\27\2\2\u083d\u083f\3\2\2"+
		"\2\u083e\u0837\3\2\2\2\u083e\u0839\3\2\2\2\u083e\u083b\3\2\2\2\u083f\u00a5"+
		"\3\2\2\2\u0840\u0845\5\u00a8U\2\u0841\u0842\7\6\2\2\u0842\u0844\5\u00a8"+
		"U\2\u0843\u0841\3\2\2\2\u0844\u0847\3\2\2\2\u0845\u0843\3\2\2\2\u0845"+
		"\u0846\3\2\2\2\u0846\u00a7\3\2\2\2\u0847\u0845\3\2\2\2\u0848\u0857\5\u00aa"+
		"V\2\u0849\u0857\7\u00db\2\2\u084a\u0857\79\2\2\u084b\u0857\7\65\2\2\u084c"+
		"\u0857\7\66\2\2\u084d\u0857\7\67\2\2\u084e\u0857\78\2\2\u084f\u0857\7"+
		":\2\2\u0850\u0857\7\62\2\2\u0851\u0857\7\63\2\2\u0852\u0857\7;\2\2\u0853"+
		"\u0857\7^\2\2\u0854\u0857\7`\2\2\u0855\u0857\7_\2\2\u0856\u0848\3\2\2"+
		"\2\u0856\u0849\3\2\2\2\u0856\u084a\3\2\2\2\u0856\u084b\3\2\2\2\u0856\u084c"+
		"\3\2\2\2\u0856\u084d\3\2\2\2\u0856\u084e\3\2\2\2\u0856\u084f\3\2\2\2\u0856"+
		"\u0850\3\2\2\2\u0856\u0851\3\2\2\2\u0856\u0852\3\2\2\2\u0856\u0853\3\2"+
		"\2\2\u0856\u0854\3\2\2\2\u0856\u0855\3\2\2\2\u0857\u00a9\3\2\2\2\u0858"+
		"\u085c\7\u00e8\2\2\u0859\u085c\5\u00acW\2\u085a\u085c\5\u00b0Y\2\u085b"+
		"\u0858\3\2\2\2\u085b\u0859\3\2\2\2\u085b\u085a\3\2\2\2\u085c\u00ab\3\2"+
		"\2\2\u085d\u085e\7\u00e9\2\2\u085e\u00ad\3\2\2\2\u085f\u0867\7\u00e5\2"+
		"\2\u0860\u0867\7\u00e6\2\2\u0861\u0867\7\u00e4\2\2\u0862\u0867\7\u00e0"+
		"\2\2\u0863\u0867\7\u00e1\2\2\u0864\u0867\7\u00e2\2\2\u0865\u0867\7\u00e7"+
		"\2\2\u0866\u085f\3\2\2\2\u0866\u0860\3\2\2\2\u0866\u0861\3\2\2\2\u0866"+
		"\u0862\3\2\2\2\u0866\u0863\3\2\2\2\u0866\u0864\3\2\2\2\u0866\u0865\3\2"+
		"\2\2\u0867\u00af\3\2\2\2\u0868\u0869\t\30\2\2\u0869\u00b1\3\2\2\2\u0126"+
		"\u00c6\u00cb\u00ce\u00d3\u00e0\u00e4\u00eb\u00f0\u00f5\u00f8\u00fe\u0103"+
		"\u0106\u0109\u0112\u0116\u011e\u0121\u0124\u0127\u012a\u012d\u0131\u0134"+
		"\u0137\u013e\u0148\u0150\u0152\u0169\u0171\u0179\u017f\u018c\u0191\u019a"+
		"\u019f\u01af\u01b6\u01ba\u01c2\u01c9\u01d0\u01d9\u01dd\u01e3\u01e9\u01ec"+
		"\u01f2\u01f6\u01fa\u01ff\u0203\u020b\u0214\u0219\u021f\u0225\u0231\u0234"+
		"\u0238\u023d\u0242\u0249\u024c\u024f\u0255\u025e\u0266\u026c\u0270\u0274"+
		"\u0278\u027a\u0283\u0289\u028e\u0292\u0295\u02a2\u02a7\u02ac\u02af\u02b9"+
		"\u02be\u02c4\u02ca\u02d1\u02d9\u02e0\u02e9\u02f4\u02fd\u0300\u0308\u030f"+
		"\u0317\u031b\u0322\u0399\u03a1\u03a9\u03b2\u03bb\u03bf\u03c5\u03d1\u03d5"+
		"\u03d8\u03de\u03e8\u03f4\u03f9\u03ff\u040b\u040d\u0412\u0416\u0418\u041c"+
		"\u0425\u042d\u0434\u043a\u043e\u0447\u044c\u045b\u0462\u0465\u046c\u0470"+
		"\u0478\u0483\u048e\u0495\u049b\u04a1\u04aa\u04ac\u04b5\u04b8\u04c1\u04c4"+
		"\u04cd\u04d0\u04d9\u04dc\u04df\u04e3\u04e6\u04f1\u04f6\u0501\u0505\u0511"+
		"\u0514\u0518\u0522\u0526\u0528\u052b\u052f\u0532\u0536\u053a\u053e\u0543"+
		"\u0546\u0548\u054d\u0552\u0555\u0559\u055c\u055e\u0566\u056c\u0576\u0585"+
		"\u058a\u0592\u0595\u0599\u059e\u05a7\u05aa\u05af\u05b6\u05b9\u05c3\u05c8"+
		"\u05cf\u05d3\u05d7\u05db\u05e1\u05e5\u05e8\u05eb\u05f6\u05fb\u0610\u0612"+
		"\u0614\u0621\u062a\u0631\u0639\u0641\u0645\u0648\u064b\u0651\u0654\u0657"+
		"\u065d\u0660\u0663\u0666\u066e\u0672\u0676\u0678\u0681\u068d\u068f\u0696"+
		"\u069d\u06a3\u06a9\u06ab\u06b0\u06b6\u06ba\u06bc\u06c3\u06d1\u06d9\u06db"+
		"\u06e0\u06e3\u06eb\u06f4\u06fa\u0702\u0708\u070b\u0711\u0726\u0728\u0738"+
		"\u073f\u0746\u0749\u074e\u0759\u075d\u0765\u0769\u0779\u0783\u0785\u0792"+
		"\u0794\u07a2\u07a9\u07ac\u07b0\u07c1\u07c5\u07ce\u07d2\u07d4\u07db\u07e0"+
		"\u07e5\u07f2\u0802\u080c\u080f\u0818\u081b\u081d\u0820\u0823\u0835\u083e"+
		"\u0845\u0856\u085b\u0866";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}