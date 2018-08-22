package de.uni_mannheim.informatik.dws.tnt.match.data;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

//import objectexplorer.MemoryMeasurer;
//import objectexplorer.ObjectGraphMeasurer;
//import objectexplorer.ObjectGraphMeasurer.Footprint;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.uni_mannheim.informatik.dws.tnt.match.ContextColumns;
import de.uni_mannheim.informatik.dws.tnt.match.SpecialColumns;
import de.uni_mannheim.informatik.dws.tnt.match.TableSchemaStatistics;
import de.uni_mannheim.informatik.dws.tnt.match.dependencies.FunctionalDependencyUtils;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.FusibleDataSet;
import de.uni_mannheim.informatik.dws.winter.model.FusibleParallelHashedDataSet;
import de.uni_mannheim.informatik.dws.winter.model.ParallelHashedDataSet;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.utils.ProgressReporter;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableContext;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;
import de.uni_mannheim.informatik.dws.winter.webtables.features.HorizontallyStackedFeature;
import de.uni_mannheim.informatik.dws.winter.webtables.parsers.CsvTableParser;
import de.uni_mannheim.informatik.dws.winter.webtables.parsers.JsonTableParser;
import de.uni_mannheim.informatik.dws.winter.webtables.parsers.JsonTableSchema;
import de.uni_mannheim.informatik.dws.winter.webtables.parsers.JsonTableWithMappingSchema;
import de.uni_mannheim.informatik.dws.winter.webtables.writers.CSVTableWriter;
import de.uni_mannheim.informatik.dws.winter.webtables.writers.JsonTableWriter;

public class WebTables {

    // data that will be matched: records and schema
    private FusibleDataSet<MatchableTableRow, MatchableTableColumn> records = new FusibleParallelHashedDataSet<>();
    private DataSet<MatchableTableColumn, MatchableTableColumn> schema = new ParallelHashedDataSet<>();
    private DataSet<MatchableTableDeterminant, MatchableTableColumn> candidateKeys = new ParallelHashedDataSet<>();
    private DataSet<MatchableTable, MatchableTableColumn> tableRecords = new ParallelHashedDataSet<>();

    // matched web tables and their key columns
    private HashMap<Integer, MatchableTableColumn> keys = new HashMap<>();

    // translation for web table identifiers
    private HashMap<String, String> columnHeaders = new HashMap<>();

    // translation from table name to table id
    private HashMap<String, Integer> tableIndices = new HashMap<>();

    // lookup for tables by id
    private HashMap<Integer, Table> tables = null;

    private boolean measure = false;

    public void setMeasureMemoryUsage(boolean measure) {
        this.measure = measure;
    }

    public void setKeepTablesInMemory(boolean keep) {
        if (keep) {
            tables = new HashMap<>();
        } else {
            tables = null;
        }
    }

    private boolean convertValues = true;

    /**
     * @param convertValues the convertValues to set
     */
    public void setConvertValues(boolean convertValues) {
        this.convertValues = convertValues;
    }

    private boolean inferSchema = true;

    /**
     * @param inferSchema the inferSchema to set
     */
    public void setInferSchema(boolean inferSchema) {
        this.inferSchema = inferSchema;
    }

    private void loadVertica(String domain, Connection con) throws SQLException {
        JsonTableParser jsonParser = new JsonTableParser();
        jsonParser.setConvertValues(convertValues);
        jsonParser.setInferSchema(inferSchema);
        Gson gson = new Gson();

        ResultSet resultSet = con.createStatement().executeQuery("SELECT id, externalid, url, content, pageTitle_tokenized, title_tokenized" +
                " FROM tables_tokenized_full WHERE domain='" + domain + "' ORDER BY id");

        while (resultSet.next()) {
            StringReader reader = new StringReader(resultSet.getString("content"));
            JsonObject content = gson.fromJson(reader, JsonObject.class);

            Table table = tableFromContent(content, resultSet, jsonParser);
            if (table == null) {
                continue;
            }
            int tblIdx = resultSet.getInt("id");
            tables.put(tblIdx, table);
            table.setTableId(tblIdx);
            tableIndices.put(table.getPath(), tblIdx);

            // list schema
            LinkedList<MatchableTableColumn> schemaColumns = new LinkedList<>();
            for (TableColumn c : table.getSchema().getRecords()) {
                MatchableTableColumn mc = new MatchableTableColumn(tblIdx, c);
                schema.add(mc);
                schemaColumns.add(mc);
                columnHeaders.put(mc.getIdentifier(), c.getHeader());
                if (table.hasSubjectColumn() && table.getSubjectColumnIndex() == c.getColumnIndex()) {
                    keys.put(mc.getTableId(), mc);
                }
            }

            // list candidate keys
            for (Set<TableColumn> candKey : table.getSchema().getCandidateKeys()) {
                Set<MatchableTableColumn> columns = new HashSet<>();
                for (TableColumn keyCol : candKey) {
                    for (MatchableTableColumn mc : schemaColumns) {
                        if (mc.getColumnIndex() == keyCol.getColumnIndex()) {
                            columns.add(mc);
                        }
                    }
                }
                MatchableTableDeterminant k = new MatchableTableDeterminant(tblIdx, columns);
                candidateKeys.add(k);
            }

            // create the matchable table record
            MatchableTable mt = new MatchableTable(table, Q.toArrayFromCollection(schemaColumns, MatchableTableColumn.class));
            if (table.getContext() == null) {
                table.setTableId(tblIdx);
            }
            tableRecords.add(mt);

            // list records
            for (TableRow r : table.getRows()) {
                MatchableTableRow row = new MatchableTableRow(r, tblIdx, Q.toArrayFromCollection(schemaColumns, MatchableTableColumn.class));
                records.add(row);
            }
        }
        resultSet.close();
        printLoadStats();
    }

    public static WebTables loadVerticaWebTables(Connection con, String domain, boolean keepTablesInMemory, boolean inferSchema, boolean convertValues, boolean serialise) throws SQLException {
        WebTables web = new WebTables();
        web.setKeepTablesInMemory(keepTablesInMemory);
        web.setInferSchema(inferSchema);
        web.setConvertValues(convertValues);

        web.loadVertica(domain, con);

        return web;
    }

    private Table tableFromContent(JsonObject content, ResultSet resultSet, JsonTableParser jsonParser) throws SQLException {
        Table web = new Table();

        web.setPath(resultSet.getString("externalid"));
        web.setSubjectColumnIndex(0);
        web.setTableId(resultSet.getInt("id"));

        TableContext ctx = new TableContext();
        ctx.setTableNum(0);
        ctx.setUrl(resultSet.getString("url"));
        ctx.setPageTitle(resultSet.getString("pageTitle_tokenized"));
        ctx.setTableTitle(resultSet.getString("title_tokenized"));
        ctx.setTextBeforeTable("");
        ctx.setTextAfterTable("");
        ctx.setTimestampBeforeTable("");
        ctx.setTimestampAfterTable("");
        ctx.setLastModified("");
        web.setContext(ctx);

        JsonTableSchema data = new JsonTableSchema();
        JsonArray tuples = content.getAsJsonArray("tuples");
        String[][] relation = new String[tuples.size()][tuples.get(0).getAsJsonObject().get("cells").getAsJsonArray().size()];
        for (int i = 0; i < tuples.size(); i++) {
            JsonArray cells = tuples.get(i).getAsJsonObject().get("cells").getAsJsonArray();
            for (int j = 0; j < cells.size(); j++) {
                JsonElement val = cells.get(j).getAsJsonObject().get("value");
                if (val == null) {
                    relation[i][j] = "";
                } else {
                    relation[i][j] = val.getAsString();

                }
            }
        }

        int[] headerRowIndex;
        int[] emptyRowCount;
        headerRowIndex = new int[1];

        //----------------parseColumnData
        JsonArray jsonColumns = content.getAsJsonObject("schema").get("columnNames").getAsJsonArray();
        String[] columnNames = new String[jsonColumns.size()];
        for (int i = 0; i < jsonColumns.size(); i++) {
            if (!jsonColumns.get(i).isJsonNull()) {
                columnNames[i] = jsonColumns.get(i).getAsString();
            } else {
                columnNames[i] = "";
            }
        }
        for (int colIdx = 0; colIdx < relation[0].length; colIdx++) {
            String columnName = null;
            if (columnNames != null && columnNames.length > colIdx) {
                columnName = columnNames[colIdx];
            } else {
                columnName = "";
            }

            TableColumn c = new TableColumn(colIdx, web);
            c.setDataType(DataType.unknown);

            c.setHeader(columnName);
            web.addColumn(c);
        }
        jsonParser.populateTable(relation, web, new int[0]);

        return web;
    }

    public Table tableFromDump(String[] elements, JsonTableParser jsonParser, Gson gson) {
        Table web = new Table();

        web.setPath(elements[1]);
        web.setSubjectColumnIndex(0);
        web.setTableId(Integer.parseInt(elements[0]));

        TableContext ctx = new TableContext();
        ctx.setTableNum(0);

        // replace %-sign in URL to catch the URISyntaxException
        String cleanUri = "";
        try {
            URI uri = new URI(elements[3]);
            cleanUri = elements[3];
        } catch (URISyntaxException e) {
            StringBuilder builder = new StringBuilder();
            for (String str : elements[3].split("/")) {
                if (str.contains("%")){
                    try {
                        builder.append(URLEncoder.encode(str, "UTF-8"));
                    } catch (UnsupportedEncodingException e1) {
                        e1.printStackTrace();
                        builder.append("/");
                    }
                } else {
                    builder.append(str);
                }
                builder.append("/");
            }
            builder.deleteCharAt(builder.length()-1);
            cleanUri = builder.toString();
        }
        ctx.setUrl(cleanUri);
        ctx.setPageTitle("");
        ctx.setTableTitle("");
        ctx.setTextBeforeTable("");
        ctx.setTextAfterTable("");
        ctx.setTimestampBeforeTable("");
        ctx.setTimestampAfterTable("");
        ctx.setLastModified("");
        web.setContext(ctx);

        JsonTableSchema data = new JsonTableSchema();
        JsonObject jsonObject = gson.fromJson(elements[6], JsonObject.class);

        JsonArray tuples = jsonObject.getAsJsonArray("tuples");
        String[][] relation = new String[tuples.size()][tuples.get(0).getAsJsonObject().get("cells").getAsJsonArray().size()];
        for (int i = 0; i < tuples.size(); i++) {
            JsonArray cells = tuples.get(i).getAsJsonObject().get("cells").getAsJsonArray();
            for (int j = 0; j < cells.size(); j++) {
                JsonElement val = cells.get(j).getAsJsonObject().get("value");
                if (val == null) {
                    relation[i][j] = "";
                } else {
                    relation[i][j] = val.getAsString();

                }
            }
        }

        int[] headerRowIndex;
        int[] emptyRowCount;
        headerRowIndex = new int[1];

        //----------------parseColumnData
        JsonArray jsonColumns = jsonObject.getAsJsonObject("schema").get("columnNames").getAsJsonArray();
        String[] columnNames = new String[jsonColumns.size()];
        for (int i = 0; i < jsonColumns.size(); i++) {
            if (!jsonColumns.get(i).isJsonNull()) {
                columnNames[i] = jsonColumns.get(i).getAsString();
            } else {
                columnNames[i] = "NULL";
            }
        }
        for (int colIdx = 0; colIdx < relation[0].length; colIdx++) {
            String columnName = null;
            if (columnNames != null && columnNames.length > colIdx) {
                columnName = columnNames[colIdx];
            } else {
                columnName = "NULL";
            }

            TableColumn c = new TableColumn(colIdx, web);
            c.setDataType(DataType.unknown);

            c.setHeader(columnName);
            web.addColumn(c);
        }
        jsonParser.populateTable(relation, web, new int[0]);

        return web;
    }

    public static WebTables loadWebTables(File location, boolean keepTablesInMemory, boolean inferSchema, boolean convertValues, boolean serialise) throws FileNotFoundException {
        // look for serialised version
        File ser = new File(location.getParentFile(), location.getName() + ".bin");

        if (ser.exists() && serialise) {
            WebTables web = WebTables.deserialise(ser);
            web.printLoadStats();
            return web;
        } else {
            WebTables web = new WebTables();
            web.setKeepTablesInMemory(keepTablesInMemory);
            web.setInferSchema(inferSchema);
            web.setConvertValues(convertValues);
            if (location.getName().endsWith("txt.gz")) {
                try {
                    web.loadExport(location);
                } catch (IOException e) {
                    System.exit(92);
                }
            } else {
                web.load(location);
            }
            // Serialise only if we loaded more than one table (otherwise we would generate .bin files in folders that contain many web tables which would lead to problem when loading the whole folder)
            if (web.getRecords().size() > 1 && serialise) {
                web.serialise(ser);
            }

            return web;
        }
    }

    public void loadExport(File location) throws IOException {
        JsonTableParser jsonParser = new JsonTableParser();
        jsonParser.setConvertValues(convertValues);
        jsonParser.setInferSchema(inferSchema);

        GZIPInputStream in = new GZIPInputStream(new FileInputStream(location));
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        Gson gson = new Gson();
        String content;
        String[] elements;

        while ((content = reader.readLine()) != null) {
            elements = content.split("\\|", 7);
            int tblIdx = Integer.parseInt(elements[0]);
            Table web = tableFromDump(elements, jsonParser, gson);

            if (tables != null) {
                tables.put(tblIdx, web);
                web.setTableId(tblIdx);
            }

            tableIndices.put(web.getPath(), tblIdx);

            // list schema
            LinkedList<MatchableTableColumn> schemaColumns = new LinkedList<>();
            for (TableColumn c : web.getSchema().getRecords()) {
                MatchableTableColumn mc = new MatchableTableColumn(tblIdx, c);
                schema.add(mc);
                schemaColumns.add(mc);
                columnHeaders.put(mc.getIdentifier(), c.getHeader());
                if (web.hasSubjectColumn() && web.getSubjectColumnIndex() == c.getColumnIndex()) {
                    keys.put(mc.getTableId(), mc);
                }
            }

            // list candidate keys
            for (Set<TableColumn> candKey : web.getSchema().getCandidateKeys()) {

                Set<MatchableTableColumn> columns = new HashSet<>();
                for (TableColumn keyCol : candKey) {
                    for (MatchableTableColumn mc : schemaColumns) {
                        if (mc.getColumnIndex() == keyCol.getColumnIndex()) {
                            columns.add(mc);
                        }
                    }
                }

                MatchableTableDeterminant k = new MatchableTableDeterminant(tblIdx, columns);

                candidateKeys.add(k);
            }

            // create the matchable table record
            MatchableTable mt = new MatchableTable(web, Q.toArrayFromCollection(schemaColumns, MatchableTableColumn.class));
            if (web.getContext() == null) {
                web.setTableId(tblIdx);
            }
            tableRecords.add(mt);

            // list records
            for (TableRow r : web.getRows()) {
                MatchableTableRow row = new MatchableTableRow(r, tblIdx, Q.toArrayFromCollection(schemaColumns, MatchableTableColumn.class));
                records.add(row);
            }
        }

    }

    public void loadGzip(File location) throws IOException {
        JsonTableParser jsonParser = new JsonTableParser();

        jsonParser.setConvertValues(convertValues);
        jsonParser.setInferSchema(inferSchema);

        GZIPInputStream in = new GZIPInputStream(new FileInputStream(location));
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));

        Gson gson = new Gson();
        String content;
        while ((content = reader.readLine()) != null) {
            int tblIdx = gson.fromJson(content, JsonObject.class).get("table").getAsJsonObject().get("tableid").getAsInt();
            JsonObject json = gson.fromJson(content, JsonObject.class);

            JsonTableSchema data = (JsonTableSchema) gson.fromJson(json.get("table").getAsJsonObject().toString(), JsonTableSchema.class);
            Table web = jsonParser.parseTable(data, "" + tblIdx, null);
            if (web == null) {
                JsonTableWithMappingSchema dataWithMapping = (JsonTableWithMappingSchema) gson.fromJson(content, JsonTableWithMappingSchema.class);
                web = jsonParser.parseTable(dataWithMapping.getTable(), "" + tblIdx, null);
            }

            if (web == null) {
                continue;
            }

            if (tables != null) {
                tables.put(tblIdx, web);
                web.setTableId(tblIdx);
            }

            tableIndices.put(web.getPath(), tblIdx);

            // list schema
            LinkedList<MatchableTableColumn> schemaColumns = new LinkedList<>();
            for (TableColumn c : web.getSchema().getRecords()) {
                MatchableTableColumn mc = new MatchableTableColumn(tblIdx, c);
                schema.add(mc);
                schemaColumns.add(mc);
                columnHeaders.put(mc.getIdentifier(), c.getHeader());
                if (web.hasSubjectColumn() && web.getSubjectColumnIndex() == c.getColumnIndex()) {
                    keys.put(mc.getTableId(), mc);
                }
            }

            // list candidate keys
            for (Set<TableColumn> candKey : web.getSchema().getCandidateKeys()) {

                Set<MatchableTableColumn> columns = new HashSet<>();
                for (TableColumn keyCol : candKey) {
                    for (MatchableTableColumn mc : schemaColumns) {
                        if (mc.getColumnIndex() == keyCol.getColumnIndex()) {
                            columns.add(mc);
                        }
                    }
                }

                MatchableTableDeterminant k = new MatchableTableDeterminant(tblIdx, columns);

                candidateKeys.add(k);
            }

            // create the matchable table record
            MatchableTable mt = new MatchableTable(web, Q.toArrayFromCollection(schemaColumns, MatchableTableColumn.class));
            if (web.getContext() == null) {
                web.setTableId(tblIdx);
            }
            tableRecords.add(mt);

            // list records
            for (TableRow r : web.getRows()) {
                MatchableTableRow row = new MatchableTableRow(r, tblIdx, Q.toArrayFromCollection(schemaColumns, MatchableTableColumn.class));
                records.add(row);
            }
        }
    }

    public void load(File location) {
        CsvTableParser csvParser = new CsvTableParser();
        JsonTableParser jsonParser = new JsonTableParser();

        //TODO add setting for value conversion to csv parser
        jsonParser.setConvertValues(convertValues);
        jsonParser.setInferSchema(inferSchema);

        List<File> webFiles = null;

        if (location.isDirectory()) {
            webFiles = Arrays.asList(location.listFiles());
        } else {
            webFiles = Arrays.asList(new File[]{location});
        }

        ProgressReporter progress = new ProgressReporter(webFiles.size(), "Loading Web Tables");

        int tblIdx = 0;

        Queue<File> toLoad = new LinkedList<>(webFiles);
//    	for(File f : webFiles) {
        while (toLoad.size() > 0) {
            File f = toLoad.poll();

            if (f.isDirectory()) {
                toLoad.addAll(Arrays.asList(f.listFiles()));
                progress = new ProgressReporter(toLoad.size(), "Loading Web Tables", progress.getProcessedElements());
            } else {

//			System.out.println("Loading Web Table " + f.getName());
                try {
                    Table web = null;

                    if (f.getName().endsWith("csv")) {
                        web = csvParser.parseTable(f);
                    } else if (f.getName().endsWith("json")) {
                        web = jsonParser.parseTable(f);
                    } else {
                        System.out.println(String.format("Unknown table format: %s", f.getName()));
                    }

                    if (web == null) {
                        continue;
                    }

                    if (tables != null) {
                        tables.put(tblIdx, web);
                        web.setTableId(tblIdx);
                    }

                    if (webFiles.size() == 1) {
                        for (TableColumn tc : web.getSchema().getRecords()) {
                            System.out.println(String.format("{%s} [%d] %s (%s)", web.getPath(), tc.getColumnIndex(), tc.getHeader(), tc.getDataType()));
                        }
                    }

                    tableIndices.put(web.getPath(), tblIdx);

                    // list schema
                    LinkedList<MatchableTableColumn> schemaColumns = new LinkedList<>();
                    for (TableColumn c : web.getSchema().getRecords()) {
                        MatchableTableColumn mc = new MatchableTableColumn(tblIdx, c);
                        schema.add(mc);
                        schemaColumns.add(mc);
                        columnHeaders.put(mc.getIdentifier(), c.getHeader());
                        if (web.hasSubjectColumn() && web.getSubjectColumnIndex() == c.getColumnIndex()) {
                            keys.put(mc.getTableId(), mc);
                        }
                    }

                    // list candidate keys
                    for (Set<TableColumn> candKey : web.getSchema().getCandidateKeys()) {

                        Set<MatchableTableColumn> columns = new HashSet<>();
                        for (TableColumn keyCol : candKey) {
                            for (MatchableTableColumn mc : schemaColumns) {
                                if (mc.getColumnIndex() == keyCol.getColumnIndex()) {
                                    columns.add(mc);
                                }
                            }
                        }

                        MatchableTableDeterminant k = new MatchableTableDeterminant(tblIdx, columns);

                        candidateKeys.add(k);
                    }

                    // create the matchable table record
                    MatchableTable mt = new MatchableTable(web, Q.toArrayFromCollection(schemaColumns, MatchableTableColumn.class));
                    if (web.getContext() == null) {
                        web.setTableId(tblIdx);
                    }
                    tableRecords.add(mt);

                    // list records
                    for (TableRow r : web.getRows()) {
                        MatchableTableRow row = new MatchableTableRow(r, tblIdx, Q.toArrayFromCollection(schemaColumns, MatchableTableColumn.class));
                        records.add(row);
                    }


                    tblIdx++;
                } catch (Exception e) {
                    System.err.println(String.format("Could not load table %s", f.getAbsolutePath()));
                    e.printStackTrace();
                }

                progress.incrementProgress();
                progress.report();
            }
        }

        printLoadStats();
    }

    public void removeHorizontallyStackedTables() throws Exception {
        HorizontallyStackedFeature f = new HorizontallyStackedFeature();
        TableSchemaStatistics stat = new TableSchemaStatistics();

        for (Integer tableId : new ArrayList<>(tables.keySet())) {
            Table t = tables.get(tableId);

            Collection<TableColumn> noContextColumns = Q.where(t.getColumns(), new ContextColumns.IsNoContextColumnPredicate());

            Table tNoContext = t.project(noContextColumns);

            double horizontallyStacked = f.calculate(tNoContext);

            if (horizontallyStacked > 0.0) {

                System.out.println(String.format("Removing table '%s' with schema '%s' (horizontally stacked)", t.getPath(), stat.generateNonContextSchemaString(t)));

                tables.remove(tableId);

                for (TableColumn c : t.getColumns()) {
                    columnHeaders.remove(c.getIdentifier());
                    schema.removeRecord(c.getIdentifier());

                }

                Iterator<MatchableTableDeterminant> ckIt = candidateKeys.get().iterator();
                while (ckIt.hasNext()) {
                    if (ckIt.next().getTableId() == tableId) {
                        ckIt.remove();
                    }
                }

                for (TableRow r : t.getRows()) {
                    records.removeRecord(r.getIdentifier());
                }

                keys.remove(tableId);
                tableIndices.remove(t.getPath());
                tableRecords.removeRecord(Integer.toString(t.getTableId()));
            }
        }
    }

    public Table verifyColumnHeaders(Table t) {
        // check if the column headers are all null, if so, skip until a non-null row is found

        for (TableColumn c : t.getColumns()) {
            if (c.getHeader() != null && !c.getHeader().isEmpty() && !"null".equals(c.getHeader())) {
                return t;
            }
        }

        // all headers are null
        TableRow headerRow = null;
        for (TableRow r : t.getRows()) {
            for (TableColumn c : t.getColumns()) {

                Object value = r.get(c.getColumnIndex());

                if (value != null && !"null".equals(value)) {
                    headerRow = r;
                }
            }
        }

        if (headerRow != null) {
            Table t2 = t.copySchema();

            for (TableColumn c : t.getColumns()) {
                Object value = headerRow.get(c.getColumnIndex());
                String header = null;
                if (value == null) {
                    header = "null";
                } else {
                    header = value.toString();
                }
                t2.getSchema().get(c.getColumnIndex()).setHeader(header);
            }

            int rowNumber = 0;
            for (TableRow r : t.getRows()) {
                if (r.getRowNumber() > headerRow.getRowNumber()) {
                    TableRow r2 = new TableRow(rowNumber++, t2);
                    t2.addRow(r2);
                }
            }

            return t2;
        } else {
            return t;
        }
    }

    void printLoadStats() {
        System.out.println(String.format("%,d Web Tables Instances", records.size()));
        System.out.println(String.format("%,d Web Tables Schema Elements", schema.size()));

        if (measure) {
            System.out.println("Measuring Memory Usage");
            measure(records, "Web Tables Dataset");
            measure(schema, "Web Tables Schema");
            measure(columnHeaders, "Web Tables Column Headers");
            measure(keys, "Web Table Keys");
        }
    }

    void measure(Object obj, String name) {
//        long memory = MemoryMeasurer.measureBytes(obj);
//
//        System.out.println(String.format("%s Memory Size: %,d", name, memory));
//        
//        Footprint footprint = ObjectGraphMeasurer.measure(obj);
//        System.out.println(String.format("%s Graph Footprint: \n\tObjects: %,d\n\tReferences %,d", name, footprint.getObjects(), footprint.getReferences()));
    }

    public FusibleDataSet<MatchableTableRow, MatchableTableColumn> getRecords() {
        return records;
    }

    public DataSet<MatchableTableColumn, MatchableTableColumn> getSchema() {
        return schema;
    }

    /**
     * @return the candidateKeys
     */
    public DataSet<MatchableTableDeterminant, MatchableTableColumn> getCandidateKeys() {
        return candidateKeys;
    }

    public HashMap<Integer, MatchableTableColumn> getKeys() {
        return keys;
    }

    /**
     * @return the tableRecords
     */
    public DataSet<MatchableTable, MatchableTableColumn> getTableRecords() {
        return tableRecords;
    }

    /**
     * A map (Column Identifier) -> (Column Header)
     *
     * @return
     */
    public HashMap<String, String> getColumnHeaders() {
        return columnHeaders;
    }

    /**
     * @return the tables
     */
    public HashMap<Integer, Table> getTables() {
        return tables;
    }

    /**
     * A map (Table Path) -> (Table Id)
     *
     * @return the tableIndices
     */
    public HashMap<String, Integer> getTableIndices() {
        return tableIndices;
    }

    public static WebTables deserialise(File location) throws FileNotFoundException {
        System.out.println("Deserialising Web Tables");

        Kryo kryo = new Kryo();

        Input input = new Input(new FileInputStream(location));
        WebTables web = kryo.readObject(input, WebTables.class);
        input.close();

        return web;
    }

    public void serialise(File location) throws FileNotFoundException {
        System.out.println("Serialising Web Tables");

        Kryo kryo = new Kryo();
        Output output = new Output(new FileOutputStream(location));
        kryo.writeObject(output, this);
        output.close();
    }

    public static void writeTables(Collection<Table> tables, File jsonLocation, File csvLocation) throws IOException {
        for (Table t : tables) {
            if (jsonLocation != null) {
                JsonTableWriter jtw = new JsonTableWriter();
                jtw.write(t, new File(jsonLocation, t.getPath()));
            }

            if (csvLocation != null) {
                CSVTableWriter tw = new CSVTableWriter();
                tw.write(t, new File(csvLocation, t.getPath()));
            }
        }
    }

    public static void calculateDependenciesAndCandidateKeys(Collection<Table> tables, File csvTablesLocation) throws IOException, AlgorithmExecutionException {
        for (Table t : tables) {
            Set<TableColumn> dedupColumns = new HashSet<>();
            for (TableColumn c : t.getColumns()) {
                if (!SpecialColumns.isSpecialColumn(c)) {
                    dedupColumns.add(c);
                }
            }
            // remove duplicate rows (otherwise UCC discovery does not work)
            t.deduplicate(dedupColumns);

            File f = new File(csvTablesLocation, t.getPath());
            CSVTableWriter tw = new CSVTableWriter();
            f = tw.write(t, f);

            Map<Collection<TableColumn>, Collection<TableColumn>> fd = FunctionalDependencyUtils.calculateFunctionalDependencies(t, f);
            t.getSchema().setFunctionalDependencies(fd);

            Collection<Set<TableColumn>> k = FunctionalDependencyUtils.calculateUniqueColumnCombinationsExcludingSpecialColumns(t, f);
            t.getSchema().setCandidateKeys(k);
        }
    }
}
