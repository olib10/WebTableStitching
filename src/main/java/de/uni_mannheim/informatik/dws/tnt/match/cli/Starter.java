package de.uni_mannheim.informatik.dws.tnt.match.cli;

import au.com.bytecode.opencsv.CSVWriter;
import com.vertica.jdbc.VerticaConnection;
import com.vertica.jdbc.VerticaCopyStream;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.tnt.match.stitching.FunctionalDependencyDiscovery;
import de.uni_mannheim.informatik.dws.tnt.match.stitching.StitchedUnionTables;
import de.uni_mannheim.informatik.dws.tnt.match.stitching.UnionTables;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.writers.JsonTableWriter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * @author Oliver Bruski (oliver.l.bruski@campus.tu-berlin.de)
 */
public class Starter {

    private static int getMaxUnionId(Connection con) {
        int unionid = 0;

        try {
            ResultSet resultSet = con.createStatement().executeQuery("SELECT max(stitched_tableid) as maxID FROM stitched_tables");
            while (resultSet.next()) {
                unionid = resultSet.getInt("maxID");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return unionid;
    }

    private static void createVerticaTables(Connection con) throws SQLException {
        con.createStatement().executeUpdate("CREATE TABLE IF NOT EXISTS stitched_tables (" +
                "tableid INT PRIMARY KEY NOT NULL," +
                "stitched_tableid INT NOT NULL" +
                ")" +
                "SEGMENTED BY HASH(tableid) ALL NODES");

        con.createStatement().executeUpdate("CREATE PROJECTION IF NOT EXISTS tokenized_to_col (" +
                "tokenized" +
                ",term" +
                ",tableid" +
                ",colid" +
                ",rowid) " +
                "AS SELECT tokenized, term, tableid, colid, rowid " +
                "FROM main_tokenized " +
                "ORDER BY tokenized, tableid, colid, rowid " +
                "SEGMENTED BY HASH(tokenized) ALL NODES");
    }

    private static ArrayList<String> getDomains(Connection con) {
        ArrayList<String> domains = new ArrayList<>();
        try {
            ResultSet resultSet = con.createStatement().executeQuery(
                    "SELECT domain FROM tables_tokenized_full GROUP BY domain HAVING count(id)>2");
            while (resultSet.next()) {
                domains.add(resultSet.getString("domain"));
            }
            resultSet.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return domains;
    }

    private static Collection<Table> createStitchedTables(WebTables web, File workingPath) throws Exception {
        //------------ Union Tables ---------------------
        UnionTables union = new UnionTables();
        Map<String, Integer> contextAttributes = null;

        System.err.println("Creating Context Attributes");
        contextAttributes = union.generateContextAttributes(web.getTables().values(), true, false);

        System.err.println("Creating Union Tables");
        Collection<Table> unionTables = union.create(new ArrayList<>(web.getTables().values()), contextAttributes);

        File outUnion = new File(workingPath.getPath() + "/union");
        outUnion.mkdirs();

        System.err.println("Writing Union Tables");
        JsonTableWriter w = new JsonTableWriter();
        for (Table t : unionTables) {
            w.write(t, new File(outUnion, t.getPath()));
        }
        System.err.println("Union Done.");
        //Clean Up
        web = null;
        contextAttributes = null;

        //------------ Functional Dependencies ---------
        System.err.println("Loading Web Tables for Functional Dependency Discovery");
        web = WebTables.loadWebTables(outUnion, true, false, false, false);
        File csvFile = new File(workingPath.getPath() + "/union_csv");
        csvFile.mkdirs();

        System.err.println("Running Functional Dependecy Discovery");
        FunctionalDependencyDiscovery discovery = new FunctionalDependencyDiscovery();
        discovery.run(web.getTables().values(), csvFile);

        File jsonFile = new File(workingPath + "/dependencies");
        jsonFile.mkdirs();

        System.err.println("Writing Tables");
        //w = new JsonTableWriter();
        for (Table t : web.getTables().values()) {
            w.write(t, new File(jsonFile, t.getPath()));
        }
        System.err.println("Functional Dependency Discovery Done.");
        //Clean Up
        web = null;
        csvFile = null;
        discovery = null;

        //------------ Stitching Union Tables ------------
        System.err.println("Loading Web Tables for Stitching");
        web = WebTables.loadWebTables(jsonFile, true, true, false, false);
        web.removeHorizontallyStackedTables();

        System.err.println("Matching Union Tables");
        CreateStitchedUnionTables stitchedUnionTables = new CreateStitchedUnionTables();
        Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences = null;
        schemaCorrespondences = stitchedUnionTables.runTableMatching(web);

        System.err.println("Creating Stitched Union Tables");
        StitchedUnionTables stitchedUnion = new StitchedUnionTables();
        Collection<Table> reconstructed = stitchedUnion.create(
                web.getTables(), web.getRecords(), web.getSchema(), web.getCandidateKeys(), schemaCorrespondences);

        File outFile = new File(workingPath + "/stitched_union");
        outFile.mkdirs();

        System.err.println("Writing Stitched Union Tables");
        //w = new JsonTableWriter();
        for (Table t : reconstructed) {
            w.write(t, new File(outFile, t.getPath()));
        }
        System.err.println("All Done.");
        //Clean Up
        web = null;
        stitchedUnion = null;
        schemaCorrespondences = null;

        return reconstructed;
    }

    public static void main(String[] args) {
        File workingPath = new File("." + File.separator + "examples");
        try {
            Connection con = DriverManager.getConnection("jdbc:vertica://localhost/xformer", "olib92", "Transformer2");
            con.setAutoCommit(false);
            createVerticaTables(con);
            VerticaCopyStream unionStream = new VerticaCopyStream(
                    (VerticaConnection) con, "COPY stitched_tables"
                    + " (stitched_tableid, stitched_colid, tableid, colid) "
                    + " FROM STDIN "
                    + " DELIMITER ',' ENCLOSED BY '\"' DIRECT"
                    + " REJECTED DATA '/home/olib92/logs/rejectedStitchedTables.txt'");
            unionStream.start();

            //------------ Get all domains with 2+ tables to union the tables ------------
            ArrayList<String> domains = getDomains(con);
            int unionid = getMaxUnionId(con);
            for (String domain : domains) {
                //------------ Union Tables ---------------------
                System.err.println("Loading Web Tables");
                WebTables web = WebTables.loadVerticaWebTables(
                        con, domain, true, false, false, false);

                Collection<Table> stitchedTables;
                try {
                    stitchedTables = createStitchedTables(web, workingPath);
                } catch (Exception e) {
                    continue;
                }

                //---------- Write to database ------
                for (int i = 0; i < stitchedTables.size(); i++) {
                    unionid++;
                    int union_colid = 0;
                    Table t = (Table) stitchedTables.toArray()[i];
                    for (int j = 0; j < t.getColumns().size(); j++) {
                        union_colid++;
                        TableColumn c = (TableColumn) t.getColumns().toArray()[j];
                        for (int k = 0; k < c.getProvenance().size(); k++) {
                            String str = (String) c.getProvenance().toArray()[k];
                            String[] provenanace = str.substring(str.lastIndexOf('#') + 1).split("~");
                            int tableid = 0;
                            int colid = 0;
                            try {
                                tableid = Integer.parseInt(provenanace[0]);
                                colid = Integer.parseInt(provenanace[1].replace("Col", ""));

                            } catch (NumberFormatException e) {
                                continue;

                            }
                            StringWriter stringWriter = new StringWriter();
                            CSVWriter unionWriter = new CSVWriter(stringWriter, ',', '"', '\\');
                            unionWriter.writeNext(new String[]{Integer.toString(unionid),
                                    Integer.toString(union_colid),
                                    Integer.toString(tableid),
                                    Integer.toString(colid)});
                            unionWriter.flush();
                            unionWriter.close();
                            String unionString = stringWriter.toString();
                            unionStream.addStream(IOUtils.toInputStream(unionString));
                        }
                    }
                }


                unionStream.execute();
                System.out.println(unionStream.getRejects().size() + " union files not written!");


                //---------- Clean Up ---------------
                FileUtils.deleteDirectory(new File(workingPath + "/union"));
                FileUtils.deleteDirectory(new File(workingPath + "/union_csv"));
                FileUtils.deleteDirectory(new File(workingPath + "/dependencies"));
                FileUtils.deleteDirectory(new File(workingPath + "/stitched_union"));
            }
            unionStream.finish();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (OutOfMemoryError e) {

        }
    }

}
