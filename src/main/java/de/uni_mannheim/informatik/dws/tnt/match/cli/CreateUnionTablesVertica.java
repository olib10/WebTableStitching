package de.uni_mannheim.informatik.dws.tnt.match.cli;

import au.com.bytecode.opencsv.CSVWriter;
import com.vertica.jdbc.VerticaConnection;
import com.vertica.jdbc.VerticaCopyStream;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.tnt.match.stitching.UnionTables;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class CreateUnionTablesVertica {

    private static int unionid;

    public static void main(String[] args) throws SQLException, URISyntaxException, IOException {

        File workingDir = null;
        try {
            workingDir = new File(args[0]);
        } catch (Exception e) {
            System.err.println("Please provide a directory with table files!");
        }

        File[] files = null;
        if (workingDir.isDirectory()) {
            files = workingDir.listFiles((File file, String name) -> name.endsWith(".txt.gz"));
        } else {
            files = new File[]{workingDir};
        }
        Queue<File> tableFiles = new LinkedList<>(Arrays.asList(files));

        Connection con = DriverManager.getConnection("jdbc:vertica://localhost/xformer", "olib92", "Transformer2");
        createUnionTablesRelation(con);

        unionid = 0;
        try {
            ResultSet resultSet = con.createStatement().executeQuery("SELECT max(unionid) as maxID FROM union_tables_new");
            while (resultSet.next()) {
                unionid = resultSet.getInt("maxID");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        VerticaCopyStream unionStream = new VerticaCopyStream(
                (VerticaConnection) con, "COPY union_tables_new"
                + " (tableid , unionid, numRows_union)"
                + " FROM STDIN "
                + " DELIMITER ',' ENCLOSED BY '\"' DIRECT"
                + " REJECTED DATA '/home/olib92/logs/rejectedUnion.txt'");
        unionStream.start();

        WebTables web;
        UnionTables union;
        Collection<Table> unionTables;
        Map<String, Integer> contextAttributes;
        unionid++;
        for (File f : tableFiles) {
            System.out.println(f.getName());
            try {
                //System.out.println("Loading file " + f.getName());
                web = WebTables.loadWebTables(f, true, false, false, false);
                union = new UnionTables();
                contextAttributes = union.generateContextAttributes(web.getTables().values(), true, false);

                unionTables = union.create(new ArrayList<>(web.getTables().values()), contextAttributes);

                StringWriter stringWriter;
                CSVWriter unionWriter = null;
                for (Table t : unionTables) {
                    int numRows = t.getRows().size();
                    ArrayList<TableColumn> columns = (ArrayList<TableColumn>) t.getColumns();
                    TableColumn column = columns.get(columns.size() - 1);
                    for (String str : column.getProvenance()) {
                        int tableid = Integer.valueOf(str.substring(str.lastIndexOf('#') + 1, str.lastIndexOf('~')));
                        //System.out.println(tableid + ", " + unionid + " " + numRows);
                        stringWriter = new StringWriter();
                        unionWriter = new CSVWriter(stringWriter, ',', '"', '\\');
                        unionWriter.writeNext(new String[]{Integer.toString(tableid), Integer.toString(unionid), Integer.toString(numRows)});

                        String unionString = stringWriter.toString();
                        unionStream.addStream(IOUtils.toInputStream(unionString));
                    }
                    unionid++;
                }//END FOR UNIONTABLES
                unionWriter.flush();
                unionWriter.close();

                if ((unionid % 5000) == 0) {
                    unionStream.execute();
                    if (unionStream.getRejects().size() != 0)
                        System.out.println(unionStream.getRejects().size() + "values rejected");
                    System.out.println("Writting Tables!");
                }

                contextAttributes.clear();
                unionTables.clear();
            } catch (Exception e) {
                unionStream.execute();
                System.out.printf("Error occured at file %s and unionid %d", f.getName(), unionid);
                System.out.println();
                e.printStackTrace();
            }
        }//END FOR FILES
        unionStream.execute();
        if (unionStream.getRejects().size() != 0)
            System.out.println(unionStream.getRejects().size() + "values rejected");
        System.out.println(unionid + " union tables created!");

        unionStream.finish();
    }

//    private static ArrayList<String> getDomainsAsList(Connection con) {
//        ArrayList<String> domains = new ArrayList<>();
//        try {
//            ResultSet resultSet = con.createStatement().executeQuery(
//                    "SELECT domain FROM tables_tokenized_full GROUP BY domain HAVING count(id)>1");
//            while (resultSet.next()) {
//                domains.add(resultSet.getString("domain"));
//            }
//            resultSet.close();
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//        return domains;
//    }

    private static void createUnionTablesRelation(Connection con) throws SQLException {
        con.createStatement().executeUpdate("CREATE TABLE IF NOT EXISTS union_tables_new (" +
                "tableid INT PRIMARY KEY NOT NULL," +
                "unionid INT NOT NULL," +
                "numRows_union INT" +
                ")" +
                "SEGMENTED BY HASH(tableid) ALL NODES");
    }

    public static void setUnionId(int id) {
        unionid = id;
    }

}
